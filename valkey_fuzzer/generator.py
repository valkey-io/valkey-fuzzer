"""Random scenarios for every experiment that declares what may be randomized.

An experiment opts in with a ``metadata.generator`` block; the blocks of the
fragments it extends merge into it like the rest of ``metadata``::

    metadata:
      generator:
        hook: valkey.generator.tmpfs-data        # optional scenario hook
        deployment:
          mode: [standalone, cluster]            # default: every mode the experiment supports
          replicas_per_primary: {minimum: 1, maximum: 2}
          cluster: {primaries: {minimum: 3, maximum: 4}}   # fields drawn only in that mode
        inputs:                                  # default: every input with enum, boolean, or min+max
          value_size: {values: [1, 64, 4096]}
          loss_window_s: fixed                   # never drawn; the input's default applies
          waves: {hook: valkey.generator.waves}
        server_config: [valkey.pool.persistence] # pools, from metadata.generator.pools of any fragment
        reject:
          - {server_config.repl-diskless-sync: "no", inputs.expect_disk_sync: false}

A rule is ``fixed``, a list or ``{values: [...]}`` (drawn uniformly, the first and
last value twice as often), ``{minimum, maximum}`` (40% of draws are the edges
min, min+1, max-1, max, and 0 or 1 when in range; the rest uniform, or
log-uniform when max/min exceeds 1000), ``{hook: <id>}``, or a scalar (that
value). Enums and booleans an experiment declares are drawn uniformly.

A pool maps server config keys to value lists; its optional ``scope:
[deployment, role]`` draws whether every instance shares one value
(``deployment.server_config``) or the primary and the replicas each draw their
own (``deployment.instance_configs``). A key one of the experiment's startup
overlays sets is never drawn, since the overlay would override it; when the
overlay sets it from an input (``{$ref: inputs.<name>}``), an exclusion of a
``server_config.<key>`` value also excludes it from that input's draws.

Hooks are the catalog's generator registrations
(:class:`~valkey_fuzzer.registrations.GeneratorRegistration`), each a
``hook(rng, scenario)``: an input hook returns the input's value, and the
scenario hook returns a partial scenario merged over the draws. Each sees a
copy of the scenario drawn so far.

Every dimension draws from its own random stream
``sha256("gen", seed, experiment, dimension, attempt)``, so adding an input
never shifts another draw and one seed draws differently for each experiment;
the experiment itself is drawn from ``sha256("gen", seed, "experiment", 0)``.
Each candidate is compiled; one that fails to compile, matches a
``reject`` rule, or holds an excluded value is redrawn with the next attempt,
up to :data:`MAX_ATTEMPTS`.

The block above is the experiment's legal space. A **search profile**
(:class:`Profile`, declared under ``metadata.generator.profiles`` of a fragment
no experiment extends) only changes how hard that space is sampled: the share
of range draws that are edges, narrower numeric ranges (clamped into the
legal range, so never wider), other weights for listed values (never another
value), and the batch budgets (a run deadline, seeds per experiment). It
cannot add a value, change a ``reject`` rule, a hook, a pool, an input the
block fixes, or anything the compiled plan's oracles check::

    metadata:
      generator:
        profiles:
          pr-quick:
            description: Small deployments, edge-heavy values, short runs.
            edge_probability: 0.6
            ranges: {deployment.replicas_per_primary: {maximum: 1}, inputs.operations: {maximum: 1000}}
            weights: {deployment.mode: {standalone: 3}}
            budget: {seeds: 3, timeout_s: 600}
"""

import copy
import hashlib
import math
import random
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from typing import Any, Callable, Optional

import yaml

from ._data import to_plain_data
from .catalog import Catalog
from .compiler import with_valkey_binary
from .documents import ABSENT, get_path, merge
from .fields import is_number
from .plan import ExecutionPlan, ExperimentSpec
from .runner import RunnerError
from .schema import ConfigError, ConfigField, ConfigSchema, ScenarioCompileError

MAX_ATTEMPTS = 50
EDGE_PROBABILITY = 0.4
# Ranges wider than this ratio are drawn log-uniformly, so small values are not starved.
LOG_UNIFORM_RATIO = 1000

_BLOCK_FIELDS = ("hook", "deployment", "inputs", "server_config", "reject", "pools")
# Where an exclusion or reject path points: the experiment, an input, a deployment
# field, or a server config key a planned instance starts with.
_PATH_NAMESPACES = ("experiment", "inputs", "deployment", "server_config")
_SCOPES = ("deployment", "role")


class GeneratorError(ValueError):
    """An experiment's generator block is malformed, or no valid scenario could be drawn."""


def generator_rng(seed: int, dimension: str, attempt: int, experiment: Optional[str] = None) -> random.Random:
    """The random stream of one dimension of one experiment's attempt (the derivation ``scenario_rng`` uses)."""

    scope = f"{experiment}\0" if experiment else ""
    material = f"gen\0{seed}\0{scope}{dimension}\0{attempt}".encode()
    return random.Random(int.from_bytes(hashlib.sha256(material).digest()[:16], "big"))


# ---------------------------------------------------------------------------
# Exclusions (--exclude path=value)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Exclusion:
    """``path=value``: the value never appears at the path in a generated scenario."""

    path: str
    value: str

    @classmethod
    def parse(cls, text: Any) -> "Exclusion":
        if not isinstance(text, str) or "=" not in text:
            raise GeneratorError(f"an exclusion is path=value, got {text!r}")
        path, value = (part.strip() for part in text.split("=", 1))
        _check_path(path, "exclusion")
        try:
            yaml.safe_load(value)
        except yaml.YAMLError as exc:
            raise GeneratorError(f"exclusion {text!r}: the value is not valid YAML ({exc})") from None
        return cls(path, value)

    def matches(self, value: Any) -> bool:
        """Whether ``value`` is the excluded one: a string as written, anything else as YAML reads it."""

        return _same(value, self.value if isinstance(value, str) else yaml.safe_load(self.value))

    def __str__(self) -> str:
        return f"{self.path}={self.value}"


def _same(value: Any, expected: Any) -> bool:
    """Equality in the YAML data model: a boolean is never a number or a string."""

    if isinstance(value, bool) or isinstance(expected, bool):
        return type(value) is type(expected) and value == expected
    if is_number(value) and is_number(expected):
        return value == expected
    return isinstance(value, str) and isinstance(expected, str) and value == expected


def _check_path(path: Any, where: str) -> None:
    if not isinstance(path, str) or path.split(".")[0] not in _PATH_NAMESPACES or path.endswith("."):
        raise GeneratorError(
            f"{where} path {path!r} must be 'experiment' or start with inputs., deployment., or server_config."
        )
    if path != "experiment" and "." not in path:
        raise GeneratorError(f"{where} path {path!r} must name a field, as in {path}.<name>")


def _config_path(dimension: str) -> str:
    """The path an exclusion or profile names a dimension by: one instance's server config key is the key's."""

    if dimension.startswith("instance_configs."):
        return "server_config." + dimension.split(".", 2)[2]
    return dimension


def _excluded(dimension: str, excludes: Sequence[Exclusion], aliases: Sequence[str] = ()) -> list[Exclusion]:
    """The exclusions of one dimension and its ``aliases``; a server config key's apply to every instance's value."""

    dimension = _config_path(dimension)
    return [item for item in excludes if item.path == dimension or item.path in aliases]


def _plan_values(plan: ExecutionPlan, path: str) -> list[Any]:
    """Every value the compiled plan holds at ``path`` (inputs with their defaults).

    A server config key's values are those its planned instances start with
    (the provider's defaults, the deployment's config, and the startup
    overlays); a plan whose provider plans no instances has only the
    deployment's ``server_config`` and the overlays.
    """

    if path == "experiment":
        return [plan.experiment_id]
    head, rest = path.split(".", 1)
    if head == "server_config":
        if plan.instances:
            configs = [instance.get("server_config") for instance in to_plain_data(plan.instances).values()]
        else:
            configs = [to_plain_data(plan.deployment).get("server_config") or {}]
            configs += [to_plain_data(overlay.server_config) for overlay in plan.startup_overlays]
        return [config[rest] for config in configs if isinstance(config, Mapping) and rest in config]
    value = get_path(to_plain_data(plan.inputs if head == "inputs" else plan.deployment), rest)
    return [] if value is ABSENT else [value]


# ---------------------------------------------------------------------------
# Rules
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _Rule:
    """How one dimension is drawn: from ``values`` (with ``weights``), a numeric range, or a hook."""

    values: tuple[Any, ...] = ()
    weights: tuple[int, ...] = ()
    minimum: Optional[float] = None
    maximum: Optional[float] = None
    integer: bool = False
    hook: Optional[str] = None
    # Explicit value lists and ranges have edges that count toward the drawn scenario's boundary.
    edges_count: bool = False

    @classmethod
    def listed(cls, values: Sequence[Any], where: str) -> "_Rule":
        values = tuple(values)
        if not values:
            raise GeneratorError(f"{where} lists no values")
        weights = tuple(2 if index in (0, len(values) - 1) else 1 for index in range(len(values)))
        return cls(values=values, weights=weights, edges_count=len(values) > 2)

    @classmethod
    def uniform(cls, values: Sequence[Any]) -> "_Rule":
        return cls(values=tuple(values), weights=(1,) * len(values))

    @classmethod
    def parse(cls, value: Any, where: str, *, integer: bool = False) -> Optional["_Rule"]:
        """The rule ``value`` writes; None for ``fixed``."""

        if value == "fixed":
            return None
        if isinstance(value, (list, tuple)):
            return cls.listed(value, where)
        if not isinstance(value, Mapping):
            return cls.uniform((value,))
        keys = set(value)
        if keys == {"values"} and isinstance(value["values"], (list, tuple)):
            return cls.listed(value["values"], where)
        if keys == {"hook"} and isinstance(value["hook"], str):
            return cls(hook=value["hook"])
        if keys == {"minimum", "maximum"}:
            low, high = value["minimum"], value["maximum"]
            if not all(is_number(bound) for bound in (low, high)) or low > high:
                raise GeneratorError(f"{where} needs numeric minimum <= maximum, got {dict(value)}")
            return cls(minimum=low, maximum=high, integer=integer, edges_count=True)
        raise GeneratorError(
            f"{where} must be fixed, a value, a list, {{values: [...]}}, {{minimum, maximum}}, or {{hook: <id>}}; "
            f"got {to_plain_data(value)!r}"
        )

    def draw(
        self,
        rng: random.Random,
        excludes: Sequence[Exclusion],
        where: str,
        edge_probability: float = EDGE_PROBABILITY,
    ) -> tuple[Any, bool]:
        """A value and whether it is an edge (the first or last listed value, or a range edge)."""

        if self.values:
            choices = [
                (value, weight)
                for value, weight in zip(self.values, self.weights)
                if not any(item.matches(value) for item in excludes)
            ]
            if not choices:
                raise GeneratorError(f"every value of {where} is excluded")
            value = rng.choices([value for value, _ in choices], [weight for _, weight in choices])[0]
            return value, self.edges_count and value in (self.values[0], self.values[-1])
        # A range: an excluded number is redrawn from the same stream.
        for _ in range(100):
            value, edge = _number(rng, self.minimum, self.maximum, self.integer, edge_probability)
            if not any(item.matches(value) for item in excludes):
                return value, edge
        raise GeneratorError(f"{where}: every draw from [{self.minimum}, {self.maximum}] was excluded")


def _number(
    rng: random.Random, low: float, high: float, integer: bool, edge_probability: float = EDGE_PROBABILITY
) -> tuple[Any, bool]:
    """A number in [low, high]: an edge with ``edge_probability``, otherwise uniform or log-uniform."""

    cast: Callable[[float], Any] = int if integer else float
    edges = sorted({cast(edge) for edge in (low, low + 1, high - 1, high, 0, 1) if low <= edge <= high})
    if rng.random() < edge_probability:
        return rng.choice(edges), True
    floor = max(low, 1)
    if low >= 0 and high / floor > LOG_UNIFORM_RATIO:
        value = math.exp(rng.uniform(math.log(floor), math.log(high)))
    else:
        value = rng.uniform(low, high)
    value = round(value) if integer else float(f"{value:.4g}")
    value = min(max(value, cast(low)), cast(high))
    return value, value in edges


# ---------------------------------------------------------------------------
# The generator block
# ---------------------------------------------------------------------------


def generator_document(spec: ExperimentSpec) -> Optional[Mapping[str, Any]]:
    """The experiment's ``metadata.generator`` block, or None when it does not opt in."""

    block = to_plain_data(spec.metadata).get("generator")
    return block if isinstance(block, Mapping) else None


def _pool(pool_id: str, document: Any) -> dict[str, Any]:
    where = f"pool {pool_id!r}"
    if not isinstance(document, Mapping) or not document:
        raise GeneratorError(f"{where} must map server config keys to value lists")
    for key, values in document.items():
        if not isinstance(values, (list, tuple)) or not values:
            raise GeneratorError(f"{where}.{key} must be a non-empty list")
        if key == "scope" and not set(values) <= set(_SCOPES):
            raise GeneratorError(f"{where}.scope must list {list(_SCOPES)}, got {values!r}")
    return dict(document)


def _fragment_tables(catalog: Catalog, key: str, what: str) -> dict[str, Any]:
    """Every entry of ``metadata.generator.<key>`` in the installed fragments, by name, as written.

    A name is global: two fragments may not define it.
    """

    found: dict[str, Any] = {}
    for name, fragment in sorted(catalog.fragments.items()):
        metadata = to_plain_data(fragment).get("metadata")
        block = metadata.get("generator") if isinstance(metadata, Mapping) else None
        table = block.get(key) if isinstance(block, Mapping) else None
        if table is None:
            continue
        if not isinstance(table, Mapping):
            raise GeneratorError(f"fragment {name!r}: metadata.generator.{key} must map {what} names to {what}s")
        for entry, document in table.items():
            if entry in found:
                raise GeneratorError(f"{what} {entry!r} is defined by more than one fragment")
            found[entry] = document
    return found


def installed_pools(catalog: Catalog) -> dict[str, dict[str, Any]]:
    """Every pool in the ``metadata.generator.pools`` of the installed fragments, by ID."""

    return {pool_id: _pool(pool_id, pool) for pool_id, pool in _fragment_tables(catalog, "pools", "pool").items()}


# ---------------------------------------------------------------------------
# Search profiles
# ---------------------------------------------------------------------------

_BOUNDS = ConfigSchema({"minimum": ConfigField(float, required=False), "maximum": ConfigField(float, required=False)})
_PROFILE = ConfigSchema(
    {
        "description": ConfigField(str, default=""),
        "edge_probability": ConfigField(float, minimum=0, maximum=1, default=EDGE_PROBABILITY),
        "ranges": ConfigField(
            dict, default=dict, allow_none=True, schema=ConfigSchema(additional=ConfigField(dict, schema=_BOUNDS))
        ),
        "weights": ConfigField(dict, default=dict, allow_none=True),
        "budget": ConfigField(
            dict,
            default=dict,
            allow_none=True,
            schema=ConfigSchema(
                {"timeout_s": ConfigField(float, required=False), "seeds": ConfigField(int, minimum=1, required=False)}
            ),
        ),
    }
)


@dataclass(frozen=True)
class Profile:
    """A search profile: how hard to sample an experiment's legal space, and the batch budgets.

    ``ranges`` maps a ``deployment.<field>`` or ``inputs.<name>`` path to
    ``(minimum, maximum)`` (either may be None); ``weights`` maps a
    ``deployment.``, ``inputs.``, or ``server_config.`` path to ``(value,
    weight)`` pairs. A path an experiment does not draw is ignored, so one
    profile serves every experiment.
    """

    name: str
    description: str = ""
    edge_probability: float = EDGE_PROBABILITY
    ranges: Mapping[str, tuple[Optional[float], Optional[float]]] = field(default_factory=dict)
    weights: Mapping[str, tuple[tuple[Any, float], ...]] = field(default_factory=dict)
    timeout_s: Optional[float] = None
    seeds: Optional[int] = None

    @classmethod
    def parse(cls, name: str, document: Any) -> "Profile":
        """The profile ``document`` declares; validity rules, hooks, pools, and oracle config belong to the
        experiment, never to a profile."""

        where = f"profiles.{name}"
        try:
            document = _PROFILE.resolve(document, where)
        except ConfigError as exc:
            raise GeneratorError(str(exc)) from None
        ranges = {}
        for path, bounds in _profile_table(document, "ranges", where, ("deployment", "inputs")):
            low, high = bounds.get("minimum"), bounds.get("maximum")
            if not bounds or (low is not None and high is not None and low > high):
                raise GeneratorError(f"{where}.ranges.{path} needs a minimum, a maximum, or both, minimum <= maximum")
            ranges[path] = (low, high)
        weights = {}
        for path, table in _profile_table(document, "weights", where, ("deployment", "inputs", "server_config")):
            # Keys are the listed values, which YAML may read as numbers or booleans.
            if not isinstance(table, Mapping) or not table:
                raise GeneratorError(f"{where}.weights.{path} must map values to weights")
            if any(not is_number(weight) or weight <= 0 for weight in table.values()):
                raise GeneratorError(f"{where}.weights.{path}: every weight must be a number greater than zero")
            weights[path] = tuple(table.items())
        budget = document["budget"] or {}
        timeout_s, seeds = budget.get("timeout_s"), budget.get("seeds")
        if timeout_s is not None and timeout_s <= 0:
            raise GeneratorError(f"{where}.budget.timeout_s must be a number of seconds greater than zero")
        edge = float(document["edge_probability"])
        return cls(name, document["description"], edge, ranges, weights, timeout_s, seeds)

    def rule(self, rule: "_Rule", dimension: str) -> "_Rule":
        """``rule`` as this profile samples it: never a value outside it.

        A range is narrowed by clamping the profile's bounds into the rule's
        own, so it can only shrink and never becomes empty; a listed value
        gets the profile's weight, and a weighted value the rule does not list
        is ignored.
        """

        path = _config_path(dimension)
        if rule.minimum is not None and path in self.ranges:
            low, high = self.ranges[path]

            def clamp(bound: Optional[float], default: float) -> float:
                return default if bound is None else min(max(bound, rule.minimum), rule.maximum)

            return replace(rule, minimum=clamp(low, rule.minimum), maximum=clamp(high, rule.maximum))
        if rule.values and path in self.weights:
            table = self.weights[path]
            weights = tuple(
                next((weight for listed, weight in table if _same(value, listed)), default)
                for value, default in zip(rule.values, rule.weights)
            )
            return replace(rule, weights=weights)
        return rule


def _profile_table(document: Mapping[str, Any], key: str, where: str, namespaces: Sequence[str]) -> list:
    table = document[key] or {}
    for path in table:
        _check_path(path, f"{where}.{key}")
        if path.split(".")[0] not in namespaces:
            raise GeneratorError(
                f"{where}.{key} path {path!r} must start with {', '.join(f'{n}.' for n in namespaces)}"
            )
    return list(table.items())


def installed_profiles(catalog: Catalog) -> dict[str, Profile]:
    """Every profile in the ``metadata.generator.profiles`` of the installed fragments, by name."""

    return {
        name: Profile.parse(name, document)
        for name, document in _fragment_tables(catalog, "profiles", "profile").items()
    }


def find_profile(catalog: Catalog, name: str) -> Profile:
    """The installed profile ``name``."""

    profiles = installed_profiles(catalog)
    if name not in profiles:
        raise GeneratorError(f"no such search profile {name!r}; installed: {sorted(profiles) or 'none'}")
    return profiles[name]


@dataclass(frozen=True)
class GeneratorBlock:
    """An experiment's parsed ``metadata.generator`` block."""

    experiment_id: str
    modes: _Rule
    # Deployment rules for every mode, then the rules of each mode's own sub-block.
    deployment: Mapping[str, _Rule]
    mode_deployment: Mapping[str, Mapping[str, _Rule]]
    inputs: Mapping[str, _Rule]
    pools: tuple[tuple[str, Mapping[str, Any]], ...]
    reject: tuple[Mapping[str, Any], ...]
    hook: Optional[str] = None
    # Server config keys the experiment's startup overlays set.
    overlay_keys: frozenset = field(default_factory=frozenset)
    # Input name to the server_config.<key> paths its value sets through an overlay.
    input_configs: Mapping[str, tuple[str, ...]] = field(default_factory=dict)

    @property
    def hook_ids(self) -> list[str]:
        ids = [rule.hook for rule in self.inputs.values() if rule is not None and rule.hook]
        return ([self.hook] if self.hook else []) + ids

    @classmethod
    def parse(cls, spec: ExperimentSpec, pools: Mapping[str, Mapping[str, Any]]) -> "GeneratorBlock":
        document = generator_document(spec)
        where = f"{spec.experiment_id} metadata.generator"
        if document is None:
            raise GeneratorError(f"experiment {spec.experiment_id!r} does not declare metadata.generator")
        unknown = sorted(set(document) - set(_BLOCK_FIELDS))
        if unknown:
            raise GeneratorError(f"{where} has unknown fields {unknown}; allowed: {list(_BLOCK_FIELDS)}")
        hook = document.get("hook")
        if hook is not None and not isinstance(hook, str):
            raise GeneratorError(f"{where}.hook must be a generator ID")

        deployment = dict(document.get("deployment") or {})
        mode_rule = _Rule.parse(deployment.pop("mode", list(spec.deployment_modes)), f"{where}.deployment.mode")
        if mode_rule is None or mode_rule.hook or not mode_rule.values:
            raise GeneratorError(f"{where}.deployment.mode must list deployment modes")
        if not set(mode_rule.values) <= set(spec.deployment_modes):
            raise GeneratorError(
                f"{where}.deployment.mode {list(mode_rule.values)} is not within the experiment's modes "
                f"{list(spec.deployment_modes)}"
            )
        mode_deployment = {
            mode: _rules(deployment.pop(mode), f"{where}.deployment.{mode}")
            for mode in spec.deployment_modes
            if mode in deployment
        }

        experiment_pools = dict(pools)
        for pool_id, pool in (document.get("pools") or {}).items():
            experiment_pools[pool_id] = _pool(pool_id, pool)
        selected = document.get("server_config") or []
        if not isinstance(selected, (list, tuple)):
            raise GeneratorError(f"{where}.server_config must list pool IDs")
        missing = [pool_id for pool_id in selected if pool_id not in experiment_pools]
        if missing:
            raise GeneratorError(f"{where}.server_config names unknown pools {missing}")

        reject = document.get("reject") or []
        if not isinstance(reject, (list, tuple)) or not all(isinstance(rule, Mapping) and rule for rule in reject):
            raise GeneratorError(f"{where}.reject must be a list of non-empty {{path: value}} mappings")
        for rule in reject:
            for path in rule:
                _check_path(path, f"{where}.reject")

        overlay_keys = frozenset(key for overlay in spec.startup_overlays for key in overlay.server_config)
        input_configs: dict[str, tuple[str, ...]] = {}
        for overlay in spec.startup_overlays:
            for key, value in overlay.server_config.items():
                ref = value.get("$ref") if isinstance(value, Mapping) else None
                if isinstance(ref, str) and ref.startswith("inputs.") and ref.count(".") == 1:
                    name = ref.split(".", 1)[1]
                    input_configs[name] = (*input_configs.get(name, ()), f"server_config.{key}")
        return cls(
            experiment_id=spec.experiment_id,
            modes=mode_rule,
            deployment=_rules(deployment, f"{where}.deployment"),
            mode_deployment=mode_deployment,
            inputs=_input_rules(spec, document.get("inputs") or {}, f"{where}.inputs"),
            pools=tuple((pool_id, experiment_pools[pool_id]) for pool_id in selected),
            reject=tuple(dict(rule) for rule in reject),
            hook=hook,
            overlay_keys=overlay_keys,
            input_configs=input_configs,
        )

    # -- drawing --------------------------------------------------------------

    def draw(
        self,
        seed: int,
        attempt: int,
        *,
        hooks: Mapping[str, Callable[[random.Random, Mapping[str, Any]], Any]],
        with_binary: Callable[[Mapping[str, Any]], dict[str, Any]],
        excludes: Sequence[Exclusion] = (),
        base: Optional[Mapping[str, Any]] = None,
        profile: Optional[Profile] = None,
        pinned: Optional[Mapping[str, Any]] = None,
    ) -> tuple[dict[str, Any], int, dict[str, Any]]:
        """One candidate scenario, how many of its draws were edges, and its draws by dimension, in draw order.

        ``profile`` changes only how it samples; a dimension in ``pinned`` takes that value instead of a draw
        (its hook does not run). ``with_binary`` gives a scenario the Valkey binary it runs, once its deployment
        mode is drawn.
        """

        edges = 0
        edge_probability = EDGE_PROBABILITY if profile is None else profile.edge_probability
        draws: dict[str, Any] = {}
        pinned = pinned or {}

        def stream(dimension: str) -> random.Random:
            return generator_rng(seed, dimension, attempt, self.experiment_id)

        def draw(rule: _Rule, dimension: str) -> Any:
            nonlocal edges
            if dimension in pinned:
                draws[dimension] = copy.deepcopy(pinned[dimension])
                return draws[dimension]
            aliases = self.input_configs.get(dimension[len("inputs.") :], ()) if dimension.startswith("inputs.") else ()
            if profile is not None:
                rule = profile.rule(rule, dimension)
            value, edge = rule.draw(
                stream(dimension), _excluded(dimension, excludes, aliases), dimension, edge_probability
            )
            edges += edge
            draws[dimension] = value
            return value

        mode = draw(self.modes, "deployment.mode")
        deployment: dict[str, Any] = {"mode": mode}
        rules = {**self.deployment, **self.mode_deployment.get(mode, {})}
        for name in sorted(rules):
            if rules[name] is not None:
                deployment[name] = draw(rules[name], f"deployment.{name}")
        self._draw_pools(deployment, draw)

        inputs = {
            name: draw(rule, f"inputs.{name}")
            for name, rule in sorted(self.inputs.items())
            if rule is not None and not rule.hook
        }
        scenario = {
            "scenario_id": f"{self.experiment_id}-seed{seed}",
            "seed": seed,
            "experiment": {"id": self.experiment_id, "inputs": inputs},
            "deployment": deployment,
        }
        visible = with_binary(merge(scenario, base or {}))
        for name, rule in sorted(self.inputs.items()):
            if rule is not None and rule.hook:
                dimension = f"inputs.{name}"
                if dimension in pinned:
                    inputs[name] = copy.deepcopy(pinned[dimension])
                else:
                    inputs[name] = hooks[rule.hook](stream(dimension), copy.deepcopy(visible))
                draws[dimension] = copy.deepcopy(inputs[name])
                visible["experiment"]["inputs"][name] = copy.deepcopy(inputs[name])
        if self.hook:
            if "hook" in pinned:
                patch = copy.deepcopy(pinned["hook"])
            else:
                patch = hooks[self.hook](stream("hook"), copy.deepcopy(visible))
            if not isinstance(patch, Mapping):
                raise GeneratorError(f"generator {self.hook!r} must return a partial scenario mapping")
            draws["hook"] = copy.deepcopy(patch)
            scenario = merge(scenario, patch)
        return with_binary(merge(scenario, base or {})), edges, draws

    def kept(self, draws: Mapping[str, Any], dimension: Optional[str]) -> dict[str, Any]:
        """The draws a scenario keeps (as ``pinned``) when it draws ``dimension`` anew; every draw for None.

        A hook sees everything drawn before it, so the hooks drawn after ``dimension`` are drawn anew too.
        """

        if dimension is None:
            return dict(draws)
        order = list(draws)
        hooked = {"hook"} | {f"inputs.{name}" for name, rule in self.inputs.items() if rule is not None and rule.hook}
        after = order.index(dimension) if dimension in order else len(order)
        return {
            name: draws[name]
            for index, name in enumerate(order)
            if name != dimension and not (index > after and name in hooked)
        }

    def _draw_pools(self, deployment: dict[str, Any], draw: Callable[[_Rule, str], Any]) -> None:
        roles = ["primary"] + (["replica"] if deployment.get("replicas_per_primary", 0) else [])
        for pool_id, pool in self.pools:
            scope = draw(_Rule.uniform(pool.get("scope", ["deployment"])), f"pools.{pool_id}.scope")
            for key in sorted(set(pool) - {"scope"} - self.overlay_keys):
                rule = _Rule.uniform(pool[key])
                if scope == "deployment":
                    targets = [(deployment.setdefault("server_config", {}), f"server_config.{key}")]
                else:
                    configs = deployment.setdefault("instance_configs", {})
                    targets = [(configs.setdefault(role, {}), f"instance_configs.{role}.{key}") for role in roles]
                for config, dimension in targets:
                    try:
                        config[key] = draw(rule, dimension)
                    except GeneratorError:
                        # Every value excluded: the server's own default applies.
                        continue

    def rejected(self, plan: ExecutionPlan, excludes: Sequence[Exclusion]) -> Optional[str]:
        """Why the compiled candidate is not acceptable, or None."""

        for item in excludes:
            if any(item.matches(value) for value in _plan_values(plan, item.path)):
                return f"holds the excluded {item}"
        for rule in self.reject:
            if all(
                any(_same(value, expected) for value in _plan_values(plan, path)) for path, expected in rule.items()
            ):
                return f"matches the reject rule {rule}"
        return None


def _rules(document: Any, where: str) -> dict[str, Optional[_Rule]]:
    """Deployment field rules; deployment fields are counts and ports, so ranges draw integers."""

    if not isinstance(document, Mapping):
        raise GeneratorError(f"{where} must be a mapping")
    rules = {name: _Rule.parse(value, f"{where}.{name}", integer=True) for name, value in document.items()}
    hooked = sorted(name for name, rule in rules.items() if rule is not None and rule.hook)
    if hooked:
        raise GeneratorError(f"{where} {hooked}: a hook draws an input or, as metadata.generator.hook, the scenario")
    return rules


def _input_rules(spec: ExperimentSpec, document: Any, where: str) -> dict[str, Optional[_Rule]]:
    """A rule for every input: the block's, or the default for enums, booleans, and bounded numbers."""

    if not isinstance(document, Mapping):
        raise GeneratorError(f"{where} must be a mapping")
    properties = to_plain_data(spec.inputs).get("properties") or {}
    unknown = sorted(set(document) - set(properties))
    if unknown:
        raise GeneratorError(f"{where} names inputs the experiment does not declare: {unknown}")
    schema = spec.input_schema
    rules: dict[str, Optional[_Rule]] = {}
    for name, declared in properties.items():
        location = f"{where}.{name}"
        kind = declared.get("type")
        if name in document:
            rule = _Rule.parse(document[name], location, integer=kind == "integer")
        elif "enum" in declared:
            rule = _Rule.uniform(declared["enum"])
        elif kind == "boolean":
            rule = _Rule.uniform((False, True))
        elif kind in ("integer", "number") and "minimum" in declared and "maximum" in declared:
            rule = _Rule(
                minimum=declared["minimum"], maximum=declared["maximum"], integer=kind == "integer", edges_count=True
            )
        else:
            rule = None
        if rule is not None and rule.minimum is not None:
            low, high = declared.get("minimum"), declared.get("maximum")
            if (low is not None and rule.minimum < low) or (high is not None and rule.maximum > high):
                raise GeneratorError(
                    f"{location} [{rule.minimum}, {rule.maximum}] exceeds the input's bounds [{low}, {high}]"
                )
        for value in rule.values if rule is not None else ():
            try:
                schema.fields[name].resolve(value, location)
            except ConfigError as exc:
                raise GeneratorError(str(exc)) from None
        rules[name] = rule
    return rules


# ---------------------------------------------------------------------------
# Generating
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Generated:
    """A generated scenario, its compiled plan, and how it was drawn."""

    scenario: dict[str, Any]
    plan: ExecutionPlan
    seed: int
    attempt: int
    boundary: int
    excludes: tuple[Exclusion, ...] = ()
    profile: Optional[str] = None
    # Every drawn value by dimension, in draw order: what a coverage-guided batch pins to derive another scenario.
    draws: Mapping[str, Any] = field(default_factory=dict)

    @property
    def provenance(self) -> dict[str, Any]:
        """What the run report records under ``reproducer.generated``."""

        return {
            "seed": self.seed,
            "experiment": self.plan.experiment_id,
            "attempt": self.attempt,
            "excludes": [str(item) for item in self.excludes],
            "boundary": self.boundary,
            "profile": self.profile,
            "draws": to_plain_data(self.draws),
        }


def opted_in(catalog: Catalog, excludes: Sequence[Exclusion] = ()) -> list[str]:
    """The IDs of the experiments that declare ``metadata.generator`` and no ``experiment=`` exclusion names, sorted."""

    return sorted(
        spec.experiment_id
        for spec in catalog.experiments.snapshot().values()
        if generator_document(spec) is not None
        and not any(item.path == "experiment" and item.matches(spec.experiment_id) for item in excludes)
    )


def choose_experiment(catalog: Catalog, seed: int, excludes: Sequence[Exclusion] = ()) -> str:
    """The experiment a seed draws from the opted-in ones that are not excluded."""

    candidates = opted_in(catalog, excludes)
    if not candidates:
        raise GeneratorError("no installed experiment declares metadata.generator (or every one is excluded)")
    return generator_rng(seed, "experiment", 0).choice(candidates)


def _hooks(catalog: Catalog, ids: Sequence[str]) -> dict[str, Callable[..., Any]]:
    hooks = {}
    for hook_id in ids:
        registration = catalog.generators.get(hook_id)
        if registration is None:
            raise GeneratorError(f"generator {hook_id!r} is not registered")
        hooks[hook_id] = registration.hook
    return hooks


def generate(
    catalog: Catalog,
    seed: int,
    *,
    compile_fn: Callable[[Mapping[str, Any]], ExecutionPlan],
    experiment: Optional[str] = None,
    excludes: Sequence[Exclusion] = (),
    base: Optional[Mapping[str, Any]] = None,
    profile: Optional[Profile] = None,
    valkey_binary: Optional[str] = None,
    keep_own_binary: bool = False,
    pinned: Optional[Mapping[str, Any]] = None,
) -> Generated:
    """A valid scenario for ``experiment`` (drawn from the opted-in ones when None) from ``seed``.

    ``base`` is a partial scenario merged over every draw and hook; ``profile``
    is the search profile that samples the experiment's legal space; ``pinned``
    holds values of an earlier scenario's draws (:attr:`Generated.draws`) that
    this one keeps, every other dimension drawn from ``seed``;
    ``valkey_binary`` is the binary the deployment runs, set before the hooks
    run (:func:`~valkey_fuzzer.compiler.with_valkey_binary`); with
    ``keep_own_binary``, only when the base names none.
    ``compile_fn`` compiles (and may statically validate) a candidate; one it
    rejects (a ``ValueError``, as every compile or config error is, or the
    :class:`~valkey_fuzzer.runner.RunnerError` of a failed plan check in the
    preflight) is redrawn; any other exception is a bug and raises. Raises :class:`GeneratorError`
    when no attempt passes it, the exclusions, and the reject rules.
    """

    experiment = experiment or choose_experiment(catalog, seed, excludes)
    spec = catalog.experiments.resolve(experiment)
    block = GeneratorBlock.parse(spec, installed_pools(catalog))
    hooks = _hooks(catalog, block.hook_ids)

    def with_binary(scenario: Mapping[str, Any]) -> dict[str, Any]:
        return with_valkey_binary(catalog, scenario, valkey_binary, override=not keep_own_binary)

    last = "no attempt was made"
    for attempt in range(MAX_ATTEMPTS):
        scenario, boundary, draws = block.draw(
            seed,
            attempt,
            hooks=hooks,
            excludes=excludes,
            base=base,
            profile=profile,
            with_binary=with_binary,
            pinned=pinned,
        )
        try:
            plan = compile_fn(scenario)
        except (ValueError, RunnerError) as exc:
            # The runner's plan check fails from a compile error; any other runner error (a deployment
            # provider factory that raises) is no fault of the candidate.
            if isinstance(exc, RunnerError) and not isinstance(exc.__cause__, ScenarioCompileError):
                raise
            last = f"is invalid: {exc}"
            continue
        reason = block.rejected(plan, excludes)
        if reason is not None:
            last = reason
            continue
        return Generated(
            scenario, plan, seed, attempt, boundary, tuple(excludes), None if profile is None else profile.name, draws
        )
    raise GeneratorError(
        f"no valid {experiment} scenario from seed {seed} in {MAX_ATTEMPTS} attempts; the last one {last}"
    )
