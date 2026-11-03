"""Run provenance: the reproducer a run report records, the source digests it compares, and how a replay here
differs from the recorded run.

Everything here is a function of the catalog: what is installed, and the code that ran.
"""

import hashlib
import platform
import sys
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any, Callable, Optional

from ._data import canonical_json, file_sha256, to_plain_data
from .catalog import Catalog, UnknownExperimentError
from .describe import components_by_role, implementation_name
from .plan import ExecutionPlan
from .registrations import DeploymentProviderRegistration
from .results import RunOutcome


def _implementation(factory: Any) -> dict[str, str]:
    return {"implementation": implementation_name(factory)}


def _top_package(target: Any) -> str:
    return str(getattr(getattr(target, "func", target), "__module__", "") or "").split(".")[0]


def source_digest(packages: Iterable[str]) -> Optional[str]:
    """SHA-256 over every file of each named top-level package (its data files too).

    The whole package, not only the module defining a class, because a
    component's behavior also comes from the helpers it calls and the core
    that judges it. None when a package is not imported or a file cannot be read.
    """

    digest = hashlib.sha256()
    for package in sorted(set(packages)):
        path = getattr(sys.modules.get(package), "__file__", None)
        if not path:
            return None
        root = Path(path)
        if root.name == "__init__.py":
            root = root.parent
            files = sorted(
                file
                for file in root.rglob("*")
                if file.is_file() and "__pycache__" not in file.parts and file.suffix != ".pyc"
            )
        else:
            files = [root]
        for file in files:
            content = file_sha256(str(file))
            if content is None:
                return None
            name = file.name if file == root else file.relative_to(root).as_posix()
            digest.update(f"{package}/{name}\0{content}\n".encode())
    return digest.hexdigest()


def _fuzzer_digest() -> Optional[str]:
    return source_digest([__name__.split(".")[0]])


def _experiment_digest(experiment: Any) -> str:
    return hashlib.sha256(canonical_json(experiment).encode()).hexdigest()


def implementation_digest(factory: Any) -> Optional[str]:
    """:func:`source_digest` of the top-level package that defines ``factory``."""

    return source_digest([_top_package(factory)])


def catalog_digest(catalog: Catalog) -> str:
    """SHA-256 over the code and experiments a draw depends on: the fuzzer and every experiment.

    A batch records it, so a resume knows whether its random jobs would
    draw the scenarios their seeds drew when the batch was created.
    """

    material = {
        "fuzzer": _fuzzer_digest(),
        "experiments": {
            experiment_id: _experiment_digest(experiment)
            for experiment_id, experiment in catalog.experiments.snapshot().items()
        },
    }
    return hashlib.sha256(canonical_json(material).encode()).hexdigest()


def reproducer(catalog: Catalog, plan: ExecutionPlan, outcome: Optional[RunOutcome] = None) -> dict[str, Any]:
    """Report-safe provenance needed to reproduce a run exactly."""

    used = {component.use for component in plan.components}
    # Their code is compared through the fuzzer's source digest.
    components = [
        {"component_id": entry.component_id, "role": entry.role.value, **_implementation(entry.factory)}
        for entry in components_by_role(catalog)
        if entry.component_id in used
    ]
    provider = catalog.deployment_providers.get((plan.deployment_mode, plan.deployment_provider.provider_id))
    experiment_digest = None
    try:
        experiment_digest = _experiment_digest(catalog.experiments.resolve(plan.experiment_id))
    except UnknownExperimentError:
        # A bare plan replayed where its experiment is not installed; the drift check reports it.
        pass
    return {
        "python": sys.version.split()[0],
        "platform": platform.platform(),
        "experiment": {"id": plan.experiment_id, "digest": experiment_digest},
        # Replay compares all the code that ran: the fuzzer's package, the Valkey components included.
        "fuzzer_digest": _fuzzer_digest(),
        "components": components,
        # The plan locks the provider's ID; this records which code ran it.
        "deployment_provider": None
        if provider is None
        else {
            "provider_id": provider.provider_id,
            **_implementation(provider.factory),
            "implementation_digest": implementation_digest(provider.factory),
        },
        "plan_digest": hashlib.sha256(plan.to_json().encode("utf-8")).hexdigest(),
        "valkey_sha": outcome.valkey_sha if outcome is not None else None,
    }


# ---------------------------------------------------------------------------
# Replay drift
# ---------------------------------------------------------------------------


def _drift(item: str, recorded: Any, replay: Any) -> dict[str, Any]:
    return {"item": item, "recorded": recorded, "replay": replay}


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _rows(value: Any) -> list[Mapping[str, Any]]:
    return [row for row in value if isinstance(row, Mapping)] if isinstance(value, list) else []


def artifact_digests(outcome: Any) -> dict[str, Optional[str]]:
    """Each resolved artifact's ``provenance.sha256`` (None without one), by dependency path, from a plain outcome.

    An artifact can feed a run without any instance starting on it (an
    upgrade target, a file to load), so replay compares these as well as the
    instances' binaries.
    """

    diagnostics = _mapping(_mapping(outcome).get("diagnostics"))
    digests: dict[str, Optional[str]] = {}
    for record in _rows(_mapping(diagnostics.get("environment_report")).get("records")):
        dependency = record.get("dependency")
        if not (isinstance(dependency, str) and dependency.startswith("artifacts.")):
            continue
        if record.get("phase") == "acquire" and record.get("outcome") == "succeeded":
            result = _mapping(_mapping(record.get("details")).get("result"))
            digest = _mapping(result.get("provenance")).get("sha256")
            digests[dependency] = digest if isinstance(digest, str) and digest else None
    return digests


def provenance_drift(
    catalog: Catalog, plan: ExecutionPlan, recorded: Mapping[str, Any]
) -> tuple[list[dict[str, Any]], list[str]]:
    """How replaying ``plan`` here differs from a report's ``reproducer``, and what could not be compared.

    The Valkey binary is compared separately, by the deployment provider's ``binary_drift`` hook.
    """

    current = reproducer(catalog, plan)
    drift: list[dict[str, Any]] = []
    unchecked: list[str] = []
    if recorded["plan_digest"] != current["plan_digest"]:
        drift.append(_drift("plan digest", recorded["plan_digest"], current["plan_digest"]))

    old_digest = _mapping(recorded["experiment"]).get("digest")
    if old_digest != current["experiment"]["digest"]:
        label = f"experiment {plan.experiment_id} digest"
        drift.append(_drift(label, old_digest, current["experiment"]["digest"]))

    def compare_digest(label: str, old_code: Any, new_code: Optional[str]) -> None:
        if old_code is None or new_code is None:
            unchecked.append(f"{label} code: its source cannot be read")
        elif old_code != new_code:
            drift.append(_drift(f"{label} code", old_code, new_code))

    def same_implementation(label: str, old_row: Any, new_row: Optional[Mapping[str, Any]]) -> bool:
        old = old_row.get("implementation") if isinstance(old_row, Mapping) else None
        new = None if new_row is None else new_row["implementation"]
        if old != new:
            drift.append(_drift(label, old, new))
        return old == new

    compare_digest("fuzzer", recorded["fuzzer_digest"], current["fuzzer_digest"])
    # A malformed list compares as empty, so every entry it should hold drifts.
    before = {row.get("component_id"): row for row in _rows(recorded["components"])}
    after = {row["component_id"]: row for row in current["components"]}
    for component_id in sorted({component.use for component in plan.components}):
        same_implementation(f"component {component_id}", before.get(component_id), after.get(component_id))

    registration = catalog.deployment_providers.get((plan.deployment_mode, plan.deployment_provider.provider_id))
    locked = to_plain_data(plan.deployment_provider)
    available = None if registration is None else to_plain_data(registration.identity)
    label = f"deployment provider for {plan.deployment_mode}"
    if available != locked:
        # The runner refuses this plan whatever --allow-drift says.
        drift.append(_drift(label, locked, available))
    else:
        old_row, new_row = recorded["deployment_provider"], current["deployment_provider"]
        if same_implementation(label, old_row, new_row) and new_row is not None:
            compare_digest(label, old_row.get("implementation_digest"), new_row["implementation_digest"])
    return drift, unchecked


def _recorded_instances(
    registration: Optional[DeploymentProviderRegistration], outcome: Mapping[str, Any]
) -> list[Mapping[str, Any]]:
    """The per-instance provenance the deployment provider left in a run's (plain) ``outcome``."""

    if registration is None or registration.recorded_instances is None:
        return []
    return list(registration.recorded_instances(outcome))


def report_drift(
    catalog: Catalog,
    compile_fn: Callable[[Mapping[str, Any]], ExecutionPlan],
    plan: ExecutionPlan,
    scenario: Optional[Mapping[str, Any]],
    recorded: Mapping[str, Any],
    valkey_binary: Optional[str],
) -> tuple[list[dict[str, Any]], list[str]]:
    """How running a report's plan here differs from the run it records, and what could not be compared.

    Compared before anything runs: the reproducer, the scenario against
    the plan, and each instance's binary (``valkey_binary`` overrides the
    default one on purpose). ``compile_fn`` compiles the scenario here.
    """

    drift, unchecked = provenance_drift(catalog, plan, recorded["reproducer"])
    mismatch = _scenario_mismatch(compile_fn, plan, scenario)
    if mismatch is not None:
        drift.append(mismatch)
    elif scenario is None:
        unchecked.append("scenario: not recorded")
    registration = catalog.deployment_providers.of_plan(plan)
    recorded_instances = _recorded_instances(registration, recorded["outcome"])
    if registration is None or registration.binary_drift is None:
        binaries, unchecked_binaries = [], ["Valkey binary: its deployment provider does not compare binaries"]
    else:
        binaries, unchecked_binaries = registration.binary_drift(plan, recorded_instances, override=valkey_binary)
        if not recorded_instances and not valkey_binary:
            unchecked_binaries.append("Valkey binary: the recorded run has no per-instance binary digests")
    unchecked_binaries.extend(
        f"{dependency}: the recorded run has no sha256 for it"
        for dependency, digest in sorted(artifact_digests(recorded["outcome"]).items())
        if digest is None
    )
    drift += binaries
    return drift, unchecked + unchecked_binaries


def _scenario_mismatch(
    compile_fn: Callable[[Mapping[str, Any]], ExecutionPlan], plan: ExecutionPlan, scenario: Optional[Mapping[str, Any]]
) -> Optional[dict[str, Any]]:
    """Drift when a report's scenario does not compile, here, to the plan it records.

    Replay runs the plan, and minimize and later replays read the scenario,
    so a report is reproduced only if both describe the same run.
    """

    if scenario is None:
        return None
    item = "the report's scenario and plan"
    try:
        compiled = compile_fn(scenario)
    except Exception as exc:
        return _drift(item, "the recorded plan", f"the scenario does not compile here: {exc}")
    recorded, replayed = plan.to_data(), compiled.to_data()
    differing = sorted(key for key in {*recorded, *replayed} if recorded.get(key) != replayed.get(key))
    if not differing:
        return None

    def shown(data: Mapping[str, Any]) -> dict[str, Any]:
        return {
            key: data.get(key) if not isinstance(data.get(key), (Mapping, list, tuple)) else "<differs>"
            for key in differing
        }

    return _drift(item, f"plan {shown(recorded)}", f"scenario compiles to {shown(replayed)}")


def replayed_drift(
    catalog: Catalog,
    plan: ExecutionPlan,
    recorded: Mapping[str, Any],
    override: Optional[str],
    outcome: RunOutcome,
) -> list[dict[str, Any]]:
    """How a replay's own run differs from the run it replayed, in what only running it reveals.

    ``recorded`` holds the report's ``reproducer`` and ``outcome``;
    ``override`` is the replay's ``valkey_binary`` override, if any.
    """

    registration = catalog.deployment_providers.of_plan(plan)
    recorded_instances = _recorded_instances(registration, recorded["outcome"])
    after: list[dict[str, Any]] = []
    if registration is not None and registration.binary_drift is not None:
        # A binary an environment provider supplies is only known once the
        # deployment runs; the replay's own deployment metadata settles every instance.
        after, _ = registration.binary_drift(
            plan,
            recorded_instances,
            override=override,
            replayed=_recorded_instances(registration, outcome.to_data()),
        )
    recorded_sha = recorded["reproducer"].get("valkey_sha")
    if not recorded_instances and not override and recorded_sha and outcome.valkey_sha:
        if outcome.valkey_sha != recorded_sha:
            after.append(_drift("Valkey git SHA", recorded_sha, outcome.valkey_sha))
    # Artifacts resolve only when the run starts, so they are compared after it.
    replayed_artifacts = artifact_digests(outcome.to_data())
    for dependency, digest in sorted(artifact_digests(recorded["outcome"]).items()):
        replayed_digest = replayed_artifacts.get(dependency)
        if digest and replayed_digest and replayed_digest != digest:
            after.append(_drift(f"{dependency} sha256", digest, replayed_digest))
    return after
