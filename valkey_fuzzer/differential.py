"""Differential and metamorphic execution of one seeded scenario.

A differential document runs the same base scenario (same seed, so the same
generated workload) under several variants, then compares selected evidence
fields.  ``equal`` rules hold externally visible behaviour constant (dataset
digests, client replies, reloaded snapshots); ``differ`` rules require the
path-specific evidence to change (e.g. the save type), proving each variant
actually exercised a different path.

    differential_id: fork-vs-forkless
    base: {<scenario>}
    variants:
      fork:
        deployment.server_config: {bgsave-default-method: fork, ...}
      forkless:
        deployment.server_config: {bgsave-default-method: forkless, ...}
    compare:
      equal:
        - {evidence_id: valkey.dataset.digest, source: dataset, field: digest}
      differ:
        - {evidence_id: valkey.bgsave.result, source: bgsave, field: after.rdb_last_bgsave_type}

Evidence IDs and fields are opaque here; the experiment defines them.
"""

import copy
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

from ._data import to_plain_data
from .catalog import ComponentRegistry
from .documents import ABSENT, SCENARIO_SECTIONS, get_path, set_path
from .evidence import check_evidence_reference
from .plan import ExecutionPlan
from .results import Evidence, OutcomeStatus, RunOutcome, evidence_of
from .schema import ConfigError, ConfigField, ConfigSchema

# How a report shows a rule value whose evidence or field is absent.
MISSING_LABEL = "<missing>"


class DifferentialError(ValueError):
    """A differential document is malformed."""


_RULE = ConfigSchema(
    {
        "evidence_id": ConfigField(str),
        "source": ConfigField(str, default=None, allow_none=True),
        "field": ConfigField(str, default=None, allow_none=True),
    }
)
_RULES = ConfigField(list, items=ConfigField(dict, schema=_RULE), default=list)
_DOCUMENT = ConfigSchema(
    {
        "differential_id": ConfigField(str),
        "base": ConfigField(dict),
        "variants": ConfigField(dict, schema=ConfigSchema(additional=ConfigField(dict))),
        "compare": ConfigField(
            dict,
            default=dict,
            allow_none=True,
            schema=ConfigSchema(
                {
                    "equal": _RULES,
                    "differ": _RULES,
                    # Not ERROR: a variant's ERROR makes the differential ERROR before this is compared.
                    "require_status": ConfigField(
                        str,
                        choices=[status.value for status in OutcomeStatus if status is not OutcomeStatus.ERROR],
                        default=OutcomeStatus.PASS.value,
                    ),
                }
            ),
        ),
    }
)


@dataclass(frozen=True)
class CompareRule:
    evidence_id: str
    source: Optional[str]
    field: Optional[str]

    def label(self) -> str:
        source = f"@{self.source}" if self.source else ""
        path = f".{self.field}" if self.field else ""
        return f"{self.evidence_id}{source}{path}"


@dataclass
class Differential:
    differential_id: str
    base: dict
    variants: dict
    equal: list = field(default_factory=list)
    differ: list = field(default_factory=list)
    require_status: str = OutcomeStatus.PASS.value

    @classmethod
    def from_document(cls, document: Mapping[str, Any]) -> "Differential":
        try:
            document = _DOCUMENT.resolve(document, "differential")
        except ConfigError as exc:
            raise DifferentialError(str(exc)) from None
        if not document["differential_id"]:
            raise DifferentialError("differential.differential_id must name the differential")
        base, variants = document["base"], document["variants"]
        if not {"experiment", "deployment", "seed"} <= set(base):
            raise DifferentialError("differential.base must be a scenario with seed, experiment, and deployment")
        if len(variants) < 2:
            raise DifferentialError("differential.variants must map at least two variant names to patches")
        for name, patch in variants.items():
            if not name.replace("-", "").replace("_", "").isalnum():
                raise DifferentialError(f"variant name {name!r} must be alphanumeric (with - or _)")
            for path in patch:
                if path.split(".")[0] not in SCENARIO_SECTIONS:
                    raise DifferentialError(
                        f"variant {name!r} path {path!r} must start with one of {list(SCENARIO_SECTIONS)}"
                    )
                # Only the inputs may change: replacing ``experiment`` or patching
                # its ID would run a different experiment.
                if path.split(".")[0] == "experiment" and path.split(".")[1:2] != ["inputs"]:
                    raise DifferentialError(
                        f"variant {name!r} path {path!r}: variants must run the same experiment, "
                        "so they may only patch experiment.inputs"
                    )
        compare = document["compare"] or {"equal": [], "differ": [], "require_status": OutcomeStatus.PASS.value}
        equal = [CompareRule(**rule) for rule in compare["equal"]]
        differ = [CompareRule(**rule) for rule in compare["differ"]]
        if not equal and not differ:
            raise DifferentialError("differential.compare needs at least one equal or differ rule")
        return cls(
            differential_id=document["differential_id"],
            base=base,
            variants=variants,
            equal=equal,
            differ=differ,
            require_status=compare["require_status"],
        )

    def scenario(self, variant: str) -> dict:
        scenario = copy.deepcopy(self.base)
        for path, value in self.variants[variant].items():
            set_path(scenario, path, copy.deepcopy(value))
        base_id = str(scenario.get("scenario_id") or self.differential_id)
        scenario["scenario_id"] = f"{base_id}-{variant}"
        return scenario


def observed(rule: CompareRule, evidence: Sequence[Evidence]) -> Any:
    """The rule's comparable value: one value, a sorted list, or ``ABSENT``.

    ``ABSENT`` when no evidence matches, or when any matching item lacks the
    field: dropping that item would compare only the items that have it.
    """

    values = [
        get_path(to_plain_data(item.value), rule.field or "")
        for item in evidence_of(evidence, rule.evidence_id, rule.source)
    ]
    if not values or any(value is ABSENT for value in values):
        return ABSENT
    if len(values) == 1:
        return values[0]
    return sorted(values, key=lambda item: json.dumps(item, sort_keys=True, default=str))


def compare_outcomes(differential: Differential, outcomes: Mapping[str, RunOutcome]) -> list[dict[str, Any]]:
    comparisons = []
    for kind, rules in (("equal", differential.equal), ("differ", differential.differ)):
        for rule in rules:
            values = {name: observed(rule, outcome.all_evidence) for name, outcome in outcomes.items()}
            present = [value for value in values.values() if value is not ABSENT]
            distinct = {json.dumps(value, sort_keys=True, default=str) for value in present}
            if len(present) != len(values):
                result = "missing"
            elif kind == "equal":
                result = "ok" if len(distinct) == 1 else "mismatch"
            else:
                result = "ok" if len(distinct) == len(values) else "unexpectedly_equal"
            shown = {name: MISSING_LABEL if value is ABSENT else value for name, value in values.items()}
            comparisons.append({"kind": kind, "rule": rule.label(), "result": result, "values": shown})
    return comparisons


def differential_status(
    differential: Differential,
    outcomes: Mapping[str, RunOutcome],
    comparisons: Sequence[Mapping[str, Any]],
) -> tuple[str, str]:
    statuses = {name: outcome.status.value for name, outcome in outcomes.items()}
    # A variant's own FAIL is a trustworthy verdict even if another variant
    # could not run, so it comes before an ERROR, as in a batch.
    failed = [name for name, status in statuses.items() if status == "FAIL" and differential.require_status != "FAIL"]
    if failed:
        return "FAIL", f"variant(s) failed: {', '.join(failed)}"
    if "ERROR" in statuses.values():
        failing = [name for name, status in statuses.items() if status == "ERROR"]
        return "ERROR", f"variant(s) errored: {', '.join(failing)}"
    if "SKIP_UNSUPPORTED" in statuses.values() and differential.require_status != "SKIP_UNSUPPORTED":
        skipped = [name for name, status in statuses.items() if status == "SKIP_UNSUPPORTED"]
        return "SKIP_UNSUPPORTED", f"variant(s) unsupported: {', '.join(skipped)}"
    off = [name for name, status in statuses.items() if status != differential.require_status]
    if off:
        return "FAIL", f"variant(s) did not reach {differential.require_status}: " + ", ".join(
            f"{name}={statuses[name]}" for name in off
        )
    # A rule's evidence is checked against the plans before anything runs, so a ``missing`` value means a
    # variant did not record it: the comparison could not be made, which says nothing about the server.
    broken = [item for item in comparisons if item["result"] not in ("ok", "missing")]
    if broken:
        return "FAIL", "; ".join(f"{item['rule']} {item['result']}" for item in broken)
    missing = [item for item in comparisons if item["result"] == "missing"]
    if missing:
        return "ERROR", "; ".join(f"{item['rule']} missing" for item in missing)
    return "PASS", f"{len(comparisons)} comparison(s) hold across {len(outcomes)} variants"


def check_rules(differential: Differential, name: str, plan: ExecutionPlan, components: ComponentRegistry) -> None:
    """Raise :class:`DifferentialError` unless every rule's evidence can come from variant ``name``'s plan.

    A misspelt evidence ID or source would otherwise be found only after every
    variant ran, as a ``missing`` comparison.
    """

    for kind, rules in (("equal", differential.equal), ("differ", differential.differ)):
        for rule in rules:
            try:
                check_evidence_reference(plan, components, rule.evidence_id, rule.source, f"compare.{kind} evidence")
            except ValueError as exc:
                raise DifferentialError(f"variant {name!r}: {exc}") from None


def run_differential(
    differential: Differential,
    *,
    compile_fn: Callable[[dict], ExecutionPlan],
    execute_fn: Callable[[str, dict, ExecutionPlan], tuple[RunOutcome, Optional[str]]],
    components: ComponentRegistry,
    on_variant: Optional[Callable[[str, RunOutcome], None]] = None,
) -> dict[str, Any]:
    """Compile every variant and check its rules against the plan first (no mutation on a bad variant), then run
    them in order."""

    plans = {}
    scenarios = {}
    for name in differential.variants:
        scenarios[name] = differential.scenario(name)
        plans[name] = compile_fn(scenarios[name])
        check_rules(differential, name, plans[name], components)
    seeds = {plan.seed for plan in plans.values()}
    if len(seeds) != 1:
        raise DifferentialError("variants must share the base seed so they generate the same workload")
    outcomes: dict[str, RunOutcome] = {}
    variants = {}
    for name, plan in plans.items():
        outcome, artifact_dir = execute_fn(name, scenarios[name], plan)
        outcomes[name] = outcome
        variants[name] = {
            "status": outcome.status.value,
            "summary": outcome.summary,
            "artifact_dir": artifact_dir,
            "scenario": scenarios[name],
        }
        if on_variant is not None:
            on_variant(name, outcome)
    comparisons = compare_outcomes(differential, outcomes)
    status, summary = differential_status(differential, outcomes, comparisons)
    return {
        "differential_id": differential.differential_id,
        "status": status,
        "summary": summary,
        "seed": next(iter(seeds)),
        "variants": variants,
        "comparisons": comparisons,
    }
