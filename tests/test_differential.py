"""Differential execution: variants of one scenario, the evidence they must agree or differ on, and the verdict."""

import pytest

from tests.support import make_plan
from tests.valkey_experiments_support import valkey_service
from valkey_fuzzer.components.workloads import replies_digest
from valkey_fuzzer.differential import (
    Differential,
    DifferentialError,
    compare_outcomes,
    differential_status,
    run_differential,
)
from valkey_fuzzer.results import Evidence, OutcomeStatus, RunOutcome

DOCUMENT = {
    "differential_id": "unit",
    "base": {
        "scenario_id": "base",
        "seed": 5,
        "experiment": {"id": "valkey.snapshot-equivalence", "inputs": {"operations": 10}},
        "deployment": {"mode": "standalone"},
    },
    "variants": {
        "a": {"deployment.server_config": {"appendonly": "no"}},
        "b": {"deployment.server_config": {"appendonly": "yes"}},
    },
    "compare": {
        "equal": [{"evidence_id": "valkey.dataset.digest", "source": "dataset", "field": "digest"}],
        "differ": [{"evidence_id": "valkey.bgsave.result", "field": "after.type"}],
    },
}


def _outcome(status=OutcomeStatus.PASS, digest="d1", save_type="fork", extra=()):
    evidence = (
        Evidence("valkey.dataset.digest", "dataset", "oracle.completed", 1.0, {"digest": digest}),
        Evidence("valkey.bgsave.result", "bgsave", "action.completed", 1.0, {"after": {"type": save_type}}),
        *extra,
    )
    return RunOutcome(plan=make_plan(), status=status, summary=status.value, evidence=evidence)


def test_variants_patch_the_base_and_share_its_seed():
    variants = {"a": {"deployment.server_config": {"appendonly": "no"}}, "b": {"experiment.inputs.operations": 30}}
    differential = Differential.from_document({**DOCUMENT, "variants": variants})
    a, b = differential.scenario("a"), differential.scenario("b")
    assert (a["scenario_id"], b["scenario_id"], a["seed"], b["seed"]) == ("base-a", "base-b", 5, 5)
    assert a["deployment"]["server_config"] == {"appendonly": "no"} and "server_config" not in b["deployment"]
    assert a["experiment"]["inputs"] == {"operations": 10} and b["experiment"]["inputs"] == {"operations": 30}


@pytest.mark.parametrize(
    ("patch", "message"),
    [
        ({"variants": {"only": {}}}, "at least two"),
        ({"variants": {"a": {"seed": 1}, "b": {}}}, "must start with"),
        ({"variants": {"a": {"experiment.id": "valkey.forkless-save"}, "b": {}}}, "may only patch experiment.inputs"),
        ({"compare": {}}, "at least one"),
        ({"compare": {"equal": [{"evidence_id": "a.b"}], "require_status": "MAYBE"}}, "require_status"),
        # An ERROR variant makes the differential ERROR before require_status is looked at.
        ({"compare": {"equal": [{"evidence_id": "a.b"}], "require_status": "ERROR"}}, "require_status"),
    ],
)
def test_a_malformed_differential_document_is_rejected(patch, message):
    with pytest.raises(DifferentialError, match=message):
        Differential.from_document({**DOCUMENT, **patch})


@pytest.mark.parametrize(
    ("a", "b", "results", "status"),
    [
        (_outcome(save_type="fork"), _outcome(save_type="forkless"), ["ok", "ok"], "PASS"),
        (_outcome(digest="d1"), _outcome(digest="d2", save_type="forkless"), ["mismatch", "ok"], "FAIL"),
        (_outcome(save_type="fork"), _outcome(save_type="fork"), ["ok", "unexpectedly_equal"], "FAIL"),
        # A variant that recorded no evidence leaves the comparison unmade: the harness could not tell.
        (_outcome(), RunOutcome(plan=make_plan(), status=OutcomeStatus.PASS, summary="x"), ["missing", "missing"],
         "ERROR"),
        # A mismatch is a finding even when another rule could not be compared.
        (_outcome(), RunOutcome(plan=make_plan(), status=OutcomeStatus.PASS, summary="x", evidence=_outcome(digest="d2").evidence[:1]),
         ["mismatch", "missing"], "FAIL"),
        (_outcome(OutcomeStatus.ERROR), _outcome(save_type="forkless"), ["ok", "ok"], "ERROR"),
        # A variant's own FAIL is not hidden behind the other's SKIP, and it names the variant.
        (_outcome(OutcomeStatus.FAIL), _outcome(OutcomeStatus.SKIP_UNSUPPORTED, save_type="forkless"), ["ok", "ok"],
         "FAIL"),
        # Nor behind the other's ERROR, as in a batch.
        (_outcome(OutcomeStatus.FAIL), _outcome(OutcomeStatus.ERROR, save_type="forkless"), ["ok", "ok"], "FAIL"),
    ],
    ids=["agree", "digest-mismatch", "unexpectedly-equal", "missing-evidence", "mismatch-and-missing", "error",
         "variant-fail", "variant-fail-and-error"],
)  # fmt: skip
def test_the_comparisons_and_the_variant_statuses_decide_the_verdict(a, b, results, status):
    differential = Differential.from_document(DOCUMENT)
    outcomes = {"a": a, "b": b}
    comparisons = compare_outcomes(differential, outcomes)
    assert [item["result"] for item in comparisons] == results
    verdict, summary = differential_status(differential, outcomes, comparisons)
    assert verdict == status
    if status == "FAIL" and a.status is OutcomeStatus.FAIL:
        assert "a" in summary


def test_evidence_whose_value_is_the_missing_label_is_compared_as_present():
    # A real "<missing>" value is present: scored as absent, the run would be ERROR.
    differential = Differential.from_document(DOCUMENT)
    outcomes = {"a": _outcome(digest="<missing>"), "b": _outcome(digest="<missing>", save_type="forkless")}
    comparisons = compare_outcomes(differential, outcomes)
    assert [item["result"] for item in comparisons] == ["ok", "ok"]
    nothing = RunOutcome(plan=make_plan(), status=OutcomeStatus.PASS, summary="x")
    absent = compare_outcomes(differential, {**outcomes, "b": nothing})
    assert absent[0]["result"] == "missing" and absent[0]["values"]["b"] == "<missing>"


def test_every_variant_compiles_before_any_runs_and_the_report_names_each_variant():
    runtime = valkey_service()
    executed = []
    broken = {**DOCUMENT, "variants": {"a": {}, "b": {"experiment.inputs.operations": -1}}}
    with pytest.raises(Exception, match=r"experiment\.inputs\.operations must be at least 1, got -1"):
        run_differential(
            Differential.from_document(broken),
            compile_fn=runtime.compile,
            execute_fn=lambda name, scenario, plan: executed.append(name),
            components=runtime.catalog.components,
        )
    assert executed == []

    outcomes = iter([_outcome(save_type="fork"), _outcome(save_type="forkless")])
    execute = lambda name, scenario, plan: (next(outcomes), f"/runs/{name}")  # noqa: E731
    report = run_differential(
        Differential.from_document(DOCUMENT),
        compile_fn=runtime.compile,
        execute_fn=execute,
        components=runtime.catalog.components,
    )
    assert (report["status"], report["variants"]["b"]["artifact_dir"]) == ("PASS", "/runs/b")


@pytest.mark.parametrize(
    ("rule", "message"),
    [
        ({"evidence_id": "valkey.dataset.digets", "source": "dataset"}, "'valkey.dataset.digets' is not declared by component 'dataset'"),
        ({"evidence_id": "valkey.dataset.digest", "source": "datset"}, "'valkey.dataset.digest' names source 'datset', which is not a component"),
        ({"evidence_id": "valkey.bgsave.reslt"}, "'valkey.bgsave.reslt' is not declared by any component"),
    ],
    ids=["misspelt-evidence", "misspelt-source", "misspelt-without-source"],
)  # fmt: skip
def test_a_rule_whose_evidence_no_variant_can_record_is_rejected_before_any_runs(rule, message):
    """A misspelt rule would otherwise be found only after every variant ran, as a missing comparison."""

    runtime, executed = valkey_service(), []
    document = {**DOCUMENT, "compare": {"equal": [rule]}}
    with pytest.raises(DifferentialError, match=f"variant 'a': compare.equal evidence {message}"):
        run_differential(
            Differential.from_document(document),
            compile_fn=runtime.compile,
            execute_fn=lambda name, scenario, plan: executed.append(name),
            components=runtime.catalog.components,
        )
    assert executed == []


def test_the_replies_digest_ignores_timing_but_not_replies():
    base = [{"id": 1, "op": "get", "key": "k", "outcome": "ok", "reply": "v", "t_start": 1, "t_end": 2}]
    assert replies_digest(base) == replies_digest([dict(base[0], t_start=50, t_end=60)])
    assert replies_digest(base) != replies_digest([dict(base[0], reply="w")])
