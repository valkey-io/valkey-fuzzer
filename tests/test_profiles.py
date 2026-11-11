"""Search profiles: they change how an experiment's legal space is sampled, never what is legal or checked."""

import json
from unittest.mock import patch

import pytest

from valkey_fuzzer.cli import main
from valkey_fuzzer.generator import GeneratorError, Profile, _Rule, find_profile, generate, opted_in
from valkey_fuzzer.protocols import ComponentRole

from .generator_support import generator_service

pytestmark = pytest.mark.usefixtures("report_root")


def _generate(experiment, seed, profile=None):
    service = generator_service()
    return generate(service.catalog, seed, compile_fn=service._checked_plan, experiment=experiment, profile=profile)


@pytest.mark.parametrize(
    ("document", "message"),
    [
        *[
            ({field: {}}, rf"unknown fields: \['{field}'\]")
            for field in ("reject", "oracles", "components", "inputs", "hook")
        ],
        ({"edge_probability": 1.5}, "edge_probability must be at most 1"),
        ({"ranges": {"inputs.operations": {"minimum": 9, "maximum": 1}}}, "minimum <= maximum"),
        ({"ranges": {"server_config.appendonly": {"maximum": 1}}}, "must start with deployment., inputs."),
        ({"weights": {"deployment.mode": {"standalone": 0}}}, "greater than zero"),
        ({"budget": {"seeds": 0}}, "at least 1"),
    ],
)
def test_a_malformed_profile_or_one_touching_validity_is_rejected(document, message):
    with pytest.raises(GeneratorError, match=message):
        Profile.parse("bad", document)


def test_a_profile_range_is_clamped_into_the_legal_range_and_never_widens_it():
    legal = _Rule(minimum=10, maximum=100, integer=True, edges_count=True)
    for wanted, clamped in [({"minimum": -5, "maximum": 10**9}, (10, 100)), ({"minimum": 500}, (100, 100)),
                            ({"maximum": 20}, (10, 20))]:  # fmt: skip
        rule = Profile.parse("p", {"ranges": {"inputs.n": wanted}}).rule(legal, "inputs.n")
        assert (rule.minimum, rule.maximum) == clamped
    # A path the experiment does not draw changes nothing.
    assert Profile.parse("p", {"ranges": {"inputs.n": {"maximum": 20}}}).rule(legal, "inputs.other") is legal


def test_weights_never_add_a_value_the_experiment_does_not_list():
    profile = Profile.parse("w", {"weights": {"deployment.mode": {"sharded": 1000, "cluster": 1000}}})
    modes = {_generate("valkey.replication-partial-resync", seed, profile).plan.deployment_mode for seed in range(30)}
    assert "cluster" in modes and modes <= {"standalone", "cluster"}
    # Replication chaos lists only standalone: no weight makes it cluster.
    assert {_generate("valkey.replication-chaos", seed, profile).plan.deployment_mode for seed in range(5)} == {
        "standalone"
    }


def test_an_edge_heavy_profile_draws_more_edges_and_changes_no_experiments_oracles():
    catalog = generator_service().catalog
    adversarial = find_profile(catalog, "adversarial")
    plain = sum(_generate("valkey.replication-chaos", seed).boundary for seed in range(40))
    assert sum(_generate("valkey.replication-chaos", seed, adversarial).boundary for seed in range(40)) > plain

    def oracles(plan):
        return sorted((item.id, item.use) for item in plan.components if item.role is ComponentRole.ORACLE)

    for experiment in opted_in(catalog):
        profiled = _generate(experiment, 3, adversarial)
        assert oracles(profiled.plan) == oracles(_generate(experiment, 3).plan), experiment
        assert profiled.provenance["profile"] == "adversarial"
        assert _generate(experiment, 3, adversarial).scenario == profiled.scenario, "a profile draw is reproducible"


def _main(*argv):
    with patch("sys.argv", ["valkey-fuzzer", *argv]):
        return main()


def test_run_records_the_profile_and_prints_how_to_reproduce_it(tmp_path, canned_runs, capsys):
    assert _main("run", "--seed", "4", "--experiment", "valkey.aof-durability", "--profile", "pr-quick") == 0
    (report,) = (tmp_path / "valkey-fuzzer" / "runs").glob("*/report.json")
    assert json.loads(report.read_text())["reproducer"]["generated"]["profile"] == "pr-quick"
    out = capsys.readouterr().out
    assert "Reproduce: valkey-fuzzer run --seed 4 --experiment valkey.aof-durability --profile pr-quick" in out
    assert _main("run", "--seed", "4", "--experiment", "valkey.aof-durability", "--profile", "fast") == 2
    assert "no such search profile 'fast'; installed: ['adversarial'" in capsys.readouterr().err
