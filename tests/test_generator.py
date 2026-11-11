"""Random scenarios: the generic generator, its declarations, and the shipped experiments' generator blocks."""

import collections
import random

import pytest
import yaml

from valkey_fuzzer.catalog import Catalog, register_valkey_components
from valkey_fuzzer.components import EXPERIMENT_DIRECTORY, experiments
from valkey_fuzzer.components.generators import renegotiates
from valkey_fuzzer.deployment.local.provider import local_deployment_provider_registrations
from valkey_fuzzer.documents import merge
from valkey_fuzzer.generator import MAX_ATTEMPTS, Exclusion, GeneratorBlock, GeneratorError, generate, installed_pools
from valkey_fuzzer.registrations import GeneratorRegistration
from valkey_fuzzer.runner import RunnerError
from valkey_fuzzer.schema import ScenarioCompileError
from valkey_fuzzer.service import RunService

from .generator_support import CLUSTER_EXPERIMENT, SMALL_CLUSTER_SEEDS, generate_scenario, generator_service
from .valkey_experiments_support import valkey_service

NO_ZSTD = (Exclusion.parse("server_config.rdbcompression=zstd"), Exclusion.parse("server_config.repl-compression=zstd"))
EXPERIMENTS = sorted(spec.experiment_id for spec in experiments())
# A data-only experiment with one input of every kind the generator draws by default.
_INPUTS = {
    "properties": {
        "stable_keys": {"minimum": 0, "maximum": 2000, "default": 10},
        "codec": {"type": "string", "enum": ["none", "fast", "small"], "default": "none"},
        "flag": {"type": "boolean", "default": False},
        "count": {"type": "integer", "minimum": 0, "maximum": 10, "default": 1},
        "ratio": {"type": "number", "minimum": 0.5, "maximum": 5000, "default": 1},
        "label": {"type": "string", "default": "x"},
        "waves": {"type": "array", "default": []},
    }
}


def _runtime(tmp_path, generator, *, inputs=_INPUTS, hooks=None, **experiment):
    document = {"extends": ["valkey.writes"], "id": "tests.random", "deployment": {"modes": ["standalone", "cluster"]},
                "metadata": {} if generator is None else {"generator": generator}, "inputs": inputs, **experiment}  # fmt: skip
    directory = tmp_path / "experiments"
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "tests.random.yaml").write_text(yaml.safe_dump(document))
    registrations = tuple(
        GeneratorRegistration(f"tests.generator.{name}", hook) for name, hook in (hooks or {}).items()
    )
    catalog = Catalog()
    catalog.deployment_providers.register_many(local_deployment_provider_registrations().values())
    register_valkey_components(catalog)
    catalog.register(generators=registrations)
    catalog.load_experiment_directories([directory])
    catalog.freeze()
    return RunService(catalog=catalog)


def _generate(runtime, seed, **kwargs):
    kwargs.setdefault("experiment", "tests.random")
    return generate(runtime.catalog, seed, compile_fn=runtime.compile, **kwargs)


def test_each_draw_has_its_own_stream_keyed_by_seed_and_experiment(tmp_path):
    block = {"deployment": {"replicas_per_primary": {"minimum": 0, "maximum": 1000}}}
    runtime, renamed = _runtime(tmp_path / "a", block), _runtime(tmp_path / "b", block, id="tests.random-renamed")
    wider = {"properties": {**_INPUTS["properties"], "extra": {"type": "integer", "minimum": 0, "maximum": 99}}}
    widened = _runtime(tmp_path / "c", block, inputs=wider)
    scenarios = [_generate(runtime, seed).scenario for seed in range(20)]
    assert len({yaml.safe_dump(scenario) for scenario in scenarios}) == 20
    seen = collections.defaultdict(set)
    for seed, scenario in enumerate(scenarios):
        # Two experiments with one block do not share their draws, and adding an input changes no other draw.
        other = _generate(renamed, seed, experiment="tests.random-renamed").scenario
        assert other["experiment"]["inputs"] != scenario["experiment"]["inputs"]
        new = _generate(widened, seed).scenario
        del new["experiment"]["inputs"]["extra"]
        assert new == scenario
        for name, value in scenario["experiment"]["inputs"].items():
            seen[name].add(value)
    # Defaults draw enums, booleans, and bounded numbers only; label and waves have nothing to draw from.
    assert "label" not in seen and "waves" not in seen and seen["codec"] == {"none", "fast", "small"}
    assert all(0 <= value <= 10 for value in seen["count"]) and all(0.5 <= value <= 5000 for value in seen["ratio"])


def test_a_candidate_that_does_not_compile_is_redrawn_until_the_attempts_run_out(tmp_path):
    runtime = _runtime(tmp_path, {})
    calls = []

    def compile_fn(scenario):
        calls.append(scenario)
        if len(calls) <= 3:
            raise ValueError("never valid")
        return runtime.compile(scenario)

    generated = generate(runtime.catalog, 5, compile_fn=compile_fn, experiment="tests.random")
    assert generated.attempt == 3 and len({yaml.safe_dump(scenario) for scenario in calls}) == 4

    def never(scenario):
        raise ValueError("never valid")

    with pytest.raises(GeneratorError, match=rf"in {MAX_ATTEMPTS} attempts; the last one is invalid: never valid"):
        generate(runtime.catalog, 5, compile_fn=never, experiment="tests.random")

    # Known-bad: a bug in the compiler or a hook is raised, not redrawn as if the candidate were invalid.
    def broken(scenario):
        calls.append(scenario)
        raise TypeError("a bug")

    calls.clear()
    with pytest.raises(TypeError, match="a bug"):
        generate(runtime.catalog, 5, compile_fn=broken, experiment="tests.random")
    assert len(calls) == 1

    # So is a deployment provider whose factory fails in the preflight; only a failed plan check is the candidate's.
    def factory_fails(scenario):
        calls.append(scenario)
        if len(calls) == 1:
            raise RunnerError("plan check failed") from ScenarioCompileError("deployment selector names nothing")
        raise RunnerError("deployment provider factory for 'standalone' failed: no docker")

    calls.clear()
    with pytest.raises(RunnerError, match="factory"):
        generate(runtime.catalog, 5, compile_fn=factory_fails, experiment="tests.random")
    assert len(calls) == 2


def test_values_the_compiler_or_the_preflight_refuse_are_never_emitted(tmp_path):
    """Known-bad: appendfsync always with a loss window, and a server config value the deployment refuses."""

    document = yaml.safe_load((EXPERIMENT_DIRECTORY / "valkey.aof-durability.yaml").read_text())
    document["id"] = "tests.aof-durability"
    fixed = ("stable_keys", "mutable_keys", "mutation_rate", "value_size", "run_before_crash_s", "run_after_crash_s")
    document["metadata"]["generator"] = {
        "inputs": {**dict.fromkeys(fixed, "fixed"), "loss_window_s": {"values": [0, 5]}}
    }
    (tmp_path / "variant.yaml").write_text(yaml.safe_dump(document))
    aof = valkey_service(experiment_directories=(str(tmp_path),))
    pool = {
        "server_config": ["tests.pool"],
        "pools": {"tests.pool": {"maxmemory-policy": ["noeviction", ["a", ["b"]]]}},
    }
    policy = _runtime(tmp_path / "pool", pool)
    redrawn = collections.Counter()
    for seed in range(40):
        generated = _generate(aof, seed, experiment="tests.aof-durability")
        assert not (generated.plan.inputs["appendfsync"] == "always" and generated.plan.inputs["loss_window_s"] > 0)
        redrawn["aof"] += generated.attempt > 0
        generated = _generate(policy, seed)
        assert generated.scenario["deployment"]["server_config"]["maxmemory-policy"] == "noeviction"
        redrawn["pool"] += generated.attempt > 0
    assert redrawn["aof"] and redrawn["pool"]


def test_excluded_and_rejected_values_never_appear(tmp_path):
    """Known-bad: --exclude holds for drawn values, defaults, pools, and inputs an overlay writes into the config."""

    inputs = {"properties": {**_INPUTS["properties"], "rdb": {"type": "string", "enum": ["no", "lz4", "zstd"]}}}
    overlay = {"overlay_id": "rdb", "server_config": {"rdbcompression": {"$ref": "inputs.rdb"}}}
    block = {"server_config": ["valkey.pool.replication"], "reject": [{"inputs.codec": "small", "inputs.flag": True}]}
    runtime = _runtime(tmp_path, block, inputs=inputs, startup_overlays=[overlay])
    excludes = (Exclusion.parse("inputs.codec=fast"), Exclusion.parse("server_config.repl-diskless-sync=yes"), *NO_ZSTD)
    attempts, rdb = [], set()
    for seed in range(100):
        generated = _generate(runtime, seed, excludes=excludes)
        inputs = generated.plan.inputs
        assert inputs["codec"] != "fast" and not (inputs["codec"] == "small" and inputs["flag"] is True)
        assert generated.scenario["deployment"]["server_config"].get("repl-diskless-sync") != "yes"
        rdb.add(inputs["rdb"])
        attempts.append(generated.attempt)
    assert rdb == {"no", "lz4"} and any(attempts)
    with pytest.raises(GeneratorError, match=r"holds the excluded inputs\.label=x"):
        _generate(runtime, 1, excludes=(Exclusion.parse("inputs.label=x"),))
    # A server config key holds what the planned instances start with, the deployment provider's defaults too.
    defaults = _runtime(tmp_path / "defaults", {"reject": [{"server_config.appendonly": "yes"}]})
    with pytest.raises(GeneratorError, match=r"matches the reject rule"):
        _generate(defaults, 1)
    # Strings compare as written, other values as YAML reads them: an unquoted no also excludes false.
    assert Exclusion.parse("inputs.flag=no").matches(False) and not Exclusion.parse("inputs.count=1").matches(True)
    assert Exclusion.parse("inputs.ratio=0.5").matches(0.5) and not Exclusion.parse("server_config.x=no").matches("yes")
    for text in ("novalue", "other.path=1", "inputs=1", "inputs.x=[unclosed"):
        with pytest.raises(GeneratorError):
            Exclusion.parse(text)


def test_a_default_pool_is_drawn_without_being_listed_and_yields_to_listed_pools_and_overlays(tmp_path):
    pools = {"tests.pool.default": {"default": True, "repl-diskless-sync": ["never"], "hz": ["11", "12"], "lfu-log-factor": ["20"]}}  # fmt: skip
    overlay = {"overlay_id": "lfu", "server_config": {"lfu-log-factor": "30"}}
    listed = _runtime(tmp_path / "listed", {"server_config": ["valkey.pool.replication"], "pools": pools}, startup_overlays=[overlay])  # fmt: skip
    unlisted = _runtime(tmp_path / "unlisted", {"pools": pools})
    hz = set()
    for seed in range(20):
        config = _generate(listed, seed).scenario["deployment"]["server_config"]
        # The listed pool draws repl-diskless-sync, and the overlay sets lfu-log-factor: neither is drawn again.
        assert config["repl-diskless-sync"] in ("yes", "no") and "lfu-log-factor" not in config
        hz.add(config["hz"])
        assert _generate(unlisted, seed).scenario["deployment"]["server_config"]["repl-diskless-sync"] == "never"
    assert hz == {"11", "12"}


def test_every_shipped_experiment_draws_every_key_of_the_default_pools_its_overlays_leave():
    service = generator_service()
    defaults = {pool_id: pool for pool_id, pool in installed_pools(service.catalog).items() if pool.get("default")}
    assert set(defaults) == {"valkey.pool.io-threads", "valkey.pool.memory", "valkey.pool.encodings"}
    keys = {key for pool in defaults.values() for key in pool} - {"default", "scope"}
    for experiment in EXPERIMENTS:
        spec = service.catalog.experiments.resolve(experiment)
        overlaid = {key for overlay in spec.startup_overlays for key in overlay.server_config}
        drawn = set()
        for seed in range(5):
            deployment = service.generate(seed, experiment=experiment, excludes=NO_ZSTD).scenario["deployment"]
            for config in (deployment.get("server_config") or {}, *(deployment.get("instance_configs") or {}).values()):
                drawn |= set(config) & keys
        assert drawn == keys - overlaid, experiment


@pytest.mark.parametrize(
    ("generator", "message"),
    [
        (None, r"does not declare metadata\.generator"),
        ({"surprise": 1}, r"unknown fields \['surprise'\]"),
        ({"inputs": {"nope": "fixed"}}, r"does not declare: \['nope'\]"),
        ({"inputs": {"count": {"minimum": 0, "maximum": 11}}}, r"exceeds the input's bounds"),
        ({"inputs": {"codec": {"values": ["none", "huge"]}}}, r"must be one of"),
        ({"inputs": {"count": {"minimum": 3}}}, r"must be fixed, a value, a list"),
        ({"inputs": {"waves": {"hook": "tests.generator.missing"}}}, r"'tests\.generator\.missing' is not registered"),
        ({"deployment": {"mode": ["sharded"]}}, r"not within the experiment's modes"),
        ({"deployment": {"base_port": {"hook": "x.generator.y"}}}, r"a hook draws an input"),
        ({"server_config": ["valkey.pool.nope"]}, r"unknown pools \['valkey.pool.nope'\]"),
        ({"reject": [{"elsewhere.x": 1}]}, r"must be 'experiment' or start with"),
        ({"pools": {"tests.pool": {"scope": ["everywhere"]}}}, r"scope must list"),
        ({"pools": {"tests.pool": {"default": "yes", "hz": ["11"]}}}, r"default must be true or false"),
        ({"pools": {"tests.pool": {"default": True, "scope": ["role"]}}}, r"must map server config keys"),
    ],
)
def test_malformed_blocks_are_rejected(tmp_path, generator, message):
    with pytest.raises(GeneratorError, match=message):
        _generate(_runtime(tmp_path, generator), 1)


@pytest.mark.parametrize("experiment", EXPERIMENTS)
def test_every_shipped_experiment_draws_the_same_valid_bounded_scenario_from_one_seed(experiment):
    service = generator_service()
    first_try = 0
    for seed in range(10):
        generated = service.generate(seed, experiment=experiment, excludes=NO_ZSTD)
        assert service.generate(seed, experiment=experiment, excludes=NO_ZSTD).scenario == generated.scenario
        first_try += generated.attempt == 0
        plan = generated.plan
        configs = [
            plan.deployment.get("server_config") or {},
            *(plan.deployment.get("instance_configs") or {}).values(),
        ]
        if experiment == CLUSTER_EXPERIMENT:
            assert 3 <= plan.deployment["primaries"] <= 16 and 7000 <= plan.deployment["base_port"] <= 8000
            # Its clusters of up to 96 nodes have at most two IO threads a node, none of them always active.
            assert all(config.get("io-threads", "1") in ("1", "2") for config in configs)
            assert all(config.get("io-threads-always-active", "no") == "no" for config in configs)
            assert all(config.get("events-per-io-thread", "2") == "2" for config in configs)
        else:
            # One host runs every instance; cluster chaos draws clusters of up to 16 shards.
            shards = plan.deployment.get("primaries", 1) if plan.deployment_mode == "cluster" else 1
            assert shards * (1 + plan.deployment["replicas_per_primary"]) <= 15
        # A codec the build lacks (zstd without BUILD_ZSTD) is then SKIP_UNSUPPORTED, never a failed start: the
        # probe names every drawn codec setting as one the build may lack.
        drawn = {key for config in configs for key in ("rdbcompression", "repl-compression") if key in config}
        if drawn:
            probes = [item for item in plan.components if item.use == "valkey.capability.build-probe"]
            assert any(drawn <= set(probe.config.get("deployment_config") or ()) for probe in probes)
    assert first_try >= 8
    if experiment == CLUSTER_EXPERIMENT:
        # The live random test runs these seeds: their clusters fit one host.
        for seed in SMALL_CLUSTER_SEEDS:
            deployment = generate_scenario(seed)["deployment"]
            assert deployment["primaries"] * (1 + deployment["replicas_per_primary"]) <= 10, seed


def test_experiment_hooks_draw_inputs_their_oracles_can_judge():
    service = generator_service()
    names = (
        "primary_repl_compression",
        "replica_repl_compression",
        "primary_change_1",
        "replica_change",
        "primary_change_2",
    )
    ratios = set()
    for seed in range(40):
        inputs = service.generate(seed, experiment="valkey.repl-compression-runtime-change").plan.inputs
        assert renegotiates(*(inputs[name] for name in names)) == (True, True)
        # Known-bad (fail-open): a ratio required of incompressible values, or none of compressible ones.
        inputs = service.generate(seed, experiment="valkey.repl-compression-full-sync").plan.inputs
        assert inputs["min_ratio"] == (1.5 if inputs["compressible"] else 0)
        ratios.add(inputs["compressible"])
    assert ratios == {True, False}
    # Known-bad: a replica that never changes what it advertises does not renegotiate.
    assert renegotiates("lz4", "zstd", "zstd", "zstd", "no") == (True, False)


@pytest.mark.parametrize("mode", ["standalone", "cluster"])
def test_generated_waves_use_only_their_modes_operations(mode):
    from valkey_fuzzer.components.generators import waves
    from valkey_fuzzer.components.wave_operations import operations_for

    deployment = {"mode": mode, "primaries": 3, "replicas_per_primary": 2, "addressing": "per_instance_loopback"}
    drawn = {
        op["op"]
        for seed in range(300)
        for wave in waves(random.Random(seed), {"deployment": deployment})
        for op in wave
    }
    assert drawn <= set(operations_for(mode))


def test_a_base_mapping_replaces_a_drawn_reference_instead_of_merging_into_it():
    # A $ref is one value: fields merged into it would make a $ref with siblings, which the compiler rejects.
    drawn = {"deployment": {"server_config": {"$ref": "inputs.config"}, "replicas_per_primary": 1}}
    base = {"deployment": {"server_config": {"maxmemory": "1mb"}}}
    assert merge(drawn, base) == {"deployment": {"server_config": {"maxmemory": "1mb"}, "replicas_per_primary": 1}}


def test_a_pinned_draw_is_kept_and_the_hooks_drawn_after_a_redrawn_dimension_are_drawn_again(tmp_path):
    # waves derives from count, and the scenario hook from every input: both must follow a changed count.
    hooks = {
        "waves": lambda rng, scenario: [scenario["experiment"]["inputs"]["count"]] * rng.randint(1, 3),
        "label": lambda rng, scenario: {"experiment": {"inputs": {"label": f"{rng.random()}"}}},
    }
    block = {"hook": "tests.generator.label", "inputs": {"waves": {"hook": "tests.generator.waves"}}}
    runtime = _runtime(tmp_path, block, hooks=hooks)
    parsed = GeneratorBlock.parse(runtime.catalog.experiments.resolve("tests.random"), installed_pools(runtime.catalog))
    parent = _generate(runtime, 1)
    assert next(iter(parent.draws)) == "deployment.mode" and list(parent.draws)[-2:] == ["inputs.waves", "hook"]
    assert parent.provenance["draws"] == parent.draws

    # Every draw kept: only the seed differs.
    same = _generate(runtime, 2, pinned=parsed.kept(parent.draws, None))
    assert same.draws == parent.draws and same.scenario["seed"] == 2

    # count drawn again: every other draw kept, the hooks after it drawn again and consistent with it.
    pinned = parsed.kept(parent.draws, "inputs.count")
    assert set(pinned) == set(parent.draws) - {"inputs.count", "inputs.waves", "hook"}
    for seed in range(2, 12):
        child = _generate(runtime, seed, pinned=pinned)
        assert all(child.draws[name] == value for name, value in pinned.items())
        assert set(child.scenario["experiment"]["inputs"]["waves"]) == {child.draws["inputs.count"]}
        assert child.draws["hook"] != parent.draws["hook"]

    # A hooked input drawn again keeps every draw before it, and the scenario hook after it is drawn again.
    assert set(parsed.kept(parent.draws, "inputs.waves")) == set(parent.draws) - {"inputs.waves", "hook"}
