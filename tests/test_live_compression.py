"""Live runs of the RDB and replication compression experiments against real valkey-server builds.

zstd rows need a ``BUILD_ZSTD=yes`` build: the build under test when it has
zstd, else ``VALKEY_FUZZER_ZSTD_BINARY``. Cross-version rows need
``VALKEY_FUZZER_OLD_BINARY``. Every known-bad row must FAIL (or SKIP) for the reason it names.
"""

import os
import time

import pytest
import yaml

from tests import live_support as live
from tests.live_support import BINARY, OLD_BINARY, check, evidence, run_scenario, scenario
from tests.valkey_experiments_support import valkey_service

pytestmark = [pytest.mark.live, pytest.mark.slow]

if not BINARY:
    pytest.skip("valkey-server is not available", allow_module_level=True)

COMPRESSION = live.build_accepts(BINARY, {"rdbcompression": "lz4", "repl-compression": "lz4"})
ZSTD = next(
    (
        binary
        for binary in (BINARY, os.environ.get("VALKEY_FUZZER_ZSTD_BINARY"))
        if live.build_accepts(binary, {"rdbcompression": "zstd", "repl-compression": "zstd"})
    ),
    None,
)
lz4 = pytest.mark.skipif(not COMPRESSION, reason="valkey-server build lacks streaming compression")
zstd = pytest.mark.skipif(not ZSTD, reason="needs a BUILD_ZSTD=yes build (VALKEY_FUZZER_ZSTD_BINARY)")
without_zstd = pytest.mark.skipif(BINARY == ZSTD, reason="VALKEY_FUZZER_BINARY supports zstd")
RDB, FULL, COMPAT = "valkey.rdb-compression", "valkey.repl-compression-full-sync", "valkey.rdb-compression-compat"


@pytest.fixture
def runtime(tmp_path):
    return valkey_service(artifact_dir=str(tmp_path / "artifacts"))


def _codec(codec):
    def then(outcome):
        saves = [item.value for item in outcome.all_evidence if item.evidence_id == "valkey.bgsave.result"]
        assert saves and all(save["rdb_format"]["codec"] == codec for save in saves)

    return then


def _links(**expected):
    def then(outcome):
        links = evidence(outcome, "valkey.repl.compression-verified")["links"]
        assert links and all(all(_get(link, key) == value for key, value in expected.items()) for link in links)

    return then


def _get(link, path):
    for key in path.split("."):
        link = link[key]
    return link


def _compat(save, load, **inputs):
    built = scenario(
        COMPAT, inputs={"load_binary": {"$ref": "artifacts.loader.binary"}, **inputs}, binary=save, seed=11
    )
    built["artifacts"] = {"loader": {"use": "valkey.artifact.local-binary", "with": {"path": load}}}
    return built


def _row(experiment, mode, inputs, then=None, marks=(lz4,), replicas=2, binary=None, id=None):
    def build():
        standalone_replicas = replicas if mode == "standalone" else None
        return scenario(experiment, mode, inputs, replicas=standalone_replicas, seed=11, binary=binary and ZSTD)

    return pytest.param(build, then, marks=marks, id=id)


@pytest.mark.parametrize(
    ("build", "then"),
    [
        _row(RDB, "standalone", {"rdbcompression": "lz4", "appendonly": "yes"}, _codec("lz4"), id="rdb-lz4-aof"),
        _row(RDB, "standalone", {"rdbcompression": "zstd", "appendonly": "yes"}, marks=(zstd,), replicas=None, binary=True, id="rdb-zstd"),
        _row("valkey.aof-durability", "standalone", {"rdbcompression": "lz4"}, replicas=None, id="aof-lz4-base"),
        _row(FULL, "standalone", {"rdbcompression": "lz4", "repl_diskless_sync": "no"}, _links(**{"full_sync.expected.target": "disk"}), id="full-sync-disk"),
        _row(FULL, "cluster", {"dual_channel": "yes"}, _links(**{"full_sync.expected.target": "replicas sockets"}), id="full-sync-dual-channel-cluster"),
        _row("valkey.repl-compression-partial-resync", "standalone", {}, _links(algo="lz4"), id="partial-resync"),
        _row("valkey.repl-compression-runtime-change", "standalone", {"primary_repl_compression": "no", "replica_repl_compression": "lz4",
             "primary_change_1": "lz4", "replica_change": "no", "primary_change_2": "no"}, id="runtime-change-lz4"),
        pytest.param(lambda: _compat(BINARY, OLD_BINARY, rdbcompression="lz4", refused_log="Wrong signature"), None,
                     marks=(lz4, live.needs_old_binary), id="older-release-refuses-a-compressed-rdb"),
        pytest.param(lambda: _compat(ZSTD, BINARY, rdbcompression="zstd", refused_log="Invalid or unsupported RDB stream envelope"), None,
                     marks=(zstd, without_zstd), id="build-without-zstd-refuses-a-zstd-rdb"),
    ],
)  # fmt: skip
def test_a_compression_experiment_passes_in_the_negotiated_codec(runtime, build, then):
    outcome = run_scenario(runtime, build())
    check(outcome)
    if then:
        then(outcome)


def _mixed(old):
    built = live.builds(
        scenario(
            "valkey.repl-compression-mixed-version", "cluster", {"old_binary": {"$ref": "artifacts.baseline.binary"}}
        ),
        old,
        BINARY,
    )
    built["deployment"].update(
        valkey_binary={"$ref": "artifacts.candidate.binary"},
        instance_binaries={"replica": {"$ref": "artifacts.baseline.binary"}},
    )
    return built


@lz4
@live.needs_old_binary
def test_mixed_versions_keep_links_plaintext_and_force_diskless_syncs(runtime):
    outcome = run_scenario(runtime, _mixed(OLD_BINARY))
    check(outcome)
    verified = evidence(outcome, "valkey.repl.compression-verified")
    assert verified["links"] and all(link["algo"] == "none" for link in verified["links"])
    # Both directions ran: the failed-over shard replicates from the old build to a newer replica, the others the other way.
    old_side = [verified["instances"][link["primary"]]["repl-compression"] is None for link in verified["links"]]
    assert any(old_side) and not all(old_side)


def _expect_lz4(document):
    document["extends"] = [*document["extends"], "valkey.liveness"]
    next(item for item in document["components"] if item["id"] == "compression")["config"]["expect_algo"] = "lz4"


@lz4
def test_known_bad_cases_get_the_verdict_their_reason_calls_for(tmp_path, runtime):
    # Forkless BGSAVE ignores whole-stream rdbcompression on unstable: src/forkless.c never wraps its rio in
    # the streaming compressor, so a save asked to be lz4 is a plain RDB. When that lands, update this test.
    for experiment, inputs in (
        (RDB, {"bgsave_method": "forkless"}),
        ("valkey.forkless-save", {}),
        ("valkey.forkless-save-cancel", {}),
    ):
        outcome = run_scenario(runtime, scenario(experiment, inputs={"rdbcompression": "lz4", **inputs}))
        check(outcome, "FAIL", says=["snapshot: FAIL", "expected codec lz4, the RDB is plain"])
    # The same build loads its own lz4 RDB, so expecting a refusal must FAIL.
    check(
        run_scenario(runtime, _compat(BINARY, BINARY, rdbcompression="lz4")),
        "FAIL",
        says=["expected the load to be refused, but it loaded"],
    )
    # repl-compression no everywhere while the oracle expects lz4: the scenario contradicts itself, which says
    # nothing about Valkey.
    variant = live.variant_runtime(tmp_path, f"{FULL}.yaml", "expect-lz4", _expect_lz4)
    inputs = {"primary_repl_compression": "no", "replica_repl_compression": "no"}
    check(
        run_scenario(variant, scenario(f"{FULL}-expect-lz4", inputs=inputs, replicas=2)),
        "ERROR",
        says=["the experiment expects lz4"],
    )
    # An "old" build that supports compression is no old build: the run was set up wrong.
    check(run_scenario(runtime, _mixed(BINARY)), "ERROR")
    if BINARY != ZSTD:
        for build in (
            scenario(RDB, inputs={"rdbcompression": "zstd"}),
            scenario("valkey.repl-compression-runtime-change", replicas=2),
        ):
            check(run_scenario(runtime, build), "SKIP_UNSUPPORTED")


@lz4
def test_the_example_rdb_codec_differential_passes(runtime):
    document = yaml.safe_load((live.EXAMPLES / "differential_rdb_codecs.yaml").read_text())
    document["base"]["deployment"].update(base_port=live.free_base_port(), valkey_binary=ZSTD or BINARY)
    if not ZSTD:
        document["variants"].pop("zstd")
    started = time.monotonic()
    assert live.differential(runtime, document)["status"] == "PASS"
    assert time.monotonic() - started < 900
