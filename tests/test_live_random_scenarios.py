"""Generated scenarios of every opted-in experiment, run against real valkey-server processes: none may ERROR.

Seeds 1..``VALKEY_FUZZER_RANDOM_SEEDS`` (default 3) per experiment; cluster
chaos uses seeds whose clusters fit one host. Two-build experiments get
``VALKEY_FUZZER_OLD_BINARY`` through ``--base``, and are skipped without it (a
run without a second build is ERROR); without root, the partition and
disk-full experiments' runs are SKIP_UNSUPPORTED.
"""

import json
import os
from unittest.mock import patch

import pytest
import yaml

from tests.generator_support import SMALL_CLUSTER_SEEDS
from tests.live_support import BINARY, OLD_BINARY, free_port_block
from tests.valkey_experiments_support import valkey_service
from valkey_fuzzer.cli import main as cli_main
from valkey_fuzzer.generator import opted_in

pytestmark = [pytest.mark.live, pytest.mark.slow]

SEEDS = int(os.environ.get("VALKEY_FUZZER_RANDOM_SEEDS", "3"))

if not BINARY:
    pytest.skip("valkey-server is not available", allow_module_level=True)


def _base(experiment):
    """The partial scenario that pins the port and, for two-build experiments, names the older build."""

    base = {"deployment": {"base_port": free_port_block(100)}}
    if not OLD_BINARY:
        return base
    old = {"old": {"use": "valkey.artifact.local-binary", "with": {"path": OLD_BINARY}}}
    if experiment in ("valkey.mixed-version-cluster", "valkey.repl-compression-mixed-version"):
        base["deployment"]["instance_binaries"] = {"replica": OLD_BINARY}
    inputs = {
        "valkey.repl-compression-mixed-version": {"old_binary": {"$ref": "artifacts.old.binary"}},
        # An older release refuses an RDB from a newer one in every codec.
        "valkey.rdb-compression-compat": {"load_binary": {"$ref": "artifacts.old.binary"}, "load_expect": "refused"},
        "valkey.rolling-upgrade": {"to_binary": {"$ref": "artifacts.new.binary"}},
    }.get(experiment)
    if inputs:
        base.update(
            artifacts={**old, "new": {"use": "valkey.artifact.local-binary", "with": {"path": BINARY}}},
            experiment={"inputs": inputs},
        )
    if experiment == "valkey.rolling-upgrade":
        base["deployment"]["valkey_binary"] = {"$ref": "artifacts.old.binary"}
    return base


# A run of these without a second build is set up wrong, so it is ERROR.
TWO_BUILDS = ("valkey.mixed-version-cluster", "valkey.repl-compression-mixed-version", "valkey.rolling-upgrade")


@pytest.mark.parametrize("experiment", opted_in(valkey_service().catalog))
def test_generated_scenarios_of_every_experiment_run_without_error(tmp_path, experiment):
    if experiment in TWO_BUILDS and not OLD_BINARY:
        pytest.skip("needs a second build: set VALKEY_FUZZER_OLD_BINARY")
    (tmp_path / "base.yaml").write_text(yaml.safe_dump(_base(experiment)))
    # A rolling upgrade starts on the older build named by the base.
    binary = [] if OLD_BINARY and experiment == "valkey.rolling-upgrade" else ["--valkey-binary", BINARY]
    reports = []
    for seed in SMALL_CLUSTER_SEEDS if experiment == "valkey.cluster-chaos" else range(1, SEEDS + 1):
        output = tmp_path / f"seed{seed}.json"
        with patch("sys.argv", ["valkey-fuzzer", "run", "--seed", str(seed), "--experiment", experiment, *binary,
                   "--base", str(tmp_path / "base.yaml"), "--artifact-dir", str(tmp_path / "artifacts"), "--timeout", "600", "--output", str(output)]):  # fmt: skip
            cli_main()
        reports.append(json.loads(output.read_text()))
    summary = "\n".join(
        f"seed {item['reproducer']['generated']['seed']}: {item['status']} {item['summary']}" for item in reports
    )
    assert all(report["status"] != "ERROR" for report in reports), summary
