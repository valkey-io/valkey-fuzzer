"""The live bug zoo through the batch runner: each case's clean control passes and its broken build fails at its rate.

Skipped unless ``VALKEY_ZOO_BUILDS`` names a directory ``zoo/build.sh`` built
into. Takes minutes per case; ``pytest -m live -k zoo``.
"""

import json
import os
from pathlib import Path

import pytest

from tests.live_support import free_port_block
from tests.valkey_experiments_support import valkey_service
from valkey_fuzzer.batch import PORT_BLOCK, BatchOptions, BatchRunner, expand, normalize_source
from valkey_fuzzer.zoo import load_zoo

pytestmark = [pytest.mark.live, pytest.mark.slow]

BUILDS = os.environ.get("VALKEY_ZOO_BUILDS")
if not BUILDS:
    pytest.skip("set VALKEY_ZOO_BUILDS to the directory zoo/build.sh built into", allow_module_level=True)

COMMIT, CASES = load_zoo()
ROOT = Path(__file__).resolve().parent.parent


@pytest.mark.parametrize("case", CASES, ids=lambda case: case.case_id)
def test_zoo_case_holds(tmp_path, case):
    sources = [normalize_source({"zoo": {"builds": BUILDS, "cases": [case.case_id]}}, os.getcwd())]
    options = BatchOptions(
        batch_id=f"zoo-{case.case_id}", workers=2, timeout_s=900, port_base=free_port_block(2 * PORT_BLOCK)
    )
    expansion = expand(sources, valkey_service(), options)
    if not expansion.jobs:
        pytest.skip(expansion.zoo["skipped"][0]["reason"])
    summary = BatchRunner.create(str(tmp_path / "batch"), options, sources, expansion).run()
    (report,) = summary["zoo"]["cases"]
    assert report["holds"] and not summary["leaks"], json.dumps(report, indent=2)
