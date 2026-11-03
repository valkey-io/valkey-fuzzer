"""detect_valkey_sha: the commit a build reports in ``valkey-server --version``, or None."""

import subprocess
from unittest.mock import patch

import pytest

from valkey_fuzzer.deployment.local.processes import detect_valkey_sha


@pytest.mark.parametrize(
    ("stdout", "sha"),
    [("v=8.1.0 sha=ABC1234:1 malloc=libc", "abc1234"), ("v=8.1.0 sha=00000000:0", None), ("v=8.1.0 malloc=libc", None)],
)  # fmt: skip
def test_the_sha_comes_from_the_version_output_and_a_release_build_has_none(stdout, sha):
    completed = subprocess.CompletedProcess(args=[], returncode=0, stdout=f"Valkey server {stdout}", stderr="")
    with patch("subprocess.run", return_value=completed):
        assert detect_valkey_sha("valkey-server") == sha
    for error in (FileNotFoundError("nope"), subprocess.TimeoutExpired(cmd="x", timeout=5)):
        with patch("subprocess.run", side_effect=error):
            assert detect_valkey_sha("valkey-server") is None
