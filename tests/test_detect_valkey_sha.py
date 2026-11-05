"""detect_valkey_sha: the commit a build reports in ``valkey-server --version``, or None."""

import os
import subprocess
from unittest.mock import patch

import pytest

from valkey_fuzzer.components import server
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


def test_a_sanitizer_builds_version_is_read_without_its_leak_report(tmp_path, monkeypatch):
    # gcc's ASan on aarch64 reports the options sds main leaves behind when --version exits, and exits 1: every
    # artifact and probe of that build failed. Its version is not a leak test.
    binary = tmp_path / "valkey-server"
    binary.write_text(
        "#!/bin/sh\n"
        'case "$ASAN_OPTIONS" in *detect_leaks=0*) ;; *) echo "ERROR: LeakSanitizer" >&2; exit 1;; esac\n'
        'echo "Valkey server v=9.9.9 sha=0123abcd:0 malloc=libc bits=64 build=1"\n'
    )
    binary.chmod(0o755)
    monkeypatch.setenv("ASAN_OPTIONS", "allocator_may_return_null=1")
    assert server.parse_version_output(server.version_output(str(binary)))["version"] == "9.9.9"
    assert detect_valkey_sha(str(binary)) == "0123abcd"
    assert os.environ["ASAN_OPTIONS"] == "allocator_may_return_null=1"
