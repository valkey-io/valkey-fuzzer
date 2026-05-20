"""Tests for detect_valkey_sha — parses git SHA from `valkey-server --version`."""

import subprocess
from unittest.mock import patch

from src.cluster_orchestrator.orchestrator import detect_valkey_sha


def _ok(stdout: str) -> subprocess.CompletedProcess:
    return subprocess.CompletedProcess(args=["valkey-server", "--version"],
                                       returncode=0, stdout=stdout, stderr="")


def test_parses_full_sha_from_version_output():
    out = "Valkey server v=8.1.0 sha=abc1234567890def:0 malloc=jemalloc-5.3.0 bits=64 build=abcdef1234567890"
    with patch("subprocess.run", return_value=_ok(out)):
        assert detect_valkey_sha("/usr/bin/valkey-server") == "abc1234567890def"


def test_parses_short_sha():
    out = "Valkey server v=8.1.0 sha=abc1234:0 malloc=jemalloc-5.3.0 bits=64 build=abc"
    with patch("subprocess.run", return_value=_ok(out)):
        assert detect_valkey_sha("/usr/bin/valkey-server") == "abc1234"


def test_handles_dirty_marker():
    """`sha=<hex>:1` indicates a dirty tree — we still record the underlying commit."""
    out = "Valkey server v=8.1.0 sha=deadbeefcafe:1 malloc=jemalloc-5.3.0 bits=64 build=x"
    with patch("subprocess.run", return_value=_ok(out)):
        assert detect_valkey_sha("/usr/bin/valkey-server") == "deadbeefcafe"


def test_returns_none_for_release_build_with_no_git_context():
    """Release tarballs emit `sha=00000000:0` — record None so callers don't
    try to clone an all-zero commit."""
    out = "Valkey server v=8.1.0 sha=00000000:0 malloc=libc bits=64 build=x"
    with patch("subprocess.run", return_value=_ok(out)):
        assert detect_valkey_sha("/usr/bin/valkey-server") is None


def test_returns_none_when_version_string_missing_sha_field():
    out = "Valkey server v=8.1.0 malloc=libc bits=64 build=x"
    with patch("subprocess.run", return_value=_ok(out)):
        assert detect_valkey_sha("/usr/bin/valkey-server") is None


def test_returns_none_when_binary_invocation_fails():
    """Permission/path errors should not bubble — return None and let the
    fuzzer continue without an embedded SHA."""
    with patch("subprocess.run", side_effect=FileNotFoundError("nope")):
        assert detect_valkey_sha("/missing/binary") is None


def test_returns_none_on_subprocess_timeout():
    with patch("subprocess.run",
               side_effect=subprocess.TimeoutExpired(cmd="x", timeout=5)):
        assert detect_valkey_sha("/usr/bin/valkey-server") is None


def test_case_insensitive_sha_lowercased():
    out = "Valkey server v=8.1.0 sha=ABCDEF1234:0 malloc=libc bits=64"
    with patch("subprocess.run", return_value=_ok(out)):
        assert detect_valkey_sha("/usr/bin/valkey-server") == "abcdef1234"
