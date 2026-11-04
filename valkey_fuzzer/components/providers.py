"""Artifact and resource providers for Valkey experiments.

``valkey.artifact.local-binary`` resolves a valkey-server binary on this host,
fingerprints it, and optionally verifies its version or build SHA; its
``binary`` output is referenced from deployments and components with
``{$ref: artifacts.<id>.binary}``. ``valkey.artifact.local-module`` is a
module build (a ``.so``) on this host, fingerprinted the same way; its ``path``
output goes to ``loadmodule`` (``{$ref: artifacts.<id>.path}``).

``valkey.resource.tmpdir`` and ``valkey.resource.tmpfs`` provision a private
directory or a size-limited tmpfs mount whose ``path`` output can hold instance
data (``deployment.instance_data_roots``). Only a ``valkey.resource.tmpfs``
mount may be filled for disk-full testing: :func:`provisioned_tmpfs_bytes`
names the mounts this process provisioned and :func:`mounted_tmpfs_bytes`
checks one is still there. A mount stays owned until its unmount is
confirmed: a busy unmount is retried, then the cleanup fails with umount's
error and the mount stays registered, so another cleanup or rollback can
still release it. ``mount`` and ``umount`` calls are bounded by
:data:`MOUNT_TIMEOUT_S`.

Both are tagged with this process as their owner, so ``valkey-fuzzer sweep``
removes them after a crash: a directory holds an owner tag
(``tag_directory``; one kept with ``keep: true`` loses it at cleanup), and a
tmpfs is mounted under ``<tmp>/valkey-fuzzer-mounts/<owner>/``
(``mount_parent``).
"""

import hashlib
import os
import platform
import re
import shutil
import tempfile
import threading
import time
from collections.abc import Mapping
from typing import Any, Optional

from valkey_fuzzer._data import file_sha256
from valkey_fuzzer.deployment.local.processes import resolve_binary_path
from valkey_fuzzer.environment import EnvironmentProviderRegistration, EnvironmentResult, SupportDecision
from valkey_fuzzer.hoststate import mount_parent, release_mount_parent, run_bounded, tag_directory, untag_directory
from valkey_fuzzer.references import EnvironmentKind
from valkey_fuzzer.schema import ConfigError, ConfigField, ConfigSchema

from .server import parse_version_output, version_output

LOCAL_BINARY_PROVIDER = "valkey.artifact.local-binary"
LOCAL_MODULE_PROVIDER = "valkey.artifact.local-module"
TMPDIR_PROVIDER = "valkey.resource.tmpdir"
TMPFS_PROVIDER = "valkey.resource.tmpfs"

# ---------------------------------------------------------------------------
# valkey.artifact.local-binary
# ---------------------------------------------------------------------------

_BINARY_CONFIG = ConfigSchema(
    {
        "path": ConfigField(str, reference=True),
        "expected_version": ConfigField(str, allow_none=True, default=None),
        "expected_sha": ConfigField(str, allow_none=True, default=None),
    }
)


def resolve_binary(path: str) -> str:
    """Return an absolute executable path for a binary path or command name, as the deployment finds it."""

    return resolve_binary_path(path, "valkey-server binary")


def describe_binary(path: str) -> dict[str, Any]:
    """Immutable provenance for one binary: digest, version, and git SHA."""

    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    output = version_output(path)
    return {"sha256": digest.hexdigest(), **parse_version_output(output), "version_output": output.strip()}


def _check_strings(spec: Any, config: Mapping[str, Any]) -> None:
    # ``path`` may still be a reference at compile time; it is checked once resolved.
    for name, value in config.items():
        if isinstance(value, str) and (not value or "\0" in value):
            raise ConfigError(f"{spec.dependency.path}.with.{name} must be a non-empty string without NUL bytes")


class LocalBinaryProvider:
    def check_support(self, context: Any, spec: Any, resolved_config: Mapping[str, Any]) -> SupportDecision:
        return SupportDecision.allow()

    def acquire(self, context: Any, spec: Any, resolved_config: Mapping[str, Any]) -> EnvironmentResult:
        path = resolve_binary(resolved_config["path"])
        record = describe_binary(path)
        expected_version = resolved_config["expected_version"]
        if expected_version and record["version"] != expected_version:
            raise ValueError(
                f"artifact {spec.id!r} ({path}) reports version {record['version']!r}, expected {expected_version!r}"
            )
        expected_sha = (resolved_config["expected_sha"] or "").lower()
        if expected_sha:
            candidates = [value for value in (record["git_sha"], record["sha256"]) if value]
            if not any(value.startswith(expected_sha) or expected_sha.startswith(value) for value in candidates):
                raise ValueError(
                    f"artifact {spec.id!r} ({path}) does not match expected_sha {expected_sha!r}: "
                    f"git sha {record['git_sha']!r}, sha256 {record['sha256'][:16]}..."
                )
        return EnvironmentResult(
            outputs={"binary": path},
            provenance={key: value for key, value in record.items() if key != "version_output"},
        )

    def rollback(self, context: Any, spec: Any) -> None:
        return None

    def cleanup(self, context: Any, spec: Any, result: EnvironmentResult) -> None:
        return None


# ---------------------------------------------------------------------------
# valkey.artifact.local-module
# ---------------------------------------------------------------------------

# A bare path: Valkey reads module arguments from the tokens after the path on one loadmodule directive, and a
# server config value is one argument (a list repeats the directive), so arguments could not reach the module.
_MODULE_CONFIG = ConfigSchema({"path": ConfigField(str, reference=True)})


class LocalModuleProvider:
    """A module build on this host: its absolute ``path``, and the SHA-256 of the file as provenance, so a replay
    of another build of the module is drift."""

    def check_support(self, context: Any, spec: Any, resolved_config: Mapping[str, Any]) -> SupportDecision:
        return SupportDecision.allow()

    def acquire(self, context: Any, spec: Any, resolved_config: Mapping[str, Any]) -> EnvironmentResult:
        path = os.path.abspath(resolved_config["path"])
        digest = file_sha256(path) if os.path.isfile(path) else None
        if digest is None:
            raise FileNotFoundError(f"artifact {spec.id!r}: module {path} is not a readable file")
        return EnvironmentResult(outputs={"path": path}, provenance={"sha256": digest})

    def rollback(self, context: Any, spec: Any) -> None:
        return None

    def cleanup(self, context: Any, spec: Any, result: EnvironmentResult) -> None:
        return None


# ---------------------------------------------------------------------------
# valkey.resource.tmpdir / valkey.resource.tmpfs
# ---------------------------------------------------------------------------

_TMPDIR_CONFIG = ConfigSchema(
    {
        "prefix": ConfigField(str, default="valkey-fuzzer-"),
        "parent": ConfigField(str, allow_none=True, default=None),
        "keep": ConfigField(bool, default=False),
    }
)
_TMPFS_CONFIG = ConfigSchema(
    {
        "size_mb": ConfigField(int, default=64, minimum=1, maximum=65536),
        "prefix": ConfigField(str, default="valkey-fuzzer-tmpfs-"),
    }
)


def _check_prefix(spec: Any, config: Mapping[str, Any]) -> None:
    if "/" in config["prefix"]:
        raise ConfigError(f"{spec.dependency.path}.with.prefix must be a plain file-name prefix")


def on_linux() -> bool:
    """Whether this host runs Linux, which iptables and size-limited tmpfs mounts need."""

    return platform.system() == "Linux"


def as_root() -> bool:
    """Whether this process runs as root, which iptables and mounts need and permission faults must not have."""

    return os.geteuid() == 0


def tmpfs_supported() -> tuple[bool, str]:
    if not on_linux():
        return False, "size-limited tmpfs mounts require Linux"
    if not as_root():
        return False, "mounting tmpfs requires root"
    if shutil.which("mount") is None or shutil.which("umount") is None:
        return False, "mount/umount are not installed"
    return True, ""


# The valkey.resource.tmpfs mounts this process holds, as real path -> size in bytes.
_TMPFS_MOUNTS: dict[str, int] = {}
_TMPFS_MOUNTS_LOCK = threading.Lock()
_PROC_MOUNTS = "/proc/mounts"
MOUNT_TIMEOUT_S = 60.0
# Pauses before each retry of a busy unmount (an instance may still be closing its files).
_UMOUNT_RETRY_DELAYS_S = (0.5, 2.0)


def provisioned_tmpfs_bytes(path: str) -> Optional[int]:
    """The size of the ``valkey.resource.tmpfs`` mount at ``path``, or None if this process holds none there."""

    with _TMPFS_MOUNTS_LOCK:
        return _TMPFS_MOUNTS.get(os.path.realpath(path))


def _mount_point(field: str) -> str:
    """A /proc/mounts path field with its octal escapes (``\\040`` for a space) decoded."""

    return re.sub(r"\\([0-7]{3})", lambda match: chr(int(match.group(1), 8)), field)


def mounted_tmpfs_bytes(path: str) -> Optional[int]:
    """The size of the tmpfs mounted at ``path`` now, or None if ``path`` is not a tmpfs mount point."""

    target = os.path.realpath(path)
    try:
        with open(_PROC_MOUNTS, encoding="utf-8") as handle:
            entries = [line.split() for line in handle]
    except OSError:
        return None
    # The last mount on a path is the visible one.
    types = [fields[2] for fields in entries if len(fields) > 2 and _mount_point(fields[1]) == target]
    if not types or types[-1] != "tmpfs":
        return None
    stats = os.statvfs(target)
    return stats.f_blocks * stats.f_frsize


class TempDirectoryProvider:
    """A private directory removed after the run."""

    def __init__(self) -> None:
        self._paths: dict[str, str] = {}

    def check_support(self, context: Any, spec: Any, resolved_config: Mapping[str, Any]) -> SupportDecision:
        return SupportDecision.allow()

    def acquire(self, context: Any, spec: Any, resolved_config: Mapping[str, Any]) -> EnvironmentResult:
        path = tempfile.mkdtemp(prefix=resolved_config["prefix"], dir=resolved_config["parent"])
        self._paths[spec.id] = path
        tag_directory(path, "tmpdir", spec.id)
        return EnvironmentResult(
            outputs={"path": path},
            provenance={"kind": "directory", "keep": resolved_config["keep"]},
        )

    def rollback(self, context: Any, spec: Any) -> None:
        path = self._paths.pop(spec.id, None)
        if path:
            shutil.rmtree(path, ignore_errors=True)

    def cleanup(self, context: Any, spec: Any, result: EnvironmentResult) -> None:
        path = self._paths.pop(spec.id, result.outputs["path"])
        if result.provenance.get("keep"):
            untag_directory(path)
        else:
            shutil.rmtree(path)


class TmpfsProvider:
    """A size-limited tmpfs mount for disk-full testing (Linux, root)."""

    def __init__(self) -> None:
        self._paths: dict[str, str] = {}

    def check_support(self, context: Any, spec: Any, resolved_config: Mapping[str, Any]) -> SupportDecision:
        supported, reason = tmpfs_supported()
        return SupportDecision.allow() if supported else SupportDecision.deny(reason)

    def acquire(self, context: Any, spec: Any, resolved_config: Mapping[str, Any]) -> EnvironmentResult:
        path = tempfile.mkdtemp(prefix=resolved_config["prefix"], dir=mount_parent())
        self._paths[spec.id] = path
        size = resolved_config["size_mb"] * 1024 * 1024
        mounted = run_bounded(
            ["mount", "-t", "tmpfs", "-o", f"size={size},mode=0755", "tmpfs", path], timeout=MOUNT_TIMEOUT_S
        )
        if mounted.returncode != 0:
            # Rollback unmounts it if the mount happened anyway, and removes the directory.
            raise RuntimeError(f"cannot mount a tmpfs at {path}: {mounted.stderr.strip()}")
        with _TMPFS_MOUNTS_LOCK:
            _TMPFS_MOUNTS[os.path.realpath(path)] = size
        return EnvironmentResult(
            outputs={"path": path},
            provenance={"kind": "tmpfs", "size_mb": resolved_config["size_mb"]},
        )

    @staticmethod
    def _release(path: str) -> None:
        """Unmount ``path`` and remove it; ownership is dropped only once the unmount is confirmed."""

        errors = []
        for delay in (0, *_UMOUNT_RETRY_DELAYS_S):
            if not os.path.ismount(path):
                break
            time.sleep(delay)
            result = run_bounded(["umount", path], timeout=MOUNT_TIMEOUT_S)
            if result.returncode != 0:
                errors.append(result.stderr.strip())
        if os.path.ismount(path):
            raise RuntimeError(f"cannot unmount the tmpfs at {path} ({len(errors)} attempt(s)): {errors[-1:]}")
        with _TMPFS_MOUNTS_LOCK:
            _TMPFS_MOUNTS.pop(os.path.realpath(path), None)
        os.rmdir(path)
        release_mount_parent(path)

    def rollback(self, context: Any, spec: Any) -> None:
        path = self._paths.get(spec.id)
        if path and os.path.exists(path):
            self._release(path)
        self._paths.pop(spec.id, None)

    def cleanup(self, context: Any, spec: Any, result: EnvironmentResult) -> None:
        self._release(self._paths.get(spec.id, result.outputs["path"]))
        self._paths.pop(spec.id, None)


ENVIRONMENT_PROVIDERS = (
    EnvironmentProviderRegistration(
        kind=EnvironmentKind.ARTIFACT,
        provider_id=LOCAL_BINARY_PROVIDER,
        factory=LocalBinaryProvider,
        outputs=("binary",),
        config=_BINARY_CONFIG,
        check=_check_strings,
    ),
    EnvironmentProviderRegistration(
        kind=EnvironmentKind.ARTIFACT,
        provider_id=LOCAL_MODULE_PROVIDER,
        factory=LocalModuleProvider,
        outputs=("path",),
        config=_MODULE_CONFIG,
        check=_check_strings,
    ),
    EnvironmentProviderRegistration(
        kind=EnvironmentKind.RESOURCE,
        provider_id=TMPDIR_PROVIDER,
        factory=TempDirectoryProvider,
        outputs=("path",),
        config=_TMPDIR_CONFIG,
        check=_check_prefix,
    ),
    EnvironmentProviderRegistration(
        kind=EnvironmentKind.RESOURCE,
        provider_id=TMPFS_PROVIDER,
        factory=TmpfsProvider,
        outputs=("path",),
        config=_TMPFS_CONFIG,
        check=_check_prefix,
    ),
)
