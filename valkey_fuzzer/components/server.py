"""Shared helpers for talking to valkey-server binaries and running instances.

``PrivateServer`` starts a throwaway server on a unix socket (no TCP port) for
capability probes and RDB reloads; ``binary_version`` reads what a binary
reports about itself; ``server_identity`` and ``instance_role`` read what a
running instance reports.
"""

import os
import re
import shutil
import subprocess
import tempfile
import time
from collections.abc import Mapping, Sequence
from typing import Any, Optional

import valkey

from valkey_fuzzer.deployment.local.processes import kill_server
from valkey_fuzzer.deployment.local.queries import parse_replication, replication_role
from valkey_fuzzer.hoststate import server_tag_arguments, tag_directory

from .instances import close_quietly, replication_info

_VERSION_RE = re.compile(r"\bv=(\S+)")
_SHA_RE = re.compile(r"sha=([0-9a-f]{7,40}):", re.IGNORECASE)


def parse_version_output(output: str) -> dict[str, Optional[str]]:
    """``{version, git_sha}`` from ``valkey-server --version`` output."""

    version = _VERSION_RE.search(output)
    sha = _SHA_RE.search(output)
    return {"version": version.group(1) if version else None, "git_sha": sha.group(1).lower() if sha else None}


def version_output(binary: str) -> str:
    """The raw ``--version`` output of ``binary``; raises ``RuntimeError`` if it cannot run."""

    try:
        return subprocess.run([binary, "--version"], capture_output=True, text=True, timeout=10, check=True).stdout
    except (OSError, subprocess.SubprocessError) as exc:
        raise RuntimeError(f"cannot run {binary} --version: {exc}") from exc


def binary_version(binary: str) -> dict[str, Optional[str]]:
    """``{version, git_sha}`` reported by ``binary --version`` (``None`` values if it cannot run)."""

    try:
        return parse_version_output(version_output(binary))
    except RuntimeError:
        return {"version": None, "git_sha": None}


def version_tuple(text: str) -> tuple[int, ...]:
    """``"8.1.2"`` -> ``(8, 1, 2)``; empty when ``text`` has no digits."""

    return tuple(int(part) for part in re.findall(r"\d+", text)[:3])


def server_identity(info: Mapping[str, Any]) -> dict[str, Any]:
    """Version, git SHA, and process of an instance from its ``INFO server``."""

    return {
        "version": str(info.get("valkey_version") or info.get("redis_version") or ""),
        "git_sha": str(info.get("valkey_git_sha1") or info.get("redis_git_sha1") or "").lower(),
        "process_id": info.get("process_id"),
        "executable": info.get("executable"),
    }


def instance_identity(handle: Any, instance_id: str) -> dict[str, Any]:
    with handle.new_client(instance_id, timeout=5.0) as client:
        return server_identity(client.info("server"))


def instance_role(handle: Any, instance_id: str) -> Optional[str]:
    """``primary``, ``replica`` (linked), ``replica-syncing``, or ``None`` if unreachable."""

    info = replication_info(handle, instance_id, 2.0)
    if info is None:
        return None
    if replication_role(info) == "primary":
        return "primary"
    return "replica" if parse_replication(info).link_up else "replica-syncing"


class PrivateServer:
    """A throwaway valkey-server on a unix socket in its own temporary directory.

    The directory and the server carry this process's owner tag, so
    ``valkey-fuzzer sweep`` removes them if the fuzzer dies before ``stop``.
    ``files`` (relative path: bytes) are written into the directory first. With
    ``config_file`` the server starts from ``valkey.conf`` in its directory,
    written once from its options, so ``CONFIG REWRITE`` works and a later
    start reads what it rewrote.

    ``start`` returns ``None`` when the server answers PING, or the server's
    output when it exits during startup; it raises ``TimeoutError`` if the
    server neither answers nor exits in time. ``shut_down`` stops it and keeps
    its directory, for ``start`` again.
    """

    def __init__(
        self,
        binary: str,
        arguments: Sequence[str] = (),
        *,
        timeout_s: float = 15.0,
        prefix: str = "vfs-",
        files: Optional[Mapping[str, bytes]] = None,
        config_file: bool = False,
    ):
        self.binary = binary
        self.arguments = list(arguments)
        self.timeout_s = timeout_s
        self.config_file = config_file
        # Unix socket paths are limited to ~104 bytes on macOS, so stay short.
        self.workdir = tempfile.mkdtemp(prefix=prefix, dir="/tmp" if os.path.isdir("/tmp") else None)
        tag_directory(self.workdir, "private-server", binary)
        for name, content in (files or {}).items():
            path = os.path.join(self.workdir, name)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "wb") as handle:
                handle.write(content)
        self.socket_path = os.path.join(self.workdir, "s.sock")
        self.log_path = os.path.join(self.workdir, "server.log")
        # Its stdout and stderr go to a file: a pipe nobody reads while it runs would block a chatty server.
        self.output_path = os.path.join(self.workdir, "output.log")
        self.process: Optional[subprocess.Popen] = None
        self.client: Optional[valkey.Valkey] = None

    def start(self) -> Optional[str]:
        command = [
            self.binary,
            "--port", "0",
            "--unixsocket", self.socket_path,
            "--unixsocketperm", "700",
            "--dir", self.workdir,
            "--logfile", self.log_path,
            "--save", "",
            "--appendonly", "no",
            "--daemonize", "no",
            *server_tag_arguments(),
            *self.arguments,
        ]  # fmt: skip
        if self.config_file:
            path = os.path.join(self.workdir, "valkey.conf")
            if not os.path.exists(path):
                with open(path, "w", encoding="utf-8") as handle:
                    handle.writelines(
                        f"{name[2:]} {config_value(value)}\n" for name, value in zip(command[1::2], command[2::2])
                    )
            command = [self.binary, path]
        # Appended to, so a second start keeps what the first wrote.
        with open(self.output_path, "ab") as output:
            self.process = subprocess.Popen(command, stdout=output, stderr=subprocess.STDOUT, cwd=self.workdir)
        deadline = time.monotonic() + self.timeout_s
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                return self.exit_output()
            if os.path.exists(self.socket_path):
                client = valkey.Valkey(unix_socket_path=self.socket_path, socket_timeout=10, decode_responses=True)
                try:
                    client.ping()
                    self.client = client
                    return None
                except valkey.exceptions.BusyLoadingError:
                    self.client = client
                    return None
                except valkey.exceptions.ConnectionError:
                    client.close()
            # A helper without a run context; the wait is bounded by timeout_s.
            time.sleep(0.05)
        raise TimeoutError(f"private server did not become ready within {self.timeout_s}s")

    def exit_output(self) -> str:
        output = ""
        for path in (self.output_path, self.log_path):
            try:
                with open(path, encoding="utf-8", errors="replace") as handle:
                    output += handle.read()
            except OSError:
                pass
        return output.strip()[-2000:] or "server exited without output"

    def shut_down(self, timeout_s: float) -> bool:
        """Stop the server as ``SHUTDOWN`` does (``SIGTERM``), keeping its directory: whether it exited within
        ``timeout_s``."""

        close_quietly(self.client)
        self.client = None
        if self.process is None:
            return True
        self.process.terminate()
        try:
            self.process.wait(timeout=timeout_s)
        except subprocess.TimeoutExpired:
            return False
        return True

    def stop(self) -> None:
        if (
            self.client is not None
            and "--loadmodule" in self.arguments
            and self.process
            and self.process.poll() is None
        ):
            # A module's data outlives the shutdown (a module with data types is never unloaded), and valkey-json
            # keeps its pointers with metadata in their top bits, which LeakSanitizer cannot follow: every document
            # still stored would read as leaked. Freed first, only what a module truly loses is reported.
            try:
                self.client.execute_command("FLUSHALL", "SYNC")
            except (valkey.exceptions.ValkeyError, OSError):
                pass
        close_quietly(self.client)
        if self.process is not None:
            if self.process.poll() is None:
                self.process.terminate()
                try:
                    self.process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    kill_server(self.process)
                    self.process.wait()
        shutil.rmtree(self.workdir, ignore_errors=True)


def config_value(value: str) -> str:
    """``value`` as one argument of a config file line, quoted as Valkey quotes a command-line argument, so the
    file means what the command line would."""

    escapes = {"\\": "\\\\", '"': '\\"', "\n": "\\n", "\r": "\\r", "\t": "\\t", "\a": "\\a", "\b": "\\b"}
    return (
        '"' + "".join(escapes.get(char, char if char.isprintable() else f"\\x{ord(char):02x}") for char in value) + '"'
    )


def server_arguments(config: Mapping[str, Any]) -> list[str]:
    """Render ``{option: value}`` as valkey-server command-line arguments."""

    arguments: list[str] = []
    for option, raw in config.items():
        for value in raw if isinstance(raw, (list, tuple)) else (raw,):
            if isinstance(value, bool):
                value = "yes" if value else "no"
            arguments.extend((f"--{option}", str(value)))
    return arguments
