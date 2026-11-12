"""Host-state tags, and the sweep that removes what a killed fuzzer left behind.

A fuzzer killed with SIGKILL, or by a power loss, never runs its cleanup: its
valkey-server processes keep running, and its iptables chains, tmpfs mounts,
and data directories stay. So every change the fuzzer makes to the host names
its owner, and the sweep finds leftovers by those names alone, without a
database. The owner is the fuzzer process that made the change:
``<pid>-<start>``, where ``<start>`` is six hex digits of a hash of that
process's start time, so a later process that reuses the PID is not taken for
the owner.

===================  ===============================================================
Host change          Tag
===================  ===============================================================
data directory       a ``.valkey-fuzzer-owner`` file (JSON: ``owner``, ``pid``,
                     ``kind``, ``label``, ``created_at``) in each deployment
                     directory, ``valkey.resource.tmpdir`` directory, and private
                     server directory
valkey-server        ``valkey-fuzzer-owner=<owner>`` in its process title, set with
                     ``--proc-title-template`` (and so also in its arguments)
iptables chain       the name ``VF-<owner>-<8 hex digits>``
tmpfs mount          the mount point ``<tmp>/valkey-fuzzer-mounts/<owner>/<name>``
                     (an empty mount point, made before its mount, too)
===================  ===============================================================

:func:`sweep` lists every tagged item whose owner process is gone and, unless
``dry_run``, removes it: it kills the servers, deletes the chains, unmounts
the mounts, and removes the directories, in that order. Anything untagged, and
anything whose owner is alive (or whose liveness cannot be decided), is never
touched; nor is a directory that, with its tag file, belongs to another user,
nor a shared one (:func:`shared_directory`), whatever tag it holds.
Tagged directories are found in the immediate children of the
roots it scans (``/tmp/valkey-fuzzer``, ``/tmp``, and the temporary directory
by default); servers, chains, and mounts are found host-wide, servers only
among the sweeping user's processes in its PID namespace. Chains and
mounts need root; without it the sweep reports them as failed.

It also makes the run folders under the shared artifact root
(:func:`new_run_folder`) and turns SIGTERM into an interrupt
(:func:`interrupt_on_sigterm`), so a stopped run still undoes its host changes.
"""

import errno
import hashlib
import json
import os
import platform
import re
import secrets
import shutil
import signal
import stat
import subprocess
import tempfile
import time
from collections.abc import Collection, Iterable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from typing import Any, Callable, Optional

from .deployment.local.models import DEFAULT_BASE_DATA_DIR

OWNER_FILE = ".valkey-fuzzer-owner"
TITLE_TAG = "valkey-fuzzer-owner="
CHAIN_PREFIX = "VF-"
MOUNT_ROOT_NAME = "valkey-fuzzer-mounts"
# Sticky and world-writable, like /tmp: see shared_directory.
SHARED_MODE = 0o1777
COMMAND_TIMEOUT_S = 30.0
# How long a killed server may take to disappear.
KILL_WAIT_S = 5.0
_UNKNOWN_START = "000000"

_OWNER = r"(\d+)-([0-9a-f]{6})"
_OWNER_RE = re.compile(rf"^{_OWNER}$")
_TITLE_RE = re.compile(rf"{re.escape(TITLE_TAG)}{_OWNER}(?!\S)")
_CHAIN_RE = re.compile(rf"^-N ({re.escape(CHAIN_PREFIX)}{_OWNER}-[0-9a-f]{{8}})$")
# The server, and the children it forks (valkey-rdb-bgsave, valkey-aof-rewrite, valkey-rdb-to-replicas,
# valkey-module-fork, ...), which carry its title, tag included, and outlive a SIGKILLed server.
_SERVER_BINARY_RE = re.compile(r"(valkey|redis)-(server|rdb-|aof-|module-fork|slot-migration-)")


# ---------------------------------------------------------------------------
# Owners
# ---------------------------------------------------------------------------


def process_start(pid: int) -> Optional[str]:
    """When process ``pid`` started, as an opaque string; None if it cannot be read.

    Linux reads ``/proc/<pid>/stat`` (start time in clock ticks since boot);
    elsewhere ``ps -o lstart=``, in UTC: it prints local time, so a sweep run
    under another time zone would otherwise take a live owner for a dead one.
    """

    try:
        with open(f"/proc/{pid}/stat", encoding="utf-8") as handle:
            return handle.read().rsplit(")", 1)[1].split()[19]
    except (OSError, IndexError):
        pass
    result = run_bounded(["ps", "-o", "lstart=", "-p", str(pid)], env={**os.environ, "LC_ALL": "C", "TZ": "UTC0"})
    started = result.stdout.strip()
    return started if result.returncode == 0 and started else None


def _start_key(started: Optional[str]) -> str:
    return _UNKNOWN_START if started is None else hashlib.sha256(started.encode()).hexdigest()[:6]


@dataclass(frozen=True)
class Owner:
    """The fuzzer process that made a host change."""

    pid: int
    start: str

    @property
    def token(self) -> str:
        return f"{self.pid}-{self.start}"

    @classmethod
    def parse(cls, text: Any) -> Optional["Owner"]:
        match = _OWNER_RE.match(text) if isinstance(text, str) else None
        return cls(int(match.group(1)), match.group(2)) if match else None

    def alive(self) -> bool:
        """Whether the owner still runs; True whenever that cannot be ruled out."""

        try:
            os.kill(self.pid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            pass
        except OSError:
            return True
        if self.start == _UNKNOWN_START:
            return True
        started = process_start(self.pid)
        return started is None or _start_key(started) == self.start


_CURRENT: dict[int, Owner] = {}


def owner_of(pid: int) -> Owner:
    """Process ``pid`` as an owner: what its own :func:`current_owner` is (the batch runner's workers)."""

    return Owner(pid, _start_key(process_start(pid)))


def current_owner() -> Owner:
    """This process as an owner (computed once per process, so a forked child gets its own)."""

    pid = os.getpid()
    if pid not in _CURRENT:
        _CURRENT[pid] = owner_of(pid)
    return _CURRENT[pid]


# ---------------------------------------------------------------------------
# Shared directories
# ---------------------------------------------------------------------------


def shared_directory(path: str) -> str:
    """Make ``path``, a directory every user's runs share, and return it.

    A directory this call creates gets :data:`SHARED_MODE`, like /tmp: any
    user can make entries in it, and none can remove or rename another's.
    Otherwise a run as root (partitions and tmpfs need root) would leave it
    root's, mode 0755, and every later run of another user could not write
    there. Missing parents are made as usual, and an existing directory keeps
    its mode: the fuzzer changes only what it has just made.
    """

    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    try:
        os.mkdir(path)
    except FileExistsError:
        if not os.path.isdir(path):
            raise
        return path
    os.chmod(path, SHARED_MODE)  # mkdir's mode is masked by the umask; chmod's is not
    return path


def open_shared_lock(path: str) -> int:
    """A read-only descriptor of lock file ``path`` in a shared directory, made (mode 0644) if missing.

    ``flock`` needs no write access, so any user can lock another's file.
    An existing file is opened without ``O_CREAT``: where
    ``fs.protected_regular`` is set (systemd's default), the kernel refuses
    ``O_CREAT`` on a file in a sticky world-writable directory that neither
    the caller nor the directory's owner owns, root included.
    """

    flags = os.O_RDONLY | os.O_NOFOLLOW
    try:
        return os.open(path, flags | os.O_CREAT | os.O_EXCL, 0o644)
    except FileExistsError:
        return os.open(path, flags)


# ---------------------------------------------------------------------------
# Run folders
# ---------------------------------------------------------------------------


def default_root() -> str:
    """Where runs leave their folders without an artifact directory: ``<tmp>/valkey-fuzzer``."""

    return os.path.join(tempfile.gettempdir(), "valkey-fuzzer")


def normalize_artifact_root(artifact_dir: str) -> str:
    """Absolute artifact root; empty means :func:`default_root`."""

    if not isinstance(artifact_dir, str) or "\x00" in artifact_dir:
        raise ValueError("artifact_dir must be a path string")
    root = os.path.normpath(os.path.abspath(artifact_dir or default_root()))
    if root == os.path.abspath(os.sep):
        raise ValueError("artifact_dir must not resolve to the filesystem root")
    return root


def folder_name(name: str) -> str:
    """``name`` as one safe path segment: anything but letters, digits, '.', '_', and '-' becomes '-'."""

    return re.sub(r"[^A-Za-z0-9_.-]+", "-", name).strip("-.")[:80] or "run"


def _timestamp() -> str:
    now = time.time()
    return f"{time.strftime('%Y%m%dT%H%M%S', time.gmtime(now))}.{int(now * 1000) % 1000:03d}Z"


def new_run_folder(parent: str, name: str) -> str:
    """Create and return ``<parent>/<UTC time>-<name>-<suffix>``; raises ``OSError`` if it cannot.

    The time sorts folders by start; the random suffix and exclusive creation
    keep two runs that start in the same millisecond, in any process, apart.
    Every user's runs share :func:`default_root` and its ``<kind>`` folders
    (``runs``, ``batches``, ...), so ``parent`` there is a shared directory,
    as is the root (:func:`shared_directory`); the
    run's folder, and a root the caller names, are made as usual.
    """

    if os.path.dirname(os.path.abspath(parent)) == default_root():
        shared_directory(default_root())
        shared_directory(parent)
    else:
        os.makedirs(parent, exist_ok=True)
    for _ in range(8):
        path = os.path.join(parent, f"{_timestamp()}-{folder_name(name)}-{secrets.token_hex(3)}")
        try:
            os.mkdir(path)
        except FileExistsError:
            continue
        return path
    raise FileExistsError(f"could not find an unused folder name under {parent}")


# ---------------------------------------------------------------------------
# Signals
# ---------------------------------------------------------------------------


@contextmanager
def interrupt_on_sigterm() -> Iterator[None]:
    """Within, SIGTERM raises :class:`KeyboardInterrupt` as Ctrl-C does.

    Its default action ends the process at once, so a run stopped with it
    would tear down nothing and leave its servers running; this way it is
    cleaned up like an interrupted run, and the command exits 130.
    """

    def interrupt(signum: int, _frame: Any) -> None:
        raise KeyboardInterrupt(signal.Signals(signum).name)

    previous = signal.signal(signal.SIGTERM, interrupt)
    try:
        yield
    finally:
        signal.signal(signal.SIGTERM, previous)


# ---------------------------------------------------------------------------
# Tagging
# ---------------------------------------------------------------------------


def tag_directory(path: str, kind: str, label: str = "") -> None:
    """Mark ``path`` as created by this process: the sweep removes it once this process is gone."""

    owner = current_owner()
    record = {"owner": owner.token, "pid": owner.pid, "kind": kind, "label": label, "created_at": time.time()}
    temporary = os.path.join(path, f"{OWNER_FILE}.{os.getpid()}.tmp")
    with open(temporary, "w", encoding="utf-8") as handle:
        json.dump(record, handle, sort_keys=True)
    os.replace(temporary, os.path.join(path, OWNER_FILE))


def untag_directory(path: str) -> None:
    """Drop ``path``'s tag: it is kept on purpose (``keep``, ``enable_cleanup: false``), so the sweep leaves it."""

    try:
        os.remove(os.path.join(path, OWNER_FILE))
    except FileNotFoundError:
        pass


def read_tag(path: str) -> Optional[dict[str, Any]]:
    """The tag of directory ``path``, or None when it has no readable tag."""

    try:
        with open(os.path.join(path, OWNER_FILE), encoding="utf-8") as handle:
            record = json.load(handle)
    except (OSError, ValueError):
        return None
    return record if isinstance(record, dict) and Owner.parse(record.get("owner")) else None


def _own_tag(path: str) -> Optional[dict[str, Any]]:
    """:func:`read_tag`, when directory ``path`` and its tag file both belong to the user running the sweep.

    Anyone can write a tag naming a dead PID under /tmp; another user's tag
    is not this user's to act on (and, without root, could never be removed).
    Nor is a sticky directory one run's leftover: it is a shared one
    (:func:`shared_directory`), holding every user's runs.
    """

    try:
        uid = os.geteuid()
        info = os.lstat(path)
        if info.st_uid != uid or info.st_mode & stat.S_ISVTX or os.lstat(os.path.join(path, OWNER_FILE)).st_uid != uid:
            return None
    except OSError:
        return None
    return read_tag(path)


def server_tag_arguments() -> list[str]:
    """valkey-server arguments that put this process's tag in the server's process title.

    Valkey rewrites its process title (and with it what ``ps`` shows as its
    arguments) from this template; with ``set-proc-title no`` the argument
    itself still carries the tag.
    """

    return ["--proc-title-template", f"{{title}} {{listen-addr}} {{server-mode}} {TITLE_TAG}{current_owner().token}"]


def chain_name(label: str) -> str:
    """``VF-<owner>-<hash of label>``: an iptables chain name of at most 26 characters."""

    return f"{CHAIN_PREFIX}{current_owner().token}-{hashlib.sha256(label.encode()).hexdigest()[:8]}"


def mount_parent() -> str:
    """The directory this process creates its tmpfs mount points in: ``<tmp>/valkey-fuzzer-mounts/<owner>``.

    ``valkey-fuzzer-mounts`` is shared (:func:`shared_directory`); the owner directory is this process's own.
    """

    parent = os.path.join(shared_directory(os.path.join(tempfile.gettempdir(), MOUNT_ROOT_NAME)), current_owner().token)
    os.makedirs(parent, mode=0o755, exist_ok=True)
    return parent


def release_mount_parent(mount_point: str) -> None:
    """Remove a mount point's owner directory once its last mount point is gone."""

    try:
        os.rmdir(os.path.dirname(mount_point))
    except OSError:
        pass


# ---------------------------------------------------------------------------
# Host commands
# ---------------------------------------------------------------------------


def run_bounded(command: Sequence[str], *, timeout: float = COMMAND_TIMEOUT_S, **kwargs: Any) -> Any:
    """Run ``command``; a missing program or one that outlives ``timeout`` is a failed call."""

    try:
        return subprocess.run(list(command), capture_output=True, text=True, timeout=timeout, **kwargs)
    except subprocess.TimeoutExpired:
        return subprocess.CompletedProcess(command, -1, "", f"{command[0]} timed out after {timeout:g}s")
    except OSError as exc:
        return subprocess.CompletedProcess(command, -1, "", f"{command[0]}: {exc}")


def run_iptables(*args: str) -> Any:
    """``iptables -w <args>``; ``-w`` waits for the xtables lock, so the call is bounded instead."""

    return run_bounded(["iptables", "-w", *args])


Iptables = Callable[..., Any]


def remove_chain(chain: str, iptables: Iptables = run_iptables) -> list[str]:
    """Delete ``chain`` and every jump to it from INPUT; the errors, empty once it is gone."""

    errors = []
    while iptables("-C", "INPUT", "-j", chain).returncode == 0:
        removed = iptables("-D", "INPUT", "-j", chain)
        if removed.returncode != 0:
            errors.append(removed.stderr.strip())
            break
    for args in (("-F", chain), ("-X", chain)):
        result = iptables(*args)
        if result.returncode != 0 and "No chain" not in result.stderr and "does not exist" not in result.stderr:
            errors.append(result.stderr.strip())
    return errors


def tagged_chains(iptables: Iptables = run_iptables) -> Optional[list[tuple[str, Owner]]]:
    """Every chain with a fuzzer name and its owner; None when the chains cannot be listed."""

    listing = iptables("-S")
    if listing.returncode != 0:
        return None
    chains = []
    for line in listing.stdout.splitlines():
        match = _CHAIN_RE.match(line.strip())
        if match:
            chains.append((match.group(1), Owner(int(match.group(2)), match.group(3))))
    return chains


# ---------------------------------------------------------------------------
# Finding leftovers
# ---------------------------------------------------------------------------


@dataclass
class Leftover:
    """One tagged host change: what it is, who made it, and what the sweep did with it.

    ``kind`` is ``server`` (``target`` is the PID), ``chain``, ``mount``,
    ``mount point`` (an empty one), or ``directory`` (``target`` is the path). ``action`` is ``removed``,
    ``would remove`` (a dry run), ``failed`` (``reason`` says why), or
    ``skipped`` (``--older-than`` excluded it, or it changed hands since it
    was found; ``reason`` says which).
    """

    kind: str
    target: str
    owner: str
    age_s: Optional[float]
    detail: str = ""
    action: str = ""
    reason: str = ""


@dataclass
class SweepReport:
    """What a sweep found and did; ``unchecked`` lists what it could not even look for."""

    dry_run: bool
    leftovers: list[Leftover] = field(default_factory=list)
    unchecked: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return all(item.action != "failed" for item in self.leftovers)

    def to_data(self) -> dict[str, Any]:
        return {
            "dry_run": self.dry_run,
            "ok": self.ok,
            "leftovers": [asdict(item) for item in self.leftovers],
            "unchecked": list(self.unchecked),
        }


def _elapsed_seconds(text: str) -> Optional[float]:
    """``ps -o etime`` (``[[dd-]hh:]mm:ss``) in seconds."""

    days, _, clock = text.rpartition("-")
    try:
        parts = [int(part) for part in clock.split(":")]
        seconds = 0
        for part in parts:
            seconds = seconds * 60 + part
        return seconds + (int(days) * 86400 if days else 0)
    except ValueError:
        return None


def _pid_namespace(pid: Any) -> Optional[str]:
    """The PID namespace of process ``pid`` (or ``"self"``) on Linux; None elsewhere, or once it is gone."""

    try:
        return os.readlink(f"/proc/{pid}/ns/pid")
    except OSError:
        return None


def own_processes() -> list[tuple[int, Optional[float], str]]:
    """``(pid, age in seconds, command line)`` of every process the sweep may act on.

    Those are the processes of the user running the sweep, in its PID namespace. An owner's liveness is
    judged by its PID, which names another process (or none) in another namespace: a fuzzer in a container
    would look dead, and its live servers would be killed. Another user's servers are not this user's to
    kill, root's sweep included, as with directories (:func:`_own_tag`).
    """

    result = run_bounded(["ps", "-A", "-ww", "-o", "pid=", "-o", "uid=", "-o", "etime=", "-o", "command="])
    if result.returncode != 0:
        raise RuntimeError(f"cannot list processes: {result.stderr.strip()}")
    uid, namespace = str(os.geteuid()), _pid_namespace("self")
    processes = []
    for line in result.stdout.splitlines():
        parts = line.split(None, 3)
        if len(parts) == 4 and parts[0].isdigit() and parts[1] == uid:
            pid = int(parts[0])
            if _pid_namespace(pid) == namespace:
                processes.append((pid, _elapsed_seconds(parts[2]), parts[3]))
    return processes


def server_owner(command: str) -> Optional[Owner]:
    """The owner a valkey-server command line is tagged with, or None (untagged, or not a server)."""

    match = _TITLE_RE.search(command)
    program = command.split(None, 1)[0] if command.strip() else ""
    if match is None or not _SERVER_BINARY_RE.search(os.path.basename(program)):
        return None
    return Owner(int(match.group(1)), match.group(2))


# Whether a leftover's owner is one the sweep removes for: a gone one (and one it was asked about).
Gone = Callable[[Owner], bool]


def _find_servers(gone: Gone) -> list[Leftover]:
    found = []
    for pid, age, command in own_processes():
        owner = server_owner(command)
        if owner is not None and pid != os.getpid() and gone(owner):
            found.append(Leftover("server", str(pid), owner.token, age, command))
    return found


def _decode_mount_point(field_text: str) -> str:
    """A /proc/mounts path with its octal escapes (``\\040`` for a space) decoded."""

    return re.sub(r"\\([0-7]{3})", lambda match: chr(int(match.group(1), 8)), field_text)


def list_mounts() -> list[tuple[str, str]]:
    """``(mount point, filesystem type)`` of every mount; empty where ``/proc/mounts`` does not exist."""

    try:
        with open("/proc/mounts", encoding="utf-8") as handle:
            lines = handle.read().splitlines()
    except OSError:
        return []
    return [
        (_decode_mount_point(fields[1]), fields[2]) for fields in (line.split() for line in lines) if len(fields) > 2
    ]


def mount_owner(mount_point: str) -> Optional[Owner]:
    """The owner of a mount point under a ``valkey-fuzzer-mounts/<owner>/`` directory, or None."""

    parent = os.path.dirname(mount_point)
    if os.path.basename(os.path.dirname(parent)) != MOUNT_ROOT_NAME:
        return None
    return Owner.parse(os.path.basename(parent))


def _age(now: float, since: Any) -> Optional[float]:
    return max(0.0, now - since) if isinstance(since, (int, float)) and not isinstance(since, bool) else None


def _find_mounts(now: float, gone: Gone) -> list[Leftover]:
    found = []
    for mount_point, kind in list_mounts():
        owner = mount_owner(mount_point)
        if kind == "tmpfs" and owner is not None and gone(owner):
            try:
                # The owner directory changes when a mount point is made in it: a lower bound on the age.
                since: Optional[float] = os.stat(os.path.dirname(mount_point)).st_mtime
            except OSError:
                since = None
            found.append(Leftover("mount", mount_point, owner.token, _age(now, since), kind))
    return found


def _subdirectories(path: str) -> list[os.DirEntry]:
    try:
        with os.scandir(path) as entries:
            return sorted((entry for entry in entries if entry.is_dir(follow_symlinks=False)), key=lambda e: e.name)
    except OSError:
        return []


def _find_mount_points(roots: Iterable[str], now: float, gone: Gone) -> list[Leftover]:
    """Empty mount points of gone owners, in ``<root>/valkey-fuzzer-mounts/<owner>/``.

    A tmpfs provider killed between making its mount point and mounting on
    it (or between unmounting and removing it) leaves one; its owner is the
    name of the directory it is in. Mounted ones are ``mount`` leftovers.
    """

    mounted = {point for point, _ in list_mounts()}
    found, seen = [], set()
    for root in roots:
        for parent in _subdirectories(os.path.join(root, MOUNT_ROOT_NAME)):
            owner = Owner.parse(parent.name)
            try:
                ours = parent.stat(follow_symlinks=False).st_uid == os.geteuid()
            except OSError:
                ours = False
            if owner is None or not ours or not gone(owner):
                continue
            for entry in _subdirectories(parent.path):
                path = os.path.realpath(entry.path)
                if path in seen or entry.path in mounted or path in mounted:
                    continue
                seen.add(path)
                try:
                    since: Optional[float] = entry.stat(follow_symlinks=False).st_mtime
                except OSError:
                    since = None
                found.append(Leftover("mount point", entry.path, owner.token, _age(now, since)))
    return found


def default_roots() -> list[str]:
    return [DEFAULT_BASE_DATA_DIR, "/tmp", tempfile.gettempdir()]


def _find_directories(roots: Iterable[str], now: float, gone: Gone) -> list[Leftover]:
    found = []
    seen = set()
    for root in roots:
        try:
            entries = list(os.scandir(root))
        except OSError:
            continue
        for entry in sorted(entries, key=lambda item: item.name):
            try:
                if not entry.is_dir(follow_symlinks=False):
                    continue
            except OSError:
                continue
            path = os.path.realpath(entry.path)
            tag = _own_tag(entry.path)
            if tag is None or path in seen:
                continue
            seen.add(path)
            owner = Owner.parse(tag["owner"])
            if gone(owner):
                detail = f"{tag.get('kind', '')} {tag.get('label', '')}".strip()
                found.append(Leftover("directory", entry.path, owner.token, _age(now, tag.get("created_at")), detail))
    return found


# ---------------------------------------------------------------------------
# Removing leftovers
# ---------------------------------------------------------------------------


def _is_root() -> bool:
    return hasattr(os, "geteuid") and os.geteuid() == 0


def _kill_server(item: Leftover) -> Optional[str]:
    pid = int(item.target)
    # The PID may have been reused since the listing: kill it only if it still carries the same tag.
    current = run_bounded(["ps", "-ww", "-o", "command=", "-p", str(pid)])
    owner = server_owner(current.stdout.strip()) if current.returncode == 0 else None
    if owner is None or owner.token != item.owner:
        return "its PID now belongs to another process"
    try:
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        return None
    except PermissionError:
        raise RuntimeError("permission denied: the server belongs to another user") from None
    deadline = time.monotonic() + KILL_WAIT_S
    while time.monotonic() < deadline:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return None
        time.sleep(0.05)
    raise RuntimeError(f"still running {KILL_WAIT_S:g}s after SIGKILL")


def _remove_chain(item: Leftover) -> Optional[str]:
    errors = remove_chain(item.target, run_iptables)
    if errors:
        raise RuntimeError("; ".join(errors))
    return None


def _unmount(item: Leftover) -> Optional[str]:
    if not _is_root():
        raise RuntimeError("unmounting needs root")
    result = run_bounded(["umount", item.target])
    if result.returncode != 0:
        raise RuntimeError(f"umount: {result.stderr.strip()}")
    os.rmdir(item.target)
    release_mount_parent(item.target)
    return None


def _remove_mount_point(item: Leftover) -> Optional[str]:
    # rmdir: only an empty directory, never one a tmpfs was mounted on since.
    os.rmdir(item.target)
    release_mount_parent(item.target)
    return None


def _make_removable(path: str, device: int, parent: Optional[int] = None) -> None:
    """Give every directory under ``path`` owner rwx (a deny-writes fault may have left one read-only).

    Each directory is opened without following symlinks and changed through
    that descriptor: a path swapped for a symlink after it was listed fails to
    open, so a root sweep never changes the mode of what the symlink names.
    Refuses a tree that reaches into another filesystem: removing it would
    delete what is mounted there.
    """

    flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_DIRECTORY
    try:
        handle = os.open(path, flags, dir_fd=parent)
    except PermissionError:
        # Unreadable to its owner (root opens anything): made readable first,
        # without following a symlink (one changes nothing, or fails).
        mode = stat.S_IMODE(os.lstat(path, dir_fd=parent).st_mode) | 0o700
        os.chmod(path, mode, dir_fd=parent, follow_symlinks=False)
        handle = os.open(path, flags, dir_fd=parent)
    try:
        info = os.fstat(handle)
        if info.st_dev != device:
            raise RuntimeError(f"{path} is on another filesystem (a mount point); nothing was removed")
        if stat.S_IMODE(info.st_mode) & 0o700 != 0o700:
            os.fchmod(handle, stat.S_IMODE(info.st_mode) | 0o700)
        with os.scandir(handle) as entries:
            names = [entry.name for entry in entries if entry.is_dir(follow_symlinks=False)]
        for name in names:
            _make_removable(name, device, handle)
    finally:
        os.close(handle)


def _remove_directory(item: Leftover) -> Optional[str]:
    # Re-read the tag: the directory must still belong to the same dead owner.
    tag = _own_tag(item.target)
    if tag is None or tag["owner"] != item.owner:
        return "its tag changed since it was found"
    _make_removable(item.target, os.lstat(item.target).st_dev)
    shutil.rmtree(item.target)
    return None


# Each remover returns why it left the item alone, or None once it is removed.
_REMOVERS: dict[str, Callable[[Leftover], Optional[str]]] = {
    "server": _kill_server,
    "chain": _remove_chain,
    "mount": _unmount,
    "mount point": _remove_mount_point,
    "directory": _remove_directory,
}


def sweep(
    roots: Optional[Sequence[str]] = None,
    *,
    older_than: Optional[float] = None,
    dry_run: bool = False,
    owners: Optional[Collection[str]] = None,
) -> SweepReport:
    """Find the leftovers of fuzzer processes that are gone and, unless ``dry_run``, remove them.

    ``roots`` are the directories whose immediate children are checked for
    tagged directories (default: :func:`default_roots`). ``older_than`` leaves
    anything younger than that many seconds, and anything whose age is unknown
    (a chain), alone. ``owners`` (owner tokens) limits the sweep to what those
    processes left: the batch runner sweeps each worker after its run.
    """

    def gone(owner: Owner) -> bool:
        # The owners asked about first: a liveness check may spawn ps.
        return (owners is None or owner.token in owners) and not owner.alive()

    now = time.time()
    report = SweepReport(dry_run=dry_run)
    found = _find_servers(gone)
    if platform.system() == "Linux" and shutil.which("iptables"):
        chains = tagged_chains(run_iptables) if _is_root() else None
        if chains is None:
            report.unchecked.append("iptables chains: listing them needs root")
        else:
            found += [Leftover("chain", chain, owner.token, None) for chain, owner in chains if gone(owner)]
    roots = default_roots() if roots is None else roots
    found += _find_mounts(now, gone)
    found += _find_mount_points(roots, now, gone)
    found += _find_directories(roots, now, gone)

    for item in found:
        if older_than is not None and (item.age_s is None or item.age_s < older_than):
            item.action = "skipped"
            item.reason = "age unknown" if item.age_s is None else f"younger than {older_than:g}s"
        elif dry_run:
            item.action = "would remove"
        else:
            try:
                reason = _REMOVERS[item.kind](item)
                item.action = "skipped" if reason else "removed"
                item.reason = reason or ""
            except Exception as exc:
                item.action = "failed"
                item.reason = str(exc) or type(exc).__name__
                if isinstance(exc, OSError) and exc.errno in (errno.EPERM, errno.EACCES):
                    item.reason += " (needs root or the owning user)"
        report.leftovers.append(item)
    return report
