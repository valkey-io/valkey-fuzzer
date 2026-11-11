"""Shared validation and argv conversion for Valkey server configuration, and the provider's defaults."""

import functools
import logging
import os
import re
import shutil
import subprocess
from collections.abc import Iterable, Mapping, Sequence
from enum import Enum
from typing import Any, Optional

logger = logging.getLogger(__name__)

_SERVER_OPTION_RE = re.compile(r"[a-z0-9][a-z0-9-]*")

# These options describe process and deployment metadata owned by the process manager.
# Scenarios must not override them because doing so would invalidate
# node identity, lifecycle, or the requested deployment.
MANAGED_SERVER_OPTIONS = frozenset(
    {
        "bind",
        # Set with per-instance addressing, so network faults can match instance pairs.
        "bind-source-addr",
        "cluster-announce-bus-port",
        "cluster-announce-hostname",
        "cluster-announce-human-nodename",
        "cluster-announce-ip",
        "cluster-announce-port",
        "cluster-announce-tls-port",
        "cluster-config-file",
        "cluster-enabled",
        "cluster-port",
        "daemonize",
        "dir",
        "include",
        "logfile",
        "pidfile",
        "port",
        # Carries the owner tag valkey-fuzzer sweep finds leftover servers by.
        "proc-title-template",
        "protected-mode",
        "replica-announce-ip",
        "replica-announce-port",
        "replicaof",
        "slave-announce-ip",
        "slave-announce-port",
        "slaveof",
        "tls-port",
        "unixsocket",
        "unixsocketperm",
    }
)


# A hard cap that keeps a fuzzing run from exhausting the host. It is
# deliberately paired with Valkey's default ``noeviction`` policy, so hitting
# the cap surfaces as write errors instead of silently evicting the data the
# validators check.
DEFAULT_MAXMEMORY = "500mb"

# The server config every local instance starts with unless the deployment,
# its instance configs, or a startup overlay sets the option. The plan records
# each instance's effective config (these under the explicit layers), so
# compile-time checks see a default the same as a setting written out.
DEFAULT_SERVER_CONFIG = {
    "loglevel": "notice",
    "appendonly": "yes",
    "appendfilename": "appendonly.aof",
    "appendfsync": "everysec",
    "save": "",
    "maxmemory": DEFAULT_MAXMEMORY,
    # A crashing server otherwise tests its memory for hardware faults before it exits. That tells a fuzzing run
    # nothing, and under AddressSanitizer the test itself is reported (it copies sanitizer-owned memory, and its
    # region table overflows past 128 mappings: valkey#4213), burying the crash that started it.
    "crash-memcheck-enabled": "no",
}
# Cluster mode adds fast failure detection and keeps serving with slots uncovered.
CLUSTER_DEFAULT_SERVER_CONFIG = {"cluster-node-timeout": "1000", "cluster-require-full-coverage": "no"}


def effective_server_config(cluster: bool, server_config: Optional[Mapping[str, Any]]) -> dict[str, Any]:
    """The provider's defaults that ``server_config`` (canonical) leaves unset, then ``server_config`` itself."""

    explicit = dict(server_config or {})
    defaults = {**DEFAULT_SERVER_CONFIG, **(CLUSTER_DEFAULT_SERVER_CONFIG if cluster else {})}
    return {**{option: value for option, value in defaults.items() if option not in explicit}, **explicit}


def normalize_server_option(raw_option: object) -> str:
    """Normalize a config key and reject the options the deployment sets itself."""
    if not isinstance(raw_option, str):
        raise TypeError(f"Server configuration keys must be strings, got {type(raw_option).__name__}")

    option = raw_option.strip().lower()
    while option.startswith("--"):
        option = option[2:]
    option = option.replace("_", "-")

    if not option or not _SERVER_OPTION_RE.fullmatch(option):
        raise ValueError(f"Invalid Valkey server configuration option: {raw_option!r}")
    if option in MANAGED_SERVER_OPTIONS:
        raise ValueError(f"Server configuration option {option!r} is managed by the local deployment")
    return option


def server_config_values(option: str, raw_value: object) -> Iterable[str]:
    """Yield complete scalar values for one or more occurrences of an option.

    A list represents repeated config directives. Multi-token Valkey directives
    should use one scalar string per occurrence, for example
    ``"replica 256mb 64mb 60"``.
    """
    values: Iterable[object]
    if isinstance(raw_value, (list, tuple)):
        values = raw_value
    else:
        values = (raw_value,)

    for value in values:
        if value is None:
            raise ValueError(f"Server configuration option {option!r} cannot have a null value")
        if isinstance(value, (dict, list, tuple, set)):
            raise TypeError(f"Server configuration option {option!r} has an unsupported nested value")
        if isinstance(value, Enum):
            value = value.value
        if isinstance(value, bool):
            yield "yes" if value else "no"
        elif isinstance(value, bytes):
            yield value.decode("utf-8")
        else:
            yield str(value)


def canonicalize_server_config(server_config: Optional[Mapping[str, Any]]) -> dict[str, Any]:
    """Validate a config mapping and return one entry per normalized option.

    Repeated directives must be represented by one list or tuple value. Two
    mapping keys such as ``repl_compression`` and ``repl-compression`` are
    ambiguous because both produce the same Valkey command-line option, so
    they are rejected instead of relying on argument order.
    """
    if server_config is None:
        return {}
    if not isinstance(server_config, Mapping):
        raise TypeError(f"Server configuration must be a mapping, got {type(server_config).__name__}")

    canonical = {}
    original_keys = {}
    for raw_option, raw_value in server_config.items():
        option = normalize_server_option(raw_option)
        # Materialize once to validate the complete value without changing its
        # representation; callers may need to preserve repeated directives.
        tuple(server_config_values(option, raw_value))
        if option in canonical:
            raise ValueError(
                f"Server configuration keys {original_keys[option]!r} and {raw_option!r} both normalize to {option!r}"
            )
        canonical[option] = raw_value
        original_keys[option] = raw_option
    return canonical


def build_server_config_arguments(server_config: Mapping[str, Any]) -> list[str]:
    """A canonical server config (:func:`canonicalize_server_config`) as valkey-server arguments."""
    return [
        argument
        for option, raw_value in server_config.items()
        for value in server_config_values(option, raw_value)
        for argument in (f"--{option}", value)
    ]


# The configs Valkey renamed, {old name: (new name, {old value: new value})}: a build knows one name or the
# other, and either may be written. valkey-io/valkey#5261 renamed bgsave-default-method (fork, forkless) to
# forkless-policy (require-fork, prefer-forkless). Every server start, and the build probe's CONFIG GET and
# CONFIG SET, give each build the name it knows (config_for_build).
RENAMED_CONFIGS = {
    "bgsave-default-method": ("forkless-policy", {"fork": "require-fork", "forkless": "prefer-forkless"}),
}
_RENAMED_NAMES = {name for old, (new, _) in RENAMED_CONFIGS.items() for name in (old, new)}


def knows_config(binary: str, name: str, value: str) -> bool:
    """Whether ``binary`` knows config ``name`` (``value`` being one of its values), without starting a server."""

    return "Bad directive" not in _config_probe(binary, name, value)


def accepts_config(binary: str, name: str, value: str) -> bool:
    """Whether ``binary`` starts with config ``name`` set to ``value``, without starting a server."""

    return ">>> 'port" in _config_probe(binary, name, value)


def _config_probe(binary: str, name: str, value: str) -> str:
    """What ``binary`` prints given ``--<name> <value> --port -1``: Valkey stops at the first directive it rejects
    and names it, so the bad port after a config it takes is the one named."""

    path = shutil.which(binary) or binary
    stat = os.stat(path)
    return _probe_output(os.path.realpath(path), stat.st_mtime_ns, stat.st_size, name, value)


@functools.cache
def _probe_output(path: str, mtime_ns: int, size: int, name: str, value: str) -> str:
    command = [path, f"--{name}", value, "--port", "-1"]
    return subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=30).stdout


# Configs that change how a server runs, never what it stores or replies, and that a build may lack or refuse:
# the value a build that refuses the drawn one starts with instead, or None to start it without the config. A
# scenario that draws one (the default pools of valkey.random) then runs on every build, where a server that
# refused it would not start; the plan records the value drawn. Active defragmentation needs jemalloc's defrag
# hints, so a build without them (MALLOC=libc, as the sanitizer builds are) exits on activedefrag yes. The
# IO-thread configs are hidden ones that only some releases know: io-threads-always-active and the copy-avoiding
# replies are 9.1's, events-per-io-thread 8.0's to 9.0's (9.1 accepts and ignores it), prefetch-batch-max-size
# 8.0's and later, and io-threads-do-reads 7.2's and 8.0's (8.1 and later accept and ignore it).
UNOBSERVABLE_CONFIGS = {
    "activedefrag": "no",
    "events-per-io-thread": None,
    "io-threads-always-active": None,
    "io-threads-do-reads": None,
    "min-io-threads-avoid-copy-reply": None,
    "min-string-size-avoid-copy-reply": None,
    "min-string-size-avoid-copy-reply-threaded": None,
    "prefetch-batch-max-size": None,
}


def config_for_build(binary: str, name: str, value: Any) -> tuple[str, Any]:
    """Config ``name`` and ``value`` as ``binary`` knows them, for a config Valkey renamed (:data:`RENAMED_CONFIGS`);
    any other config, and a value the rename does not name, as it is."""

    for old, (new, values) in RENAMED_CONFIGS.items():
        if name in (old, new):
            if knows_config(binary, new, next(iter(values.values()))):
                return new, values.get(str(value), value)
            return old, {to: was for was, to in values.items()}.get(str(value), value)
    return name, value


def arguments_for_build(binary: str, arguments: Sequence[str]) -> list[str]:
    """valkey-server ``arguments`` (``--option value`` pairs) with each renamed config as ``binary`` knows it, and
    each unobservable config it refuses (:data:`UNOBSERVABLE_CONFIGS`) replaced or left out."""

    translated: list[str] = []
    for index in range(0, len(arguments) - 1, 2):
        option, value = arguments[index][2:], arguments[index + 1]
        if option in _RENAMED_NAMES:
            option, value = config_for_build(binary, option, value)
        elif option in UNOBSERVABLE_CONFIGS and value != UNOBSERVABLE_CONFIGS[option]:
            if not accepts_config(binary, option, value):
                instead = UNOBSERVABLE_CONFIGS[option]
                logger.info("%s refuses %s %s: it starts with %s", binary, option, value, instead or "its default")
                if instead is None:
                    continue
                value = instead
        translated += [f"--{option}", value]
    if len(arguments) % 2:
        translated.append(arguments[-1])
    return translated
