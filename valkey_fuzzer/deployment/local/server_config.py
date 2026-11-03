"""Shared validation and argv conversion for Valkey server configuration, and the provider's defaults."""

import re
from collections.abc import Iterable, Mapping
from enum import Enum
from typing import Any, Optional

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
