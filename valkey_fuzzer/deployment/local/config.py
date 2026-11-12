"""The local deployment document: its schema, defaults, and the configuration it describes.

A deployment runs in standalone or cluster mode. Standalone mode has one
primary and ``replicas_per_primary`` replicas (none by default, which is a
single instance); cluster mode has ``primaries`` shards of one primary and
``replicas_per_primary`` replicas each (three primaries with one replica
each by default).
"""

from collections.abc import Mapping
from typing import Any

from ...schema import ConfigField, ConfigSchema
from .models import CLUSTER_BUS_PORT_OFFSET, DEFAULT_BASE_PORT, DEFAULT_DIMENSIONS, DeploymentConfig, DeploymentMode
from .server_config import canonicalize_server_config

MAX_TCP_PORT = 65535
MAX_CLUSTER_PRIMARIES = 16
MAX_REPLICAS_PER_PRIMARY = 5
ADDRESSING_MODES = ("shared_loopback", "per_instance_loopback")
# valkey-N binds 127.0.B.(N+2), B the loopback block; the last usable address of a block is 127.0.B.254.
MAX_PER_INSTANCE_LOOPBACK_INSTANCES = 253
MAX_LOOPBACK_BLOCK = 255
# The fields that hold paths: they may still be environment references when a plan is compiled.
_PATHS = ConfigField(
    dict, reference=True, required=False, schema=ConfigSchema(additional=ConfigField(str, reference=True))
)


def _schema(mode: DeploymentMode) -> ConfigSchema:
    """The document of one mode. Only the instance counts have defaults, so a normalized
    document gains nothing else; the other fields keep the :class:`DeploymentConfig` defaults."""

    defaults = DEFAULT_DIMENSIONS[mode]
    fields = {
        "mode": ConfigField(str, choices=[mode.value]),
        "replicas_per_primary": ConfigField(
            int, minimum=0, maximum=MAX_REPLICAS_PER_PRIMARY, default=defaults["replicas_per_primary"]
        ),
        "base_port": ConfigField(int, minimum=1, maximum=MAX_TCP_PORT, required=False),
        "base_data_dir": ConfigField(str, reference=True, required=False),
        "valkey_binary": ConfigField(str, reference=True, required=False),
        "enable_cleanup": ConfigField(bool, required=False),
        "server_config": ConfigField(dict, reference=True, required=False),
        "instance_configs": ConfigField(
            dict, required=False, schema=ConfigSchema(additional=ConfigField(dict, reference=True))
        ),
        "instance_binaries": _PATHS,
        "instance_data_roots": _PATHS,
        "addressing": ConfigField(str, choices=ADDRESSING_MODES, required=False),
        "loopback_block": ConfigField(int, minimum=0, maximum=MAX_LOOPBACK_BLOCK, required=False),
    }
    if mode is DeploymentMode.CLUSTER:
        fields["primaries"] = ConfigField(int, minimum=3, maximum=MAX_CLUSTER_PRIMARIES, default=defaults["primaries"])
    return ConfigSchema(fields)


DEPLOYMENT_DOCUMENT = ConfigField(
    dict, reference=True, variants=("mode", {mode.value: _schema(mode) for mode in DeploymentMode})
)


def instance_count(mode: DeploymentMode, primaries: int, replicas_per_primary: int) -> int:
    """How many instances a deployment of this shape runs."""
    return (primaries if mode is DeploymentMode.CLUSTER else 1) * (1 + replicas_per_primary)


def normalize_deployment_document(document: Mapping[str, Any]) -> dict[str, Any]:
    """The deployment document checked against its mode's schema, with the instance counts filled in (pure).

    A cluster document always comes back with ``primaries`` and
    ``replicas_per_primary``, a standalone one with ``replicas_per_primary``;
    every other field comes back as written. Raises :class:`ValueError`.
    """
    if isinstance(document, Mapping) and document.get("mode") == "standalone" and "primaries" in document:
        raise ValueError(
            "deployment.primaries is only valid in cluster mode; a standalone deployment has one primary "
            "and replicas_per_primary replicas"
        )
    resolved = DEPLOYMENT_DOCUMENT.resolve(document, "deployment")  # a ConfigError names the field's path
    # The filled-in counts first, then the document's own order.
    return {key: resolved[key] for key in dict.fromkeys([*resolved, *document]) if key in resolved}


def _instance_selectors(field_name: str, values: Mapping[str, Any], selectors: set[str], mode: DeploymentMode) -> None:
    for selector in values:
        if selector not in selectors:
            raise ValueError(f"Unknown {field_name} selector '{selector}' for this {mode.value} deployment")


def _path(value: Any, where: str, *, empty: bool = False) -> None:
    if isinstance(value, str) and ("\0" in value or not (value or empty)):
        raise ValueError(f"{where} must be a {'' if empty else 'non-empty '}path without null bytes")


def deployment_config_from_document(document: Mapping[str, Any]) -> DeploymentConfig:
    """Build a fresh configuration from a deployment document.

    This function is pure: it does not resolve binaries, allocate operating
    system resources, create directories, or open sockets. Beyond the
    schema, it checks what spans fields: the ports the instances need, the
    instance selectors, the server config, and how many per-instance
    addresses a loopback block has.
    """
    normalized = normalize_deployment_document(document)
    mode = DeploymentMode(normalized["mode"])
    primaries = normalized.get("primaries", 1)
    replicas_per_primary = normalized["replicas_per_primary"]
    count = instance_count(mode, primaries, replicas_per_primary)
    validate_deployment_base_port(mode, count, normalized.get("base_port", DEFAULT_BASE_PORT))

    _path(normalized.get("base_data_dir"), "base_data_dir")
    _path(normalized.get("valkey_binary"), "valkey_binary", empty=True)
    selectors = {"primary", *(["replica"] if replicas_per_primary else []), *(f"valkey-{i}" for i in range(count))}
    for field_name in ("instance_configs", "instance_binaries", "instance_data_roots"):
        values = normalized.get(field_name, {})
        _instance_selectors(field_name, values, selectors, mode)
        if field_name != "instance_configs":
            for selector, path in values.items():
                _path(path, f"{field_name}.{selector}")

    def server_config(value: Any, where: str) -> dict[str, Any]:
        try:
            return canonicalize_server_config(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Invalid {where}: {exc}") from exc

    if normalized.get("addressing") == "per_instance_loopback" and count > MAX_PER_INSTANCE_LOOPBACK_INSTANCES:
        raise ValueError(f"per_instance_loopback supports at most {MAX_PER_INSTANCE_LOOPBACK_INSTANCES} instances")

    passed = {name: value for name, value in normalized.items() if name not in ("server_config", "instance_configs")}
    return DeploymentConfig(
        **{**passed, "mode": mode, "primaries": primaries},
        server_config=server_config(normalized.get("server_config", {}), "server_config"),
        instance_configs={
            selector: server_config(layer, f"instance_configs.{selector}")
            for selector, layer in normalized.get("instance_configs", {}).items()
        },
    )


def validate_deployment_base_port(mode: DeploymentMode, count: int, base_port: int) -> None:
    """Ensure every client and cluster bus port of ``count`` instances fits in the TCP port range."""
    highest_client_port = base_port + count - 1
    if highest_client_port > MAX_TCP_PORT:
        raise ValueError(
            f"base_port {base_port} with {count} instances would require client port "
            f"{highest_client_port}; maximum is {MAX_TCP_PORT}"
        )
    if mode is DeploymentMode.CLUSTER and highest_client_port + CLUSTER_BUS_PORT_OFFSET > MAX_TCP_PORT:
        raise ValueError(
            f"base_port {base_port} with {count} cluster nodes would require cluster bus port "
            f"{highest_client_port + CLUSTER_BUS_PORT_OFFSET}; maximum is {MAX_TCP_PORT}"
        )
