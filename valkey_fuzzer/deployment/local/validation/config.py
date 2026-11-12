"""The state validation's configuration: which checks run and how each judges, as ``validation`` sets it, and
:data:`STATE_VALIDATION_SCHEMA`, its YAML form."""

import dataclasses
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Optional

from ....schema import ConfigError, ConfigField, ConfigSchema

# The validation configs below are also the YAML form of ``state_validation``
# (:data:`STATE_VALIDATION_SCHEMA` is built from them): a field's
# ``minimum`` metadata bounds it there.
_AT_LEAST_0 = {"minimum": 0}
_TIMEOUT = {"minimum": 0.001}  # seconds each query of the check may wait


@dataclass
class ReplicationValidationConfig:
    max_acceptable_lag: float = field(default=10.0, metadata=_AT_LEAST_0)  # seconds
    require_all_replicas_synced: bool = False  # Allow dead replicas after chaos
    check_replication_offset: bool = True
    min_replicas_per_shard: int = field(default=1, metadata=_AT_LEAST_0)  # Live replicas per shard (0 = no check)
    timeout: float = field(default=10.0, metadata=_TIMEOUT)


@dataclass
class ClusterStatusValidationConfig:
    acceptable_states: list[str] = field(default_factory=lambda: ["ok"])
    timeout: float = field(default=10.0, metadata=_TIMEOUT)


@dataclass
class SlotCoverageValidationConfig:
    timeout: float = field(default=10.0, metadata=_TIMEOUT)


@dataclass
class ViewConsistencyValidationConfig:
    # A validation whose views disagree is retried, as gossip converges in a round or two; False judges the first.
    allow_transient_inconsistency: bool = True
    timeout: float = field(default=15.0, metadata=_TIMEOUT)


@dataclass
class DataConsistencyValidationConfig:
    num_test_keys: int = field(default=100, metadata={"minimum": 1})  # Number of test keys to write/validate
    key_prefix: str = "fuzzer:test:"  # Prefix for test keys
    timeout: float = field(default=10.0, metadata=_TIMEOUT)


@dataclass
class StateValidationConfig:
    """The state validation of :class:`~.check.DeploymentStateCheck`, as ``validation`` sets it."""

    check_replication: bool = True
    check_cluster_status: bool = True
    check_slot_coverage: bool = True
    check_roles: bool = True
    check_view_consistency: bool = True
    check_data_consistency: bool = True
    check_logs: bool = True

    # Timing configuration
    stabilization_wait: float = field(default=5.0, metadata=_AT_LEAST_0)  # Wait before validation
    validation_timeout: float = field(default=30.0, metadata=_TIMEOUT)  # Total validation timeout

    # Behavior configuration
    blocking_on_failure: bool = False  # Halt execution on failure
    retry_on_transient_failure: bool = True
    max_retries: int = field(default=3, metadata=_AT_LEAST_0)
    retry_delay: float = field(default=2.0, metadata=_AT_LEAST_0)

    # Sub-component configurations
    replication_config: ReplicationValidationConfig = field(default_factory=ReplicationValidationConfig)
    cluster_status_config: ClusterStatusValidationConfig = field(default_factory=ClusterStatusValidationConfig)
    slot_coverage_config: SlotCoverageValidationConfig = field(default_factory=SlotCoverageValidationConfig)
    view_consistency_config: ViewConsistencyValidationConfig = field(default_factory=ViewConsistencyValidationConfig)
    data_consistency_config: DataConsistencyValidationConfig = field(default_factory=DataConsistencyValidationConfig)


def _schema(config_type: type) -> ConfigSchema:
    """The YAML form of a validation config dataclass: every field optional, with no default, since what is
    left out keeps the dataclass default (and a compiled plan records only what was written); a nested config
    may be null."""

    fields = {}
    for item in dataclasses.fields(config_type):
        kind = getattr(item.type, "__origin__", item.type)
        if dataclasses.is_dataclass(kind):
            fields[item.name] = ConfigField(dict, required=False, allow_none=True, schema=_schema(kind))
        else:
            items = ConfigField(item.type.__args__[0]) if kind is list else None
            fields[item.name] = ConfigField(kind, required=False, items=items, **item.metadata)
    return ConfigSchema(fields)


class _StateValidationSchema(ConfigSchema):
    """The YAML form of :class:`StateValidationConfig`, with the one rule across its fields: the stabilization
    wait is shorter than the validation timeout, or it would use the whole budget and the validation would
    always fail. A field left out counts with its default."""

    def resolve(self, config: Mapping[str, Any], location: str) -> dict[str, Any]:
        resolved = super().resolve(config, location)
        wait = resolved.get("stabilization_wait", StateValidationConfig.stabilization_wait)
        timeout = resolved.get("validation_timeout", StateValidationConfig.validation_timeout)
        if wait >= timeout:
            raise ConfigError(
                f"{location}.stabilization_wait ({wait:g}s) must be less than {location}.validation_timeout "
                f"({timeout:g}s): the wait would use the whole budget and the validation would always fail"
            )
        return resolved


STATE_VALIDATION_SCHEMA = _StateValidationSchema(_schema(StateValidationConfig).fields)


def _build(config_type: type, values: Mapping[str, Any]) -> Any:
    nested = {item.name: item.type for item in dataclasses.fields(config_type) if dataclasses.is_dataclass(item.type)}
    return config_type(
        **{
            name: _build(nested[name], value) if name in nested else value
            for name, value in values.items()
            if not (name in nested and value is None)
        }
    )


def validation_config_from_document(document: Optional[Mapping[str, Any]]) -> StateValidationConfig:
    """Build a :class:`StateValidationConfig` from its YAML form (:data:`STATE_VALIDATION_SCHEMA`); a field
    left out keeps its default, and an unknown or invalid one raises :class:`~valkey_fuzzer.schema.ConfigError`."""

    return _build(StateValidationConfig, STATE_VALIDATION_SCHEMA.resolve(document or {}, "validation"))
