"""Scalar predicates on observed values, and payload filters on events."""

from collections.abc import Mapping
from typing import Any, Optional

from valkey_fuzzer.schema import ConfigError, ConfigField, ConfigSchema

from .schemas import optional


def validate_payload_filter(rules: Mapping[str, Any], where: str = "when_payload") -> None:
    for key, rule in rules.items():
        if not isinstance(key, str) or not key:
            raise ConfigError(f"{where} keys must be payload field names")
        if isinstance(rule, Mapping):
            ConfigSchema(PREDICATE_FIELDS).resolve(rule, f"{where}.{key}")
            validate_predicate(rule, f"{where}.{key}")
        elif isinstance(rule, (list, tuple)):
            if not rule or not all(isinstance(item, (str, int, float, bool)) for item in rule):
                raise ConfigError(f"{where}.{key} must be a non-empty list of scalars")
        elif not isinstance(rule, (str, int, float, bool)):
            raise ConfigError(f"{where}.{key} must be a scalar, a list of scalars, or a predicate")


def payload_matches(payload: Mapping[str, Any], rules: Mapping[str, Any]) -> bool:
    """Match an event payload: scalars compare as strings, lists are any-of, mappings are predicates."""

    for key, rule in rules.items():
        actual = payload.get(key)
        if isinstance(rule, Mapping):
            if not compare(actual, rule):
                return False
        elif actual is None:
            return False
        elif isinstance(rule, (list, tuple)):
            if str(actual) not in {str(item) for item in rule}:
                return False
        elif str(actual) != str(rule):
            return False
    return True


def compare(actual: Any, rule: Mapping[str, Any]) -> bool:
    """Evaluate one scalar predicate against an observed value."""

    if "exists" in rule:
        return (actual is not None) is bool(rule["exists"])
    if actual is None:
        return False
    if "equals" in rule:
        return str(actual) == str(rule["equals"])
    if "not_equals" in rule:
        return str(actual) != str(rule["not_equals"])
    if "in" in rule:
        return str(actual) in {str(item) for item in rule["in"]}
    if "contains" in rule:
        return str(rule["contains"]) in str(actual)
    numeric = _as_number(actual)
    if numeric is None:
        return False
    if "min" in rule and numeric < float(rule["min"]):
        return False
    if "max" in rule and numeric > float(rule["max"]):
        return False
    return "min" in rule or "max" in rule


# A scalar predicate's fields (:func:`compare`): one of exists, equals, not_equals, in, and contains, or min,
# max, or both (:func:`validate_predicate`).
_SCALAR = (str, int, float, bool)


PREDICATE_FIELDS = {
    "exists": optional(bool),
    "equals": optional(_SCALAR),
    "not_equals": optional(_SCALAR),
    "in": optional(list, items=ConfigField(_SCALAR)),
    "contains": optional(_SCALAR),
    "min": optional(float),
    "max": optional(float),
}


def validate_predicate(rule: Mapping[str, Any], where: str) -> None:
    present = [key for key in PREDICATE_FIELDS if key in rule]
    if not present:
        raise ConfigError(f"{where} needs one predicate of {list(PREDICATE_FIELDS)}")
    if len([key for key in present if key not in ("min", "max")]) > 1:
        raise ConfigError(f"{where} may combine only min and max")


def _as_number(value: Any) -> Optional[float]:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return None
