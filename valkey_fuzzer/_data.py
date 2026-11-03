"""Immutable snapshots of JSON/YAML-safe data, their plain-data and canonical JSON forms, file content digests,
and the plain-data record of an exception.

Every immutable model freezes its mapping and sequence fields here, and every
report and plan file is written from :func:`to_plain_data`.
"""

import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import fields, is_dataclass
from enum import Enum
from types import MappingProxyType
from typing import Any, Optional

from .references import REFERENCE_DATA_KEY, EnvironmentReference, EnvironmentReferenceError


def freeze(value: Any, field_name: str = "value", *, references: bool = False) -> Any:
    """Take a recursive immutable snapshot of JSON/YAML-safe data.

    Mappings become read-only mappings and sequences become tuples. Typed
    environment references are kept only when ``references`` is true.
    """

    if isinstance(value, EnvironmentReference):
        if references:
            return value
        raise TypeError(f"{field_name} must not contain environment references")
    if isinstance(value, Mapping):
        if REFERENCE_DATA_KEY in value:
            raise EnvironmentReferenceError(
                f"{field_name} contains the reserved {REFERENCE_DATA_KEY!r} serialization marker"
            )
        frozen = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError(f"{field_name} mapping keys must be strings")
            frozen[key] = freeze(item, f"{field_name}.{key}", references=references)
        return MappingProxyType(frozen)
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return tuple(freeze(item, f"{field_name}[{index}]", references=references) for index, item in enumerate(value))
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError(f"{field_name} cannot contain NaN or infinite numbers")
    if value is None or isinstance(value, (str, bool, int, float)):
        return value
    raise TypeError(f"{field_name} must contain only JSON/YAML-safe values; got {type(value).__name__}")


def freeze_mapping(value: Any, field_name: str, *, references: bool = False) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{field_name} must be a mapping")
    return freeze(value, field_name, references=references)


def to_plain_data(value: Any) -> Any:
    """Convert models and frozen snapshots to JSON/YAML-safe dicts and lists.

    Dataclass fields whose metadata sets ``serialize=False`` (runtime-only
    handles and services) are omitted. References serialize as the reserved
    ``{"$valkey_fuzzer_ref": path}`` marker.
    """

    if isinstance(value, EnvironmentReference):
        return {REFERENCE_DATA_KEY: value.path}
    if isinstance(value, Enum):
        return value.value
    if is_dataclass(value) and not isinstance(value, type):
        return {
            item.name: to_plain_data(getattr(value, item.name))
            for item in fields(value)
            if item.metadata.get("serialize", True)
        }
    if isinstance(value, Mapping):
        return {key: to_plain_data(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_plain_data(item) for item in value]
    if value is None or isinstance(value, (str, bool, int, float)):
        return value
    raise TypeError(f"cannot convert {type(value).__name__} to plain data")


def jsonable(value: Any) -> Any:
    """Arbitrary reply data as JSON-safe plain data (bytes decoded, sets sorted)."""

    if isinstance(value, Mapping):
        return {str(key): jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        items = [jsonable(item) for item in value]
        if isinstance(value, (set, frozenset)):
            items.sort(key=lambda item: json.dumps(item, sort_keys=True, default=str))
        return items
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, float):
        return value if math.isfinite(value) else str(value)
    if value is None or isinstance(value, (str, bool, int)):
        return value
    return str(value)


def canonical_json(value: Any) -> str:
    """Serialize data in a stable, replay-friendly JSON form."""

    return json.dumps(to_plain_data(value), allow_nan=False, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def file_sha256(path: str) -> Optional[str]:
    """The SHA-256 of a file's contents, or ``None`` when it cannot be read."""

    digest = hashlib.sha256()
    try:
        with open(path, "rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError:
        return None
    return digest.hexdigest()


def exception_type(error: BaseException) -> str:
    """``error``'s class as reports name it: ``ValueError`` for a builtin, else ``module.Class``."""

    error_type = type(error)
    module = error_type.__module__
    return error_type.__qualname__ if module == "builtins" else f"{module}.{error_type.__qualname__}"


# How much of an error's message a report keeps.
_MAX_ERROR_MESSAGE = 2000


def error_record(error: BaseException, phase: Optional[str] = None) -> dict[str, str]:
    """How a report records ``error``: its ``type``, its ``message`` (cut short), and the ``phase`` it ended.

    Every layer that catches a run's error records it this way, so the same
    error has one failure signature whichever layer caught it.
    """

    record = {"type": exception_type(error)}
    message = str(error)
    if message:
        record["message"] = message[:_MAX_ERROR_MESSAGE] + ("..." if len(message) > _MAX_ERROR_MESSAGE else "")
    if phase is not None:
        record["phase"] = phase
    return record
