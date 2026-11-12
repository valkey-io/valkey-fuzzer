"""Field checks every model makes (names and identifiers, numbers, sequences, types, and the document path an
error names, which ``valkey-fuzzer validate`` reports), and the strict loaders that turn mappings into validated
models, so every loader names a bad field the same way.
"""

import math
import re
from collections.abc import Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import MISSING, fields
from typing import Any, Callable, Optional, TypeVar

NAME_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$")
SEGMENT_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]*$")
QUALIFIED_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]*(?:[.:/][A-Za-z0-9][A-Za-z0-9_-]*)+$")


def require_text(value: Any, field_name: str, *, error: type = ValueError) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise error(f"{field_name} must be a non-empty string without surrounding whitespace")
    return str(value)


def document_text(value: Any, path: str, error: type) -> str:
    """:func:`require_text` for the document field at ``path``, which the error names."""

    try:
        return require_text(value, path, error=error)
    except error as exc:
        raise located(exc, path) from None


def require_name(value: Any, field_name: str) -> str:
    """An opaque name: component, event, overlay, deployment mode, or scenario ID."""

    value = require_text(value, field_name)
    if not NAME_PATTERN.fullmatch(value):
        raise ValueError(f"{field_name} contains unsupported characters: {value!r}")
    return value


def require_segment(value: Any, field_name: str) -> str:
    """One path segment: letters, digits, underscores, and hyphens."""

    value = require_text(value, field_name)
    if not SEGMENT_PATTERN.fullmatch(value):
        raise ValueError(
            f"{field_name} must start with a letter or digit and contain only letters, digits, "
            f"underscores, and hyphens: {value!r}"
        )
    return value


def require_qualified_id(value: Any, field_name: str) -> str:
    """A namespaced identifier such as ``example.component``."""

    value = require_text(value, field_name)
    if not QUALIFIED_ID_PATTERN.fullmatch(value):
        raise ValueError(f"{field_name} must be a namespaced identifier such as 'example.component': {value!r}")
    return value


def is_number(value: Any) -> bool:
    """A finite int or float, not a bool."""

    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def require_number(
    value: Any,
    field_name: str,
    *,
    positive: bool = False,
    integer: bool = False,
) -> Any:
    if not is_number(value) or (integer and not isinstance(value, int)):
        raise ValueError(f"{field_name} must be a finite {'integer' if integer else 'number'}")
    if positive and value <= 0:
        raise ValueError(f"{field_name} must be greater than zero")
    return value


def as_tuple(value: Any, field_name: str, *, error: type = TypeError) -> tuple[Any, ...]:
    if isinstance(value, (str, bytes, bytearray, Mapping)) or not isinstance(value, Sequence):
        raise error(f"{field_name} must be an ordered sequence")
    return tuple(value)


def document_sequence(value: Any, path: str, error: type) -> tuple[Any, ...]:
    """:func:`as_tuple` for the document field at ``path``, which the error names."""

    try:
        return as_tuple(value, path, error=error)
    except error as exc:
        raise located(exc, path) from None


def unique_tuple(
    value: Any,
    field_name: str,
    check: Callable[[Any, str], Any] = require_name,
) -> tuple[Any, ...]:
    entries = as_tuple(value, field_name)
    for entry in entries:
        check(entry, f"{field_name} entry")
    if len(set(entries)) != len(entries):
        raise ValueError(f"{field_name} contains duplicates")
    return entries


def require_type(value: Any, expected: type, field_name: str) -> Any:
    if not isinstance(value, expected):
        raise TypeError(f"{field_name} must be a {expected.__name__}")
    return value


def located(error: Exception, path: str) -> Exception:
    """``error`` tagged with the document ``path`` it is about, as its ``path`` attribute.

    Only a document path is a location: a model's own field checks name the
    field in their message, and the loader that built the model tags it.

    A path set closer to the cause is kept, so an error re-raised through
    several layers names the most precise location. ``valkey-fuzzer validate``
    reports it next to the message.
    """

    if getattr(error, "path", None) is None:
        error.path = path
    return error


def carry_path(error: Exception, cause: BaseException) -> Exception:
    """``error`` with the document path of ``cause``, for an error that wraps another."""

    path = getattr(cause, "path", None)
    return error if path is None else located(error, path)


T = TypeVar("T")


def strict_fields(
    value: Any,
    field_name: str,
    *,
    allowed: Optional[Iterable[str]],
    required: Iterable[str] = (),
    error: type = ValueError,
) -> Mapping[str, Any]:
    """Return ``value`` after rejecting non-mappings, non-string keys, and unknown or missing keys.

    ``allowed`` None accepts any key. Every document and model loader checks
    its keys here, so they all name a bad field the same way.
    """

    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        raise located(error(f"{field_name} must be a mapping with string keys"), field_name)
    unknown = [] if allowed is None else sorted(set(value) - set(allowed))
    if unknown:
        raise located(error(f"{field_name} contains unknown fields: {unknown}"), f"{field_name}.{unknown[0]}")
    missing = next((name for name in required if name not in value), None)
    if missing is not None:
        raise located(error(f"{field_name} is missing required field {missing!r}"), f"{field_name}.{missing}")
    return value


@contextmanager
def errors_at(location: str, error: type = ValueError) -> Iterator[None]:
    """Re-raise a ``TypeError`` or ``ValueError`` from the block as ``error`` naming ``location``.

    An ``error`` that already names its location, such as a nested model's,
    is re-raised as it is.
    """

    try:
        yield
    except (TypeError, ValueError) as exc:
        if isinstance(exc, error) and getattr(exc, "path", None) is not None:
            raise
        raise located(carry_path(error(f"{location}: {exc}"), exc), location) from exc


def model_from_data(
    cls: Callable[..., T], value: Any, location: str, *, error: type = ValueError, **convert: Callable[[Any], Any]
) -> T:
    """Dataclass ``cls`` from a mapping of its init fields, checked by :func:`strict_fields`.

    ``convert`` maps a field to the function that builds its value, such as a
    nested model's loader. Whatever the converters or ``cls`` reject is
    ``error`` naming the field or ``location`` (:func:`errors_at`).
    """

    init = [item for item in fields(cls) if item.init]
    required = [item.name for item in init if item.default is MISSING and item.default_factory is MISSING]
    allowed = [item.name for item in init]
    mapping = dict(strict_fields(value, location, allowed=allowed, required=required, error=error))
    for name, converter in convert.items():
        if name in mapping:
            with errors_at(f"{location}.{name}", error):
                mapping[name] = converter(mapping[name])
    with errors_at(location, error):
        return cls(**mapping)


def models_from_data(
    cls: Callable[..., T], value: Any, location: str, *, error: type = ValueError, **convert: Callable[[Any], Any]
) -> tuple[T, ...]:
    """Each item of the sequence ``value`` as ``cls`` (:func:`model_from_data`), at ``<location>[<index>]``."""

    items = document_sequence(value, location, error)
    return tuple(
        model_from_data(cls, item, f"{location}[{index}]", error=error, **convert) for index, item in enumerate(items)
    )
