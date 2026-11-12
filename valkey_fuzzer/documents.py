"""Plain-data documents such as scenarios and experiment fragments: deep merge and dotted paths."""

from collections.abc import Mapping
from typing import Any

# Scenario sections a batch matrix dimension or differential variant may patch.
SCENARIO_SECTIONS = ("experiment", "deployment", "artifacts", "resources", "policies")


def _copied(value: Any) -> Any:
    """``value`` with each mapping and list in it new, so a merge result shares none with its inputs."""

    if isinstance(value, Mapping):
        return {key: _copied(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_copied(item) for item in value]
    return value


def merge(base: Mapping[str, Any], overlay: Mapping[str, Any]) -> dict[str, Any]:
    """A copy of ``base`` with ``overlay`` merged in: mappings merge key by key, anything else is replaced.

    A ``{$ref: ...}`` mapping on either side is one value, replaced whole:
    fields merged into it would make a reference with sibling fields, which
    the compiler rejects.
    """

    merged = _copied(base)
    for key, value in overlay.items():
        current = merged.get(key)
        if (
            isinstance(value, Mapping)
            and isinstance(current, Mapping)
            and "$ref" not in value
            and "$ref" not in current
        ):
            merged[key] = merge(current, value)
        else:
            merged[key] = _copied(value)
    return merged


# What get_path returns for a path the document does not hold; None is a value.
ABSENT = object()


def get_path(document: Any, dotted: str) -> Any:
    """The value at the dotted path in ``document``, or :data:`ABSENT`; ``""`` is the document itself.

    A segment names a mapping key, or, as a number, a list item.
    """

    current = document
    for part in dotted.split(".") if dotted else ():
        if isinstance(current, Mapping) and part in current:
            current = current[part]
        elif isinstance(current, (list, tuple)) and part.isdigit() and int(part) < len(current):
            current = current[int(part)]
        else:
            return ABSENT
    return current


def set_path(document: dict, dotted: str, value: Any) -> None:
    """Set the dotted path in ``document``, creating mappings on the way; ``None`` removes the key."""

    parts = dotted.split(".")
    current = document
    for part in parts[:-1]:
        nxt = current.get(part)
        if not isinstance(nxt, dict):
            nxt = {}
            current[part] = nxt
        current = nxt
    if value is None and parts[-1] in current:
        del current[parts[-1]]
    elif value is not None:
        current[parts[-1]] = value
