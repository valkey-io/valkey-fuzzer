"""Value schemas for config fields and for the events and evidence components declare."""

from collections.abc import Mapping
from typing import Any, Optional

from valkey_fuzzer.declarations import EvidenceDeclaration
from valkey_fuzzer.schema import ConfigField, ConfigSchema

# Value schemas for declared events and evidence (see :func:`mapping_of`).
TEXT = ConfigField(str)


INTEGER = ConfigField(int)


NUMBER = ConfigField(float)


FLAG = ConfigField(bool)


MAPPING = ConfigField(dict)


LIST = ConfigField(list)


ANY = ConfigField(object)


def optional(types: Any, **kwargs: Any) -> ConfigField:
    """A field a record or payload may leave out."""

    return ConfigField(types, required=False, **kwargs)


def nullable(types: Any) -> ConfigField:
    """A required field that may be null."""

    return ConfigField(types, allow_none=True)


def mapping_of(
    fields: Optional[Mapping[str, ConfigField]] = None, *, additional: Any = False, **kwargs: Any
) -> ConfigField:
    """A mapping value with ``fields``; ``additional`` is :class:`ConfigSchema`'s, for keys that vary."""

    return ConfigField(dict, schema=ConfigSchema(fields or {}, additional=additional), **kwargs)


def declared(value: ConfigField, *, many: bool = False) -> EvidenceDeclaration:
    return EvidenceDeclaration(value, many=many)
