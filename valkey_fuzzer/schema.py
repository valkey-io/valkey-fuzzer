"""Strict documents and config schemas.

A :class:`ConfigSchema` of :class:`ConfigField` rules checks one mapping and
returns it with its defaults filled in. It is the one validator for a
component's config, an artifact or resource provider's config, an
experiment's ``inputs:``, the field rules in an experiment's ``deployment:``
block (the last two read from their YAML form by
:meth:`ConfigSchema.from_document` and :meth:`ConfigSchema.from_rules`), and
the fuzzer's own documents: batch specs, search profiles, zoo cases, and
differentials.
The YAML loaders here reject duplicate keys, so every document is parsed the
same way.
"""

import math
import re
from collections.abc import Mapping, Sequence
from enum import Enum
from types import MappingProxyType
from typing import Any, Optional, TextIO, Union

import yaml

from ._data import to_plain_data
from .fields import is_number, located, strict_fields
from .references import EnvironmentKind, EnvironmentReference, collect_environment_references

INPUT_SEGMENT = r"[A-Za-z0-9][A-Za-z0-9_-]*"
ENVIRONMENT_NAMESPACES = frozenset(kind.value for kind in EnvironmentKind)


class ExperimentError(ValueError):
    """Base class for catalog, experiment document, and compilation failures."""


class ScenarioCompileError(ExperimentError):
    """The scenario cannot be compiled safely."""


class ExperimentDocumentError(ScenarioCompileError):
    """A data-only experiment, fragment, or scenario document is malformed."""


def document_mapping(value: Any, location: str) -> Mapping[str, Any]:
    """``value`` if it is a mapping with string keys, else :class:`ExperimentDocumentError` naming ``location``."""

    return strict_fields(value, location, allowed=None, error=ExperimentDocumentError)


class _UniqueKeyLoader(yaml.SafeLoader):
    """A safe YAML loader with YAML 1.2 core scalars that rejects duplicate mapping keys.

    PyYAML reads YAML 1.1, where ``yes``/``no``/``on``/``off`` are booleans,
    ``1:30`` is 90, ``1_000`` is 1000, ``017400`` is octal, and a date is a
    timestamp: a reject rule or an ``equals`` written ``yes`` then never
    matches the string, and a typo becomes a number. Here only ``true`` and
    ``false`` are booleans, numbers are plain decimals (``0o`` and ``0x``
    allowed), and everything else is a string, except a leading-zero
    integer, which YAML 1.1 and 1.2 read differently and is refused.
    """


_LEADING_ZERO_TAG = "tag:valkey-fuzzer,2026:leading-zero-integer"
_UniqueKeyLoader.yaml_implicit_resolvers = {
    first: [(tag, regexp) for tag, regexp in resolvers if tag in ("tag:yaml.org,2002:null", "tag:yaml.org,2002:merge")]
    for first, resolvers in yaml.SafeLoader.yaml_implicit_resolvers.items()
}
for _tag, _pattern, _first in (
    ("tag:yaml.org,2002:bool", r"^(?:true|True|TRUE|false|False|FALSE)$", "tTfF"),
    ("tag:yaml.org,2002:int", r"^(?:[-+]?(?:0|[1-9][0-9]*)|0o[0-7]+|0x[0-9a-fA-F]+)$", "-+0123456789"),
    (_LEADING_ZERO_TAG, r"^[-+]?0[0-9]+$", "-+0"),
    (
        "tag:yaml.org,2002:float",
        r"^(?:[-+]?(?:\.[0-9]+|[0-9]+(?:\.[0-9]*)?)(?:[eE][-+]?[0-9]+)?|[-+]?\.(?:inf|Inf|INF)|\.(?:nan|NaN|NAN))$",
        "-+.0123456789",
    ),
):
    _UniqueKeyLoader.add_implicit_resolver(_tag, re.compile(_pattern), list(_first))


def _refuse_leading_zero(loader: _UniqueKeyLoader, node: yaml.ScalarNode) -> None:
    raise yaml.constructor.ConstructorError(
        None,
        None,
        f"{node.value} has a leading zero, which YAML 1.1 reads as octal: write "
        f"{int(node.value, 10)} for the decimal, 0o{node.value.lstrip('+-0')} for octal, or quote it for a string",
        node.start_mark,
    )


_UniqueKeyLoader.add_constructor(_LEADING_ZERO_TAG, _refuse_leading_zero)


def _construct_unique_mapping(loader: _UniqueKeyLoader, node: yaml.MappingNode, deep: bool = False) -> dict:
    loader.flatten_mapping(node)
    mapping = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        try:
            duplicate = key in mapping
        except TypeError as exc:
            raise yaml.constructor.ConstructorError(
                "while constructing a mapping", node.start_mark, "found an unhashable key", key_node.start_mark
            ) from exc
        if duplicate:
            raise yaml.constructor.ConstructorError(
                "while constructing a mapping", node.start_mark, f"found duplicate key {key!r}", key_node.start_mark
            )
        mapping[key] = loader.construct_object(value_node, deep=deep)
    return mapping


_UniqueKeyLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _construct_unique_mapping)


def load_yaml(source: Union[str, bytes, TextIO], source_name: str) -> Any:
    if hasattr(source, "read"):
        source = source.read()
    if not isinstance(source, (str, bytes)):
        raise TypeError("source must be YAML text, bytes, or a text stream")
    try:
        return yaml.load(source, Loader=_UniqueKeyLoader)
    except yaml.YAMLError as exc:
        raise ExperimentDocumentError(f"{source_name}: {exc}") from exc


def load_yaml_mapping(source: Union[str, bytes, TextIO], *, source_name: str = "<yaml>") -> Mapping[str, Any]:
    """Load one duplicate-key-free YAML mapping, such as a scenario or an experiment fragment."""

    value = load_yaml(source, source_name)
    if not isinstance(value, Mapping):
        raise ExperimentDocumentError(f"{source_name}: document must be a mapping")
    return value


# ---------------------------------------------------------------------------
# Config schemas
# ---------------------------------------------------------------------------


class ConfigError(ValueError):
    """A config, input, or deployment document does not match its schema."""


_MISSING = object()

# How messages name the types a field accepts.
_TYPE_NAMES = {
    bool: "a boolean",
    int: "an integer",
    float: "a number",
    str: "a string",
    list: "a list",
    dict: "a mapping",
    type(None): "null",
    EnvironmentReference: "an environment reference",
}


def plain_config(value: Any) -> Any:
    """A frozen config or a scenario as plain dicts and lists; environment references are kept."""

    if isinstance(value, (EnvironmentReference, str, bytes)):
        return value
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Mapping):
        return {key: plain_config(item) for key, item in value.items()}
    if isinstance(value, Sequence):
        return [plain_config(item) for item in value]
    return value


def _json_equal(left: Any, right: Any) -> bool:
    """Equality in the JSON data model (``True`` is not ``1``)."""

    if isinstance(left, bool) or isinstance(right, bool) or left is None or right is None:
        return type(left) is type(right) and left == right
    if is_number(left) and is_number(right):
        return left == right
    if isinstance(left, Mapping) and isinstance(right, Mapping):
        return set(left) == set(right) and all(_json_equal(left[key], right[key]) for key in left)
    if isinstance(left, (list, tuple)) and isinstance(right, (list, tuple)):
        return len(left) == len(right) and all(_json_equal(a, b) for a, b in zip(left, right))
    return isinstance(left, str) and isinstance(right, str) and left == right


def _type_name(value: Any) -> str:
    return "null" if value is None else type(value).__name__


class ConfigField:
    """One field of a :class:`ConfigSchema`: the types it accepts, its default, and its constraints.

    A field without a default is required unless ``required=False``, in which
    case it stays absent when it is not given. ``types`` is a type or a tuple
    of types: ``float`` also accepts integers, ``list`` tuples, ``dict`` any
    mapping, and ``object`` any value; ``allow_none`` also accepts ``None``.
    ``choices`` compare as JSON values (``True`` is not ``1``), and
    ``minimum`` and ``maximum`` bound a number and reject anything else.

    Only a field with ``reference=True`` may be, or contain, an
    ``artifacts``/``resources`` reference, which compile-time validation
    accepts unresolved (the executable plan then holds the resolved value);
    ``reference_kinds`` limits the kinds it may name, and the type
    ``EnvironmentReference`` accepts nothing but a reference. ``schema``
    resolves a mapping value through a nested schema, and ``items`` checks
    each item of a list value. ``variants`` is ``(tag, {value: schema})``, a
    tagged union of mappings: the value of the field ``tag`` picks the schema
    the mapping resolves through (each schema declares ``tag`` itself). A
    default must itself be valid. ``description`` is shown in the catalog
    (:meth:`json_schema`).

    Every :class:`ConfigError` it raises carries the location of the value
    it rejects as its ``path``.
    """

    def __init__(
        self,
        types: Any,
        *,
        default: Any = _MISSING,
        required: bool = True,
        choices: Optional[Sequence[Any]] = None,
        minimum: Optional[float] = None,
        maximum: Optional[float] = None,
        allow_none: bool = False,
        reference: bool = False,
        reference_kinds: Optional[Sequence[str]] = None,
        schema: Optional["ConfigSchema"] = None,
        items: Optional["ConfigField"] = None,
        description: Optional[str] = None,
        variants: Optional[tuple[str, Mapping[str, "ConfigSchema"]]] = None,
    ):
        self.types = types if isinstance(types, tuple) else (types,)
        if not self.types or not all(isinstance(kind, type) for kind in self.types):
            raise TypeError("types must be a type or a tuple of types")
        self.default = default
        self.required = required and default is _MISSING
        self.choices = None if choices is None else tuple(choices)
        if self.choices is not None and (
            not self.choices
            or any(_json_equal(a, b) for index, a in enumerate(self.choices) for b in self.choices[:index])
        ):
            raise ValueError("allowed values must be a non-empty list without duplicates")
        for name, bound in (("minimum", minimum), ("maximum", maximum)):
            if bound is not None and not is_number(bound):
                raise ValueError(f"{name} must be a finite number")
        if minimum is not None and maximum is not None and minimum > maximum:
            raise ValueError("minimum is greater than maximum")
        self.minimum = minimum
        self.maximum = maximum
        self.allow_none = allow_none
        self.reference = reference or EnvironmentReference in self.types
        self.reference_kinds = None if reference_kinds is None else tuple(reference_kinds)
        if self.reference_kinds is not None:
            if not self.reference:
                raise ValueError("reference_kinds needs a field that accepts environment references")
            if not self.reference_kinds or len(set(self.reference_kinds)) != len(self.reference_kinds):
                raise ValueError("reference_kinds must be non-empty and distinct")
            if not set(self.reference_kinds) <= ENVIRONMENT_NAMESPACES:
                raise ValueError("reference_kinds must be 'artifacts' or 'resources'")
        if schema is not None and (not isinstance(schema, ConfigSchema) or dict not in self.types):
            raise TypeError("schema must be a ConfigSchema for a dict field")
        if items is not None and (not isinstance(items, ConfigField) or list not in self.types):
            raise TypeError("items must be a ConfigField for a list field")
        if variants is not None:
            tag, schemas = variants
            if schema is not None or dict not in self.types or not schemas:
                raise TypeError("variants need a dict field without a schema, and at least one variant")
            if not all(isinstance(item, ConfigSchema) and tag in item.fields for item in schemas.values()):
                raise TypeError(f"every variant must be a ConfigSchema that declares {tag!r}")
            variants = (tag, MappingProxyType(dict(schemas)))
        self.schema = schema
        self.items = items
        if description is not None and not isinstance(description, str):
            raise TypeError("description must be a string")
        self.description = description
        self.variants = variants
        if default is not _MISSING:
            self.resolve(self.default_value(), "default")

    def default_value(self) -> Any:
        """The default; a callable default is called for a fresh value."""

        return self.default() if callable(self.default) else self.default

    def _describe(self) -> str:
        names = [_TYPE_NAMES.get(kind, kind.__name__) for kind in self.types]
        if self.allow_none:
            names.append("null")
        return " or ".join(names)

    def resolve(self, value: Any, where: str) -> Any:
        """``value`` checked against this field, with the defaults of nested schemas filled in."""

        try:
            return self._resolve(value, where)
        except ConfigError as exc:
            located(exc, where)
            raise

    def _resolve(self, value: Any, where: str) -> Any:
        if value is None and self.allow_none:
            return None
        references = collect_environment_references(value)
        if references and not self.reference:
            raise ConfigError(f"{where} cannot contain environment references")
        for reference in references:
            if self.reference_kinds is not None and reference.kind.value not in self.reference_kinds:
                raise ConfigError(
                    f"{where} references kind {reference.kind.value!r}; allowed kinds are {list(self.reference_kinds)}"
                )
        if isinstance(value, EnvironmentReference):
            return value
        if object not in self.types:
            accepted = self.types
            if float in accepted:
                accepted = (*accepted, int)
            if list in accepted:
                accepted = (*accepted, tuple)
            if dict in accepted:
                accepted = (*accepted, Mapping)
            if not isinstance(value, accepted) or (isinstance(value, bool) and bool not in self.types):
                raise ConfigError(f"{where} must be {self._describe()}, got {_type_name(value)}")
        if isinstance(value, float) and not math.isfinite(value):
            raise ConfigError(f"{where} must be finite")
        if self.choices is not None and not any(_json_equal(value, choice) for choice in self.choices):
            raise ConfigError(f"{where} must be one of {list(self.choices)}, got {value!r}")
        if self.minimum is not None or self.maximum is not None:
            if not is_number(value):
                raise ConfigError(f"{where} must be a number, got {_type_name(value)}")
            if self.minimum is not None and value < self.minimum:
                raise ConfigError(f"{where} must be at least {self.minimum}, got {value!r}")
            if self.maximum is not None and value > self.maximum:
                raise ConfigError(f"{where} must be at most {self.maximum}, got {value!r}")
        if self.schema is not None:
            return self.schema.resolve(value, where)
        if self.variants is not None:
            tag, schemas = self.variants
            kind = plain_config(value).get(tag) if isinstance(value, Mapping) else None
            if kind not in schemas:
                raise ConfigError(f"{where}.{tag} must be one of {list(schemas)}, got {kind!r}")
            return schemas[kind].resolve(value, where)
        if self.items is not None:
            return [self.items.resolve(item, f"{where}[{index}]") for index, item in enumerate(value)]
        return value

    def json_schema(self) -> dict[str, Any]:
        """This field as a JSON Schema: types, enum, bounds, default, nested fields, and variants (``oneOf``).

        A field that also accepts an environment reference lists the type
        ``environment_reference`` (with ``reference_kinds`` when limited), as
        an experiment's ``inputs:`` document writes it; a field of any type has
        no ``type``. Whether it is required is its schema's ``required`` list.
        """

        names = [] if object in self.types else [_JSON_TYPES.get(kind, kind.__name__) for kind in self.types]
        if names and self.reference and _REFERENCE_TYPE not in names:
            names.append(_REFERENCE_TYPE)
        if names and self.allow_none and "null" not in names:
            names.append("null")
        schema: dict[str, Any] = {}
        if names:
            schema["type"] = names[0] if len(names) == 1 else names
        if self.reference_kinds is not None:
            schema["reference_kinds"] = list(self.reference_kinds)
        if self.choices is not None:
            schema["enum"] = to_plain_data(list(self.choices))
        if self.minimum is not None:
            schema["minimum"] = self.minimum
        if self.maximum is not None:
            schema["maximum"] = self.maximum
        if self.default is not _MISSING:
            schema["default"] = to_plain_data(self.default_value())
        if self.description is not None:
            schema["description"] = self.description
        if self.schema is not None:
            nested = self.schema.json_schema()
            schema.update((key, value) for key, value in nested.items() if key != "type")
        if self.variants is not None:
            # Each variant's tag field is an enum of its one value, which picks it.
            schema["oneOf"] = [variant.json_schema() for variant in self.variants[1].values()]
        if self.items is not None:
            schema["items"] = self.items.json_schema()
        return schema


class ConfigSchema:
    """The fields of one config mapping; :meth:`resolve` checks a mapping and fills in its defaults.

    A field missing from ``fields`` is rejected unless ``additional`` allows
    it: ``True`` keeps any value as written, and a :class:`ConfigField`
    checks each undeclared field.
    """

    def __init__(
        self,
        fields: Optional[Mapping[str, ConfigField]] = None,
        *,
        additional: Union[bool, ConfigField] = False,
    ):
        fields = dict(fields or {})
        for name, rule in fields.items():
            if not isinstance(name, str) or not name:
                raise TypeError("config field names must be non-empty strings")
            if not isinstance(rule, ConfigField):
                raise TypeError(f"config field {name!r} must be a ConfigField")
        if not isinstance(additional, (bool, ConfigField)):
            raise TypeError("additional must be a boolean or a ConfigField")
        self._fields = MappingProxyType(fields)
        self.additional = additional

    @property
    def fields(self) -> Mapping[str, ConfigField]:
        return self._fields

    def resolve(self, config: Mapping[str, Any], location: str) -> dict[str, Any]:
        """``config`` as plain data with every declared default filled in.

        Raises :class:`ConfigError` naming ``location`` and the field. Reference
        fields may hold unresolved references, so one schema checks both the
        compiled config and the executable one.
        """

        allowed = self._fields if self.additional is False else None
        config = strict_fields(plain_config(config), location, allowed=allowed, error=ConfigError)
        resolved = {}
        # In declared order, so the first field a config gets wrong, missing or not, names the error.
        for name, rule in self._fields.items():
            if name in config:
                resolved[name] = rule.resolve(config[name], f"{location}.{name}")
            elif rule.default is not _MISSING:
                resolved[name] = rule.resolve(rule.default_value(), f"{location}.{name}")
            elif rule.required:
                raise located(ConfigError(f"{location} is missing required field {name!r}"), f"{location}.{name}")
        for name in (name for name in config if name not in self._fields):
            value = config[name]
            resolved[name] = value if self.additional is True else self.additional.resolve(value, f"{location}.{name}")
        return resolved

    def json_schema(self) -> dict[str, Any]:
        """The fields as a JSON Schema object: ``properties`` by name, sorted ``required``, and
        ``additionalProperties`` (``false``, ``true`` for any value, or the schema undeclared fields must meet)."""

        additional = self.additional
        return {
            "type": "object",
            "properties": {name: rule.json_schema() for name, rule in sorted(self._fields.items())},
            "required": sorted(name for name, rule in self._fields.items() if rule.required),
            "additionalProperties": additional.json_schema() if isinstance(additional, ConfigField) else additional,
        }

    @classmethod
    def from_document(cls, document: Mapping[str, Any], location: str = "inputs") -> "ConfigSchema":
        """The schema an experiment's ``inputs:`` document declares.

        The document is ``{type: object, properties: {<name>: <rule>},
        required: [...], additionalProperties: <bool>}``. A rule holds
        ``type`` (a JSON type or ``environment_reference``), ``enum``,
        ``minimum``, ``maximum``, ``default``, ``reference_kinds``, and the
        annotations ``title`` and ``description``; an input without a
        ``type``, or an undeclared one, may be any value without environment
        references. An empty document accepts any such inputs.
        """

        document = strict_fields(document, location, allowed=_DOCUMENT_FIELDS, error=ConfigError)
        if not document:
            return cls(additional=_ANY_VALUE)
        if document.get("type", "object") != "object":
            raise ConfigError(f"{location}.type must be 'object'")
        properties = document.get("properties", {})
        if not isinstance(properties, Mapping):
            raise ConfigError(f"{location}.properties must be a mapping")
        required = document.get("required", ())
        if (
            isinstance(required, (str, bytes, Mapping))
            or not isinstance(required, Sequence)
            or not all(isinstance(name, str) for name in required)
            or len(set(required)) != len(required)
            or not set(required) <= set(properties)
        ):
            raise ConfigError(f"{location}.required must list distinct declared properties")
        additional = document.get("additionalProperties", False)
        if not isinstance(additional, bool):
            raise ConfigError(f"{location}.additionalProperties must be a boolean")
        fields = {
            name: _rule_field(name, rule, f"{location}.properties.{name}", _RULE_FIELDS, required=name in required)
            for name, rule in properties.items()
        }
        return cls(fields, additional=_ANY_VALUE if additional else False)

    @classmethod
    def from_rules(cls, rules: Mapping[str, Any], location: str = "deployment") -> "ConfigSchema":
        """The schema of the field rules in an experiment's ``deployment:`` block.

        ``rules`` maps deployment fields to rules such as ``{minimum: 1}``,
        holding ``type``, ``enum``, ``minimum``, ``maximum``, and
        ``description``. Every field named is required; the deployment's other
        fields are its provider's to check.
        """

        if not isinstance(rules, Mapping):
            raise ConfigError(f"{location} must be a mapping")
        fields = {}
        for name, rule in rules.items():
            if not isinstance(rule, Mapping):
                raise ConfigError(
                    f"{location}.{name} must be a rule such as {{minimum: 1}} (type, enum, minimum, maximum); "
                    "an experiment's deployment holds its modes and rules for deployment fields"
                )
            fields[name] = _rule_field(name, rule, f"{location}.{name}", _DEPLOYMENT_RULE_FIELDS, required=True)
        return cls(fields, additional=True)


# Any value without environment references: an input without a type, or an undeclared one.
_ANY_VALUE = ConfigField(object, required=False)

_DOCUMENT_FIELDS = ("$schema", "additionalProperties", "description", "properties", "required", "title", "type")
_RULE_FIELDS = ("default", "description", "enum", "maximum", "minimum", "reference_kinds", "title", "type")
_DEPLOYMENT_RULE_FIELDS = ("description", "enum", "maximum", "minimum", "type")
_REFERENCE_TYPE = "environment_reference"
_JSON_TYPES = {
    bool: "boolean",
    int: "integer",
    float: "number",
    str: "string",
    list: "array",
    dict: "object",
    type(None): "null",
    EnvironmentReference: _REFERENCE_TYPE,
}
_DOCUMENT_TYPES = {
    "array": list,
    "boolean": bool,
    "integer": int,
    "null": type(None),
    "number": float,
    "object": dict,
    "string": str,
    _REFERENCE_TYPE: EnvironmentReference,
}


def _rule_field(name: Any, rule: Any, where: str, allowed: Sequence[str], *, required: bool) -> ConfigField:
    """The :class:`ConfigField` one YAML rule declares."""

    if not isinstance(name, str) or not re.fullmatch(INPUT_SEGMENT, name):
        raise ConfigError(
            f"{where}: a field name must be one path segment of letters, digits, underscores, or hyphens: {name!r}"
        )
    rule = strict_fields(rule, where, allowed=allowed, error=ConfigError)
    type_name = rule.get("type")
    # A list of types takes a value of any of them, e.g. [string, "null"].
    type_names = list(type_name) if isinstance(type_name, (list, tuple)) else [type_name]
    if "type" in rule and (
        not type_names
        or len(set(map(str, type_names))) != len(type_names)
        or not all(isinstance(name, str) and name in _DOCUMENT_TYPES for name in type_names)
        or (len(type_names) > 1 and _REFERENCE_TYPE in type_names)
        or type_names == ["null"]
    ):
        raise ConfigError(f"{where} has unsupported schema type {type_name!r}")
    nullable = len(type_names) > 1 and "null" in type_names
    if nullable:
        type_names.remove("null")
    if len(type_names) == 1:
        type_name = type_names[0]
    if type_name == _REFERENCE_TYPE:
        unsupported = sorted(set(rule) & {"default", "enum", "maximum", "minimum"})
        if unsupported:
            raise ConfigError(f"{where} with type {_REFERENCE_TYPE!r} does not support {unsupported}")
    for key in ("enum", "reference_kinds"):
        if key in rule and (isinstance(rule[key], (str, bytes, Mapping)) or not isinstance(rule[key], Sequence)):
            raise ConfigError(f"{where}.{key} must be a sequence")
    try:
        return ConfigField(
            tuple(_DOCUMENT_TYPES[name] for name in type_names)
            if len(type_names) > 1
            else _DOCUMENT_TYPES.get(type_name, object),
            allow_none=nullable,
            default=rule.get("default", _MISSING),
            required=required,
            choices=rule.get("enum"),
            minimum=rule.get("minimum"),
            maximum=rule.get("maximum"),
            reference_kinds=rule.get("reference_kinds"),
            description=rule.get("description"),
        )
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"{where}: {exc}") from None
