"""The reply schemas Valkey declares for its commands, and whether a reply matches one.

Every command's spec in Valkey's ``src/commands/*.json`` has a ``reply_schema``: a JSON Schema of its replies
that are not errors. Valkey holds its own test suite to them (``utils/req-res-log-validator.py``), on a server
built to log every request and reply and over RESP3 only. An ordinary build does not report them (``COMMAND
DOCS`` lists ``reply_schema`` only when built with ``LOG_REQ_RES``), so they ship here, in
``reply_schemas.json``, which names the Valkey commit they were read from and is written by::

    python -m valkey_fuzzer.components.reply_schemas <valkey source tree> "<the commit of the tree>"

A schema describes the RESP3 reply. Over RESP2 the server sends the same reply in the forms RESP2 has, and
:func:`mismatch` takes each of them for what the schema describes (see :func:`_from_resp2`).
"""

import base64
import functools
import json
import re
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Optional

from valkey_fuzzer._data import canonical_json
from valkey_fuzzer.schema import BASE64_KEY

_PATH = Path(__file__).with_name("reply_schemas.json")
# The JSON Schema keywords :func:`mismatch` judges (``description`` says nothing about a reply). A test holds
# the shipped schemas to them, so a schema written with another is not taken as checked.
KEYWORDS = frozenset(
    {
        "$ref",
        "additionalItems",
        "additionalProperties",
        "anyOf",
        "const",
        "description",
        "items",
        "maxItems",
        "maximum",
        "minItems",
        "minimum",
        "oneOf",
        "pattern",
        "patternProperties",
        "properties",
        "type",
        "uniqueItems",
    }
)
_TYPES = {
    "string": lambda value: isinstance(value, str),
    "integer": lambda value: isinstance(value, int) and not isinstance(value, bool),
    "number": lambda value: isinstance(value, (int, float)) and not isinstance(value, bool),
    "null": lambda value: value is None,
    "array": lambda value: isinstance(value, list),
    "object": lambda value: isinstance(value, dict),
}
# A double that is no finite number, as a recorded reply holds it (see raw_reply).
_NOT_FINITE = ("inf", "-inf", "nan")
_SHOWN = 120
_PROBLEM = 600


def read_specs(commands: Path) -> dict[str, Any]:
    """``{name: reply schema}`` of the command specs in the directory ``commands`` (Valkey's ``src/commands``).
    A name is as the command table has it: upper case, a subcommand's after its container's (``CONFIG GET``)."""

    schemas = {}
    for path in sorted(commands.glob("*.json")):
        for name, spec in json.loads(path.read_text(encoding="utf-8")).items():
            if "reply_schema" in spec:
                schemas[f"{spec.get('container', '')} {name}".strip().upper()] = spec["reply_schema"]
    return schemas


def dumped(valkey: str, schemas: Mapping[str, Any]) -> str:
    """The text of ``reply_schemas.json``: ``valkey`` (the commit the schemas come from) and one command a line."""

    lines = [f"{json.dumps(name)}: {json.dumps(schemas[name], sort_keys=True)}" for name in sorted(schemas)]
    return "\n".join(("{", f'"valkey": {json.dumps(valkey)},', '"schemas": {', ",\n".join(lines), "}", "}", ""))


@functools.cache
def reply_schemas() -> Mapping[str, Any]:
    """``{command name: reply schema}``, as shipped."""

    return json.loads(_PATH.read_text(encoding="utf-8"))["schemas"]


def mismatch(schema: Mapping[str, Any], reply: Any, resp: int) -> Optional[str]:
    """Where and why ``reply``, as :func:`~valkey_fuzzer.components.actions.raw_reply` records what a RESP
    ``resp`` connection read, does not match ``schema``; None when it matches."""

    problem = _mismatch(schema, reply, resp, schema, "reply")
    return problem if problem is None or len(problem) <= _PROBLEM else problem[: _PROBLEM - 3] + "..."


def _shown(value: Any) -> str:
    text = canonical_json(value)
    return text if len(text) <= _SHOWN else text[: _SHOWN - 3] + "..."


def _from_resp2(schema: Mapping[str, Any], value: Any) -> Any:
    """``value``, read over RESP2, as RESP3 sends what ``schema`` describes, when it is RESP2's form of that:

    - a double as a string (``addReplyDouble``);
    - a map as a flat array of its keys and values (``addReplyMapLen``), or as an array of ``[key, value]`` pairs
      (XREAD and XREADGROUP);
    - an array of n-item arrays as one flat array (the ``WITHSCORES`` and ``WITHVALUES`` replies).

    Anything else is returned as it is, for the schema to judge.
    """

    kind = schema.get("type")
    if kind == "number" and isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return value
    if not isinstance(value, list):
        return value
    if kind == "object":
        if all(isinstance(item, list) and len(item) == 2 for item in value):
            pairs = value
        elif len(value) % 2 == 0:
            pairs = [value[index : index + 2] for index in range(0, len(value), 2)]
        else:
            return value
        # A key may be an integer (a LATENCY HISTOGRAM bucket): a recorded RESP3 map has it as text.
        if all(isinstance(key, (str, int)) for key, _ in pairs):
            return {str(key): item for key, item in pairs}
        return value
    items = schema.get("items")
    if kind == "array" and isinstance(items, Mapping) and items.get("type") == "array":
        size = items.get("minItems")
        flat = not any(isinstance(item, list) for item in value)
        if size and size == items.get("maxItems") and flat and len(value) % size == 0:
            return [value[index : index + size] for index in range(0, len(value), size)]
    return value


def _mismatch(schema: Mapping[str, Any], value: Any, resp: int, root: Mapping[str, Any], where: str) -> Optional[str]:
    if schema.get("$ref") == "#":
        schema = root
    if isinstance(value, dict) and set(value) == {BASE64_KEY}:
        # A bulk string that is not UTF-8.
        value = base64.b64decode(value[BASE64_KEY]).decode("latin-1")
    kind = schema.get("type")
    if resp == 2:
        value = _from_resp2(schema, value)
    elif kind == "number" and value in _NOT_FINITE:
        value = float(value)
    if kind is not None and not _TYPES[kind](value):
        return f"{where} is not of type {kind}: {_shown(value)}"
    if "const" in schema and (type(value) is not type(schema["const"]) or value != schema["const"]):
        return f"{where} is not {_shown(schema['const'])}: {_shown(value)}"
    for keyword in ("oneOf", "anyOf"):
        if keyword not in schema:
            continue
        problems = [_mismatch(option, value, resp, root, where) for option in schema[keyword]]
        matched = problems.count(None)
        if not matched:
            return f"{where} matches none of the {len(problems)} alternatives ({'; '.join(problems)})"
        # A RESP2 reply may match the alternative written for RESP2 and, by its RESP2 form, the one for RESP3.
        if matched > 1 and keyword == "oneOf" and resp == 3:
            return f"{where} matches {matched} alternatives of a oneOf: {_shown(value)}"
    if isinstance(value, str):
        if "pattern" in schema and not re.search(schema["pattern"], value):
            return f"{where} does not match the pattern {schema['pattern']}: {_shown(value)}"
    elif isinstance(value, list):
        return _array_mismatch(schema, value, resp, root, where)
    elif isinstance(value, dict):
        return _object_mismatch(schema, value, resp, root, where)
    elif _TYPES["number"](value):
        if value < schema.get("minimum", value) or value > schema.get("maximum", value):
            return f"{where} is outside {schema.get('minimum', '')}..{schema.get('maximum', '')}: {_shown(value)}"
    return None


def _array_mismatch(schema: Mapping[str, Any], value: list, resp: int, root: Mapping[str, Any], where: str) -> Any:
    if len(value) < schema.get("minItems", 0) or len(value) > schema.get("maxItems", len(value)):
        return f"{where} has {len(value)} items: {_shown(value)}"
    if schema.get("uniqueItems") and len({canonical_json(item) for item in value}) < len(value):
        return f"{where} repeats an item: {_shown(value)}"
    items = schema.get("items", {})
    for index, item in enumerate(value):
        if isinstance(items, Mapping):
            of = items
        elif index < len(items):
            of = items[index]
        else:
            of = schema.get("additionalItems", {})
        problem = _mismatch(of, item, resp, root, f"{where}[{index}]")
        if problem is not None:
            return problem
    return None


def _object_mismatch(schema: Mapping[str, Any], value: dict, resp: int, root: Mapping[str, Any], where: str) -> Any:
    properties, patterns = schema.get("properties", {}), schema.get("patternProperties", {})
    additional = schema.get("additionalProperties", {})
    for key, item in value.items():
        described = [properties[key]] if key in properties else []
        described += [of for pattern, of in patterns.items() if re.search(pattern, key)]
        if not described and additional is False:
            return f"{where} has a field the schema does not: {key}"
        if not described and additional is not True:
            described = [additional]
        for of in described:
            problem = _mismatch(of, item, resp, root, f"{where}.{key}")
            if problem is not None:
                return problem
    return None


if __name__ == "__main__":
    _PATH.write_text(dumped(sys.argv[2], read_specs(Path(sys.argv[1]) / "src" / "commands")), encoding="utf-8")
