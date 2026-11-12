"""Reply schemas: the ones shipped, how they are read from Valkey's command specs, and which replies match one over
RESP3 and over RESP2."""

import json

import pytest

from valkey_fuzzer.components import reply_schemas
from valkey_fuzzer.components.actions import raw_reply
from valkey_fuzzer.components.reply_schemas import dumped, mismatch, read_specs

_STRING, _INTEGER, _NULL = {"type": "string"}, {"type": "integer"}, {"type": "null"}
_PAIR = {"type": "array", "minItems": 2, "maxItems": 2, "items": [_STRING, {"type": "number"}]}


def _keywords(schema):
    """Every keyword of ``schema`` and of the schemas inside it, with the value it has."""

    for keyword, value in schema.items():
        yield keyword, value
        if keyword in ("properties", "patternProperties"):
            inner = list(value.values())
        elif keyword in ("oneOf", "anyOf") or (keyword == "items" and isinstance(value, list)):
            inner = value
        elif keyword in ("items", "additionalItems", "additionalProperties") and isinstance(value, dict):
            inner = [value]
        else:
            inner = []
        for item in inner:
            yield from _keywords(item)


def test_the_shipped_schemas_use_only_what_a_reply_is_judged_by():
    schemas = reply_schemas.reply_schemas()
    assert len(schemas) > 400 and {"GET", "CONFIG GET", "XINFO STREAM"} <= set(schemas)
    for name, schema in schemas.items():
        for keyword, value in _keywords(schema):
            assert keyword in reply_schemas.KEYWORDS, f"{name}: {keyword}"
            assert keyword != "type" or value in reply_schemas._TYPES, f"{name}: type {value}"
            assert keyword != "$ref" or value == "#", f"{name}: $ref {value}"


def test_the_schemas_are_read_from_the_command_specs_by_table_name(tmp_path):
    specs = {
        "get.json": {"GET": {"reply_schema": {"oneOf": [_STRING, _NULL]}, "arguments": []}},
        "config-get.json": {"GET": {"container": "CONFIG", "reply_schema": {"type": "object"}}},
        # A container, and a command Valkey declares no schema for.
        "config.json": {"CONFIG": {"group": "server"}},
        "sync.json": {"SYNC": {"group": "server"}},
    }
    for name, spec in specs.items():
        (tmp_path / name).write_text(json.dumps(spec))
    schemas = read_specs(tmp_path)
    assert schemas == {"GET": {"oneOf": [_STRING, _NULL]}, "CONFIG GET": {"type": "object"}}
    text = dumped("unstable 0123abc", schemas)
    assert json.loads(text) == {"valkey": "unstable 0123abc", "schemas": schemas}
    # One command a line, in name order, so a newer tree's file differs only where a schema does.
    assert text.splitlines()[3:5] == [
        '"CONFIG GET": {"type": "object"},',
        '"GET": {"oneOf": [{"type": "string"}, {"type": "null"}]}',
    ]


@pytest.mark.parametrize(
    ("schema", "reply", "problem"),
    [
        (_STRING, "OK", None),
        (_STRING, 1, "reply is not of type string: 1"),
        # A bulk string that is not UTF-8 is a string.
        (_STRING, raw_reply(b"\xff\x00"), None),
        (_INTEGER, 1, None),
        # An integer is an integer reply, not a double without a fraction, nor a boolean.
        (_INTEGER, 1.0, "reply is not of type integer: 1.0"),
        (_INTEGER, True, "reply is not of type integer: true"),
        ({"type": "number"}, 1.5, None),
        ({"type": "number"}, 2, None),
        # A double that is not finite, as a reply is recorded.
        ({"type": "number"}, raw_reply(float("inf")), None),
        ({"type": "number"}, "1.5", 'reply is not of type number: "1.5"'),
        (_NULL, None, None),
        (_NULL, 0, "reply is not of type null: 0"),
        ({"const": "OK"}, "OK", None),
        ({"const": "OK"}, "QUEUED", 'reply is not "OK": "QUEUED"'),
        ({"const": 1}, True, "reply is not 1: true"),
        ({"type": "string", "pattern": "^[0-9]+-[0-9]+$"}, "12-0", None),
        (
            {"type": "string", "pattern": "^[0-9]+-[0-9]+$"},
            "12",
            'reply does not match the pattern ^[0-9]+-[0-9]+$: "12"',
        ),
        ({"type": "integer", "minimum": 0}, -1, "reply is outside 0..: -1"),
        ({"type": "integer", "minimum": 0, "maximum": 1}, 2, "reply is outside 0..1: 2"),
        ({"type": "array", "items": _STRING}, ["a", "b"], None),
        ({"type": "array", "items": _STRING}, ["a", None], "reply[1] is not of type string: null"),
        ({"type": "array", "items": _STRING}, {"a": "b"}, 'reply is not of type array: {"a":"b"}'),
        (_PAIR, ["a", 1.5], None),
        (_PAIR, ["a", 1.5, "b"], 'reply has 3 items: ["a",1.5,"b"]'),
        (_PAIR, ["a"], 'reply has 1 items: ["a"]'),
        # Items by position, and what follows them.
        ({"type": "array", "items": [_STRING], "additionalItems": _INTEGER}, ["a", 1, 2], None),
        (
            {"type": "array", "items": [_STRING], "additionalItems": _INTEGER},
            ["a", 1, "b"],
            'reply[2] is not of type integer: "b"',
        ),
        ({"type": "array", "uniqueItems": True}, ["a", "b"], None),
        ({"type": "array", "uniqueItems": True}, ["a", "a"], 'reply repeats an item: ["a","a"]'),
        ({"type": "object", "properties": {"n": _INTEGER}}, {"n": 1, "other": "x"}, None),
        ({"type": "object", "properties": {"n": _INTEGER}}, {"n": "1"}, 'reply.n is not of type integer: "1"'),
        (
            {"type": "object", "properties": {"n": _INTEGER}, "additionalProperties": False},
            {"n": 1, "other": "x"},
            "reply has a field the schema does not: other",
        ),
        ({"type": "object", "additionalProperties": _STRING}, {"a": 1}, "reply.a is not of type string: 1"),
        ({"type": "object", "additionalProperties": True}, {"a": 1}, None),
        (
            {"type": "object", "patternProperties": {r"^db\.\d+$": _INTEGER}, "additionalProperties": False},
            {"db.0": 1, "db.x": 1},
            "reply has a field the schema does not: db.x",
        ),
        ({"oneOf": [_STRING, _NULL]}, None, None),
        (
            {"oneOf": [_STRING, _NULL]},
            1,
            "reply matches none of the 2 alternatives (reply is not of type string: 1; reply is not of type null: 1)",
        ),
        # Exactly one alternative of a oneOf, any of an anyOf.
        ({"oneOf": [_INTEGER, {"type": "number"}]}, 1, "reply matches 2 alternatives of a oneOf: 1"),
        ({"anyOf": [_INTEGER, {"type": "number"}]}, 1, None),
        # A schema inside itself (COMMAND DOCS: a subcommand's docs).
        ({"type": "object", "properties": {"n": _INTEGER, "sub": {"$ref": "#"}}}, {"sub": {"sub": {"n": 1}}}, None),
        (
            {"type": "object", "properties": {"n": _INTEGER, "sub": {"$ref": "#"}}},
            {"sub": {"sub": {"n": "1"}}},
            'reply.sub.sub.n is not of type integer: "1"',
        ),
    ],
)
def test_a_resp3_reply_matches_its_schema_or_names_where_it_does_not(schema, reply, problem):
    assert mismatch(schema, reply, 3) == problem


@pytest.mark.parametrize(
    ("schema", "reply"),
    [
        # A double is a string.
        ({"type": "number"}, "1.5"),
        ({"type": "number"}, "-1e3"),
        ({"oneOf": [{"type": "number"}, _NULL]}, "3"),
        # A map is a flat array of its keys and values, an integer key as it is.
        ({"type": "object", "additionalProperties": _STRING}, ["f1", "v1", "f2", "v2"]),
        ({"type": "object", "additionalProperties": _STRING}, []),
        ({"type": "object", "additionalProperties": _INTEGER}, [8, 1, 16, 3]),
        (
            {"type": "object", "properties": {"calls": _INTEGER, "usec": {"type": "object"}}},
            ["calls", 3, "usec", [8, 1]],
        ),
        # XREAD's map is an array of [key, value] pairs.
        ({"type": "object", "additionalProperties": {"type": "array"}}, [["stream", [["1-0", ["f", "v"]]]]]),
        # An array of pairs is one flat array, a score in it a string.
        ({"type": "array", "items": _PAIR}, ["a", "1.5", "b", "2"]),
        ({"type": "array", "uniqueItems": True, "items": _PAIR}, ["a", "1", "b", "1"]),
    ],
)
def test_a_resp2_reply_matches_in_the_form_resp2_has_for_what_the_schema_describes(schema, reply):
    assert mismatch(schema, reply, 2) is None
    assert mismatch(schema, reply, 3) is not None


def test_a_resp2_reply_may_match_the_alternative_written_for_resp2_and_the_one_for_resp3():
    schema = {"oneOf": [{"type": "array", "items": _STRING}, {"type": "object"}]}
    assert mismatch(schema, ["f", "v"], 2) is None
    assert mismatch(schema, ["f", "v"], 3) is None and mismatch(schema, {"f": "v"}, 3) is None
    assert mismatch(schema, 1, 2).startswith("reply matches none of the 2 alternatives")


@pytest.mark.parametrize(
    ("schema", "reply", "problem"),
    [
        ({"type": "number"}, "abc", 'reply is not of type number: "abc"'),
        (
            {"type": "object", "additionalProperties": _STRING},
            ["f1", "v1", "f2"],
            'reply is not of type object: ["f1","v1","f2"]',
        ),
        ({"type": "object", "additionalProperties": _STRING}, ["f1", 1], "reply.f1 is not of type string: 1"),
        ({"type": "array", "items": _PAIR}, ["a", "1.5", "b"], 'reply[0] is not of type array: "a"'),
        ({"type": "array", "items": _PAIR}, ["a", "x"], 'reply[0][1] is not of type number: "x"'),
        (_INTEGER, "1", 'reply is not of type integer: "1"'),
    ],
)
def test_a_resp2_reply_in_no_form_of_what_the_schema_describes_does_not_match(schema, reply, problem):
    assert mismatch(schema, reply, 2) == problem


def test_a_long_reply_is_shown_cut_and_so_is_a_long_problem():
    problem = mismatch(_INTEGER, "x" * 5000, 3)
    assert problem.startswith('reply is not of type integer: "xxx') and len(problem) < 200
    # A reply that matches none of many alternatives names each one's problem.
    problem = mismatch({"oneOf": [{"const": f"alternative {n}"} for n in range(20)]}, "x" * 5000, 3)
    assert problem.startswith("reply matches none of the 20 alternatives (reply is not") and problem.endswith("...")
    assert len(problem) == 600


@pytest.mark.parametrize(
    ("name", "resp", "reply"),
    [
        ("GET", 2, b"value"),
        ("GET", 3, None),
        ("HGETALL", 3, {b"field:1": b"v"}),
        ("HGETALL", 2, [b"field:1", b"v"]),
        ("ZSCORE", 3, 1.5),
        ("ZSCORE", 2, b"1.5"),
        ("ZRANGE", 3, [[b"a", 1.0], [b"b", 1.0]]),
        ("ZRANGE", 2, [b"a", b"1", b"b", b"1"]),
        ("ZPOPMIN", 2, [b"a", b"1", b"b", b"2"]),
        ("HRANDFIELD", 2, [b"f", b"v", b"f", b"v"]),
        ("XREAD", 3, {b"stream": [[b"1-0", [b"f", b"v"]]]}),
        ("XREAD", 2, [[b"stream", [[b"1-0", [b"f", b"v"]]]]]),
        ("SMEMBERS", 3, {b"a", b"b"}),
        ("LATENCY HISTOGRAM", 2, [b"set", [b"calls", 2, b"histogram_usec", [8, 1, 16, 2]]]),
        ("LATENCY HISTOGRAM", 3, {b"set": {b"calls": 2, b"histogram_usec": {8: 1, 16: 2}}}),
    ],
)
def test_replies_valkey_sends_match_the_shipped_schemas_over_both_protocols(name, resp, reply):
    assert mismatch(reply_schemas.reply_schemas()[name], raw_reply(reply), resp) is None
