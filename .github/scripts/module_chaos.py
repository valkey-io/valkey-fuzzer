"""Usage: module_chaos.py DIR

Write the module chaos experiments: module data written before and while an instance is killed, stopped or
restarted, then read back on every instance, and every replica's module values digested against its primary's."""

import base64
import json
import random
import struct
import sys
from pathlib import Path

import yaml

OUT = Path(sys.argv[1])
OUT.mkdir(parents=True, exist_ok=True)


def cmd(*words, expect=None):
    item = {"command": [str(w) for w in words]}
    if expect is not None:
        item["expect"] = expect
    return item


def faults_and_oracles(module_command, load, during, verify, oracles):
    """The shared shape: probe, load, a replica fault and an optional primary stall at load.completed, writes
    during the fault, a settle, reads on every instance, the digest of every replica, and the module's own
    ``oracles`` once the replicas are linked."""
    return [
        {
            "use": "valkey.capability.build-probe",
            "id": "build",
            "config": {"commands": [module_command], "deployment_config": ["loadmodule"]},
        },
        {"use": "valkey.action.commands", "id": "load", "config": {"timeout_s": 60, "commands": load}},
        # The faults fire once the load is in: a sleep that follows the load publishes the event they wait for.
        {
            "use": "valkey.action.sleep",
            "id": "loaded",
            "depends_on": ["load", "hit-replica", "stall-primary"],
            "config": {"seconds": 1},
        },
        {
            "use": "valkey.fault.process-signal",
            "id": "hit-replica",
            "config": {
                "when_event": "loaded.completed",
                "instance": "replica",
                "signal": {"$ref": "inputs.signal"},
                "restart_mode": {"$ref": "inputs.restart_mode"},
                "restart_delay_s": {"$ref": "inputs.restart_delay_s"},
                "pause_s": {"$ref": "inputs.pause_s"},
            },
        },
        {
            "use": "valkey.fault.process-signal",
            "id": "stall-primary",
            "config": {
                "when_event": "loaded.completed",
                "instance": "primary",
                "signal": "SIGSTOP",
                "pause_s": {"$ref": "inputs.primary_pause_s"},
            },
        },
        {
            "use": "valkey.action.commands",
            "id": "during",
            "depends_on": ["loaded"],
            "config": {"timeout_s": 60, "commands": during},
        },
        {"use": "valkey.action.sleep", "id": "settle", "depends_on": ["during"], "config": {"seconds": 5}},
        {
            "use": "valkey.action.poll",
            "id": "links-up",
            "depends_on": ["settle"],
            "config": {
                "instances": "replicas",
                "section": "replication",
                "field": "master_link_status",
                "predicate": {"equals": "up"},
                "timeout_s": 120,
            },
        },
        {
            "use": "valkey.action.commands",
            "id": "verify",
            "depends_on": ["links-up"],
            "config": {"instances": "all", "timeout_s": 60, "commands": verify},
        },
        {
            "use": "valkey.oracle.dataset-digest",
            "id": "digest",
            "depends_on": ["verify"],
            "config": {"include_replicas": True, "converge_timeout_s": 60},
        },
        # The ledger may lose what a SIGKILLed instance acknowledged within its fsync window.
        {"use": "valkey.oracle.ledger", "id": "data", "config": {"workload": "writes", "loss_window_s": 2.5}},
        # They judge on their own, so a fixed read that fails does not hide what they find.
        *({**oracle, "depends_on": ["links-up"]} for oracle in oracles),
    ]


def experiment(name, module, module_command, load, during, verify, oracles=()):
    doc = {
        "extends": ["valkey.base"],
        "id": f"proposed.{name}-chaos",
        "deployment": {"modes": ["standalone"], "replicas_per_primary": {"minimum": 1}},
        "metadata": {
            "description": f"{module} data written before and while a replica is killed, stopped or restarted "
            "(and the primary stalled); every instance must hold the same data after",
            "generator": {
                "deployment": {"standalone": {"replicas_per_primary": {"minimum": 1, "maximum": 3}}},
                "inputs": {
                    "signal": {"values": ["SIGKILL", "SIGTERM", "SIGSTOP"]},
                    "restart_mode": {"values": ["immediate", "background"]},
                    "restart_delay_s": {"minimum": 0, "maximum": 3},
                    "pause_s": {"minimum": 0.5, "maximum": 5},
                    "primary_pause_s": {"minimum": 0, "maximum": 4},
                    "mutation_rate": {"minimum": 50, "maximum": 1500},
                },
                "server_config": ["valkey.pool.replication"],
            },
        },
        "inputs": {
            "properties": {
                "signal": {"type": "string", "enum": ["SIGKILL", "SIGTERM", "SIGSTOP"], "default": "SIGKILL"},
                "restart_mode": {"type": "string", "enum": ["immediate", "background"], "default": "immediate"},
                "restart_delay_s": {"type": "number", "minimum": 0, "maximum": 60, "default": 0},
                "pause_s": {"type": "number", "minimum": 0, "maximum": 60, "default": 2},
                "primary_pause_s": {"type": "number", "minimum": 0, "maximum": 60, "default": 0},
            }
        },
        "components": faults_and_oracles(module_command, load, during, verify, oracles),
        "required_events": ["loaded.completed"],
        "required_evidence": [{"evidence_id": "valkey.fault.injected", "source": "hit-replica"}],
    }
    (OUT / f"proposed.{name}-chaos.yaml").write_text(yaml.safe_dump(doc, sort_keys=False, width=120))


def json_doc(i):
    return {
        "id": i,
        "name": f"n{i}",
        "tags": [f"t{i % 7}", f"t{i % 3}"],
        "nested": {"a": {"b": [i, i * 2, None]}, "s": "x" * (i % 50)},
        "price": i * 1.5,
        "ok": i % 2 == 0,
    }


N = 300
load = [cmd("JSON.SET", f"j:{i}", "$", json.dumps(json_doc(i)), expect={"equals": "OK"}) for i in range(N)]
load += [cmd("JSON.ARRAPPEND", f"j:{i}", "$.tags", '"extra"') for i in range(0, N, 5)]
load += [cmd("JSON.NUMINCRBY", f"j:{i}", "$.price", "2.25") for i in range(0, N, 3)]
during = [cmd("JSON.SET", f"j:{i}", "$.nested.a.b[1]", str(i * 10)) for i in range(N)]
during += [cmd("JSON.DEL", f"j:{i}", "$.nested.s") for i in range(0, N, 4)]
during += [
    cmd("JSON.SET", f"jn:{i}", "$", json.dumps({"big": ["y" * 200] * 20, "i": i}), expect={"equals": "OK"})
    for i in range(100)
]
during += [
    cmd("JSON.MSET", *[w for i in range(10) for w in (f"jm:{i}", "$", json.dumps({"m": i}))], expect={"equals": "OK"})
]
verify = [cmd("JSON.GET", f"j:{i}", "$.nested.a.b[1]", expect={"equals": json.dumps([i * 10])}) for i in range(0, N, 7)]
verify += [cmd("JSON.ARRLEN", f"j:{i}", "$.tags", expect={"equals": [3 if i % 5 == 0 else 2]}) for i in range(0, N, 11)]
verify += [cmd("JSON.GET", f"jn:{i}", "$.i", expect={"equals": json.dumps([i])}) for i in range(0, 100, 9)]
verify += [cmd("JSON.TYPE", "jm:3", "$.m", expect={"equals": ["integer"]})]
experiment("json", "valkey-json", "JSON.SET", load, during, verify)

# Bloom: filters of every shape, items added before and during; a filter never forgets an item.
shapes = [
    ("b:s1", ["0.01", "1000"]),
    ("b:s2", ["0.001", "100", "EXPANSION", "2"]),
    ("b:s3", ["0.1", "50", "NONSCALING"]),
    ("b:s4", ["0.001", "10", "EXPANSION", "4"]),
    ("b:s5", ["0.0001", "5000"]),
]
load = [cmd("BF.RESERVE", key, *args, expect={"equals": "OK"}) for key, args in shapes]
for key, _ in shapes:
    load += [cmd("BF.MADD", key, *[f"{key}-i{j}" for j in range(start, start + 40)]) for start in range(0, 40, 40)]
load += [cmd("BF.ADD", f"b:a{i}", f"item{i}") for i in range(200)]
during = []
for key, args in shapes:
    if "NONSCALING" in args:
        continue
    during += [cmd("BF.MADD", key, *[f"{key}-i{j}" for j in range(start, start + 40)]) for start in range(40, 200, 40)]
during += [
    cmd("BF.INSERT", f"b:ins{i}", "CAPACITY", "100", "ERROR", "0.01", "ITEMS", *[f"x{j}" for j in range(30)])
    for i in range(50)
]
during += [cmd("COPY", "b:s1", f"b:copy{i}") for i in range(5)]
verify = []
for key, args in shapes:
    # A NONSCALING filter holds only what the load added: 40 items.
    upto, step = (40, 40) if "NONSCALING" in args else (200, 50)
    for start in range(0, upto, step):
        verify.append(
            cmd("BF.MEXISTS", key, *[f"{key}-i{j}" for j in range(start, start + step)], expect={"equals": [1] * step})
        )
verify += [cmd("BF.EXISTS", f"b:a{i}", f"item{i}", expect={"equals": 1}) for i in range(0, 200, 13)]
verify += [
    cmd("BF.MEXISTS", f"b:ins{i}", *[f"x{j}" for j in range(30)], expect={"equals": [1] * 30}) for i in range(0, 50, 7)
]
verify += [cmd("BF.MEXISTS", "b:copy2", *[f"b:s1-i{j}" for j in range(40)], expect={"equals": [1] * 40})]
experiment("bloom", "valkey-bloom", "BF.ADD", load, during, verify)


# Search: indexes of every field type over hashes written before and during; every instance indexes the same.
def vec(i):
    return {"$base64": base64.b64encode(struct.pack("<4f", i, i % 7, -i, 1.0)).decode()}


schema = [
    "SCHEMA",
    "v",
    "VECTOR",
    "HNSW",
    "6",
    "TYPE",
    "FLOAT32",
    "DIM",
    "4",
    "DISTANCE_METRIC",
    "L2",
    "f",
    "VECTOR",
    "FLAT",
    "6",
    "TYPE",
    "FLOAT32",
    "DIM",
    "4",
    "DISTANCE_METRIC",
    "COSINE",
    "g",
    "VECTOR",
    "FLAT",
    "6",
    "TYPE",
    "FLOAT32",
    "DIM",
    "4",
    "DISTANCE_METRIC",
    "L2",
    "tag",
    "TAG",
    "c",
    "TAG",
    "n",
    "NUMERIC",
    "p",
    "NUMERIC",
    "title",
    "TEXT",
    "w",
    "TEXT",
    "NOSTEM",
    "WITHSUFFIXTRIE",
]

# Words that share prefixes and suffixes and lie a few edits apart, so every text matcher has near misses; stop
# words, which take no position, and every case and separator the tokenizer folds or splits at.
VOCABULARY = [
    "alpha", "alpine", "alp", "alps", "beta", "bet", "better", "bitter", "gamma", "gama", "magma",
    "delta", "deltas", "melt", "echo", "eco", "cheo", "zulu", "zulus", "lulu", "kilo", "kiloo",
    "milo", "oak", "oaks", "soak", "x1", "x12", "x123",
]  # fmt: skip
SEPARATORS = [" ", "  ", ",", ", ", ".", "-", "/", "\t", ";", "!?", "(", ")"]


def text(i, salt=0):
    """A few words, seeded by the key, in mixed case with stop words between them."""

    rng = random.Random(i * 1000 + salt)
    picked = [
        rng.choice(VOCABULARY) if rng.random() < 0.8 else rng.choice(["a", "of", "And", "THE"])
        for _ in range(rng.randint(2, 7))
    ]
    cased = [rng.choice([word, word.upper(), word.capitalize()]) for word in picked]
    return "".join(word + rng.choice(SEPARATORS) for word in cased).rstrip()


def fields(i):
    """The fields the search-results oracle judges: a FLAT L2 vector, tags of mixed case with several to a
    hash, a numeric field of fractions and negatives, and text. No rewrite below changes only a tag's case:
    valkey-search drops such a key from the tag (valkey-io/valkey-search#1599, open)."""
    return ["g", vec(i * 7 % 50), "c", f"c{i % 4},D{i % 6}", "p", str((i % 37) * 0.25 - 4), "w", text(i)]


load = [
    cmd("FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "d:", *schema, expect={"equals": "OK"}),
    cmd(
        "FT.CREATE",
        "idx2",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "e:",
        "SCHEMA",
        "tag",
        "TAG",
        "n",
        "NUMERIC",
        expect={"equals": "OK"},
    ),
]
load += [
    {
        "command": [
            "HSET",
            f"d:{i}",
            "v",
            vec(i),
            "f",
            vec(i + 1),
            "tag",
            f"t{i % 5}",
            "n",
            str(i),
            "title",
            f"hello word{i % 9}",
            *fields(i),
        ]
    }
    for i in range(400)
]
load += [cmd("HSET", f"e:{i}", "tag", f"g{i % 3}", "n", str(i)) for i in range(200)]
load += [cmd("FT.SEARCH", "idx", "*", "LIMIT", "0", "0", expect={"equals": [400]}) | {"wait_s": 60}]
during = [{"command": ["HSET", f"d:{i}", "v", vec(i * 3), "n", str(i * 2)]} for i in range(0, 400, 2)]
during += [cmd("DEL", f"d:{i}") for i in range(0, 400, 10)]
during += [
    {
        "command": [
            "HSET",
            f"d:{i}",
            "v",
            vec(i),
            "f",
            vec(i),
            "tag",
            "late",
            "n",
            str(i),
            "title",
            "late doc",
            *fields(i),
        ]
    }
    for i in range(400, 500)
]
# Tags rewritten, a numeric field removed, and vectors moved, on keys the DELs left: the index must follow each.
during += [cmd("HSET", f"d:{i}", "c", f"moved,c{i % 3}") for i in range(1, 400, 3) if i % 10]
during += [cmd("HDEL", f"d:{i}", "p") for i in range(3, 400, 7) if i % 10]
during += [{"command": ["HSET", f"d:{i}", "g", vec(i % 11)]} for i in range(5, 500, 4)]
# Text rewritten with other words, rewritten in another case only, and removed.
during += [cmd("HSET", f"d:{i}", "w", text(i, salt=1)) for i in range(2, 500, 5) if i % 10]
during += [cmd("HSET", f"d:{i}", "w", text(i).swapcase()) for i in range(4, 500, 9) if i % 10 and i % 5 != 2]
during += [cmd("HDEL", f"d:{i}", "w") for i in range(6, 500, 11) if i % 10]
during += [
    cmd("FT.DROPINDEX", "idx2", expect={"equals": "OK"}),
    cmd(
        "FT.CREATE",
        "idx2",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "e:",
        "SCHEMA",
        "tag",
        "TAG",
        "n",
        "NUMERIC",
        expect={"equals": "OK"},
    ),
]
# 400 + 100 written, 40 deleted.
verify = [
    cmd("FT.SEARCH", "idx", "*", "LIMIT", "0", "0", expect={"equals": [460]}) | {"wait_s": 60},
    cmd("FT.SEARCH", "idx", "@tag:{late}", "LIMIT", "0", "0", expect={"equals": [100]}) | {"wait_s": 30},
    cmd("FT.SEARCH", "idx", "@n:[0 0]", "LIMIT", "0", "0", expect={"equals": [0]}),
    cmd("FT.SEARCH", "idx2", "*", "LIMIT", "0", "0", expect={"equals": [200]}) | {"wait_s": 60},
    cmd("FT.SEARCH", "idx2", "@tag:{g1}", "LIMIT", "0", "0", expect={"equals": [67]}) | {"wait_s": 30},
    {
        "command": ["FT.SEARCH", "idx", "*=>[KNN 5 @v $q]", "PARAMS", "2", "q", vec(401), "NOCONTENT", "DIALECT", "2"],
        "expect": {"exists": True},
    },
]
results = {
    "use": "valkey.oracle.search-results",
    "id": "results",
    "config": {
        "index": "idx",
        "prefix": "d:",
        "tag_fields": ["tag", "c"],
        "numeric_fields": ["n", "p"],
        "text_fields": ["title", "w"],
        "suffix_fields": ["w"],
        "vector_field": "g",
        "dim": 4,
        "queries": 200,
        "converge_timeout_s": 60,
    },
}
experiment("search", "valkey-search", "FT.CREATE", load, during, verify, [results])
print("written", sorted(p.name for p in OUT.iterdir()))
