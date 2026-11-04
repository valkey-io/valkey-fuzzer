"""The key/value workload's renames and list reads, and that their defaults leave every other draw unchanged."""

import hashlib
import json
import os
from types import SimpleNamespace

import pytest
import valkey

from tests.test_replication_durability import _component, _standalone, _StoreClient
from tests.valkey_experiments_support import fake_context
from valkey_fuzzer.components.ledger import LedgerModel, load_ledger
from valkey_fuzzer.components.snapshots import _list_problems
from valkey_fuzzer.components.workloads import (
    _WORKLOAD_FIELDS,
    KeyValueWorkload,
    _validate_workload,
    list_digest,
    list_elements,
    list_key,
)
from valkey_fuzzer.protocols import ComponentRole
from valkey_fuzzer.schema import ConfigError, ConfigSchema

# The ledger (id, op, key, value, outcome, reply) and the preload the workload produced from seed 7 with
# `_run`'s configuration before renames and list reads existed. The defaults must still draw exactly these.
_DRAWS_BEFORE = "d67600e46ae1a148e0780fe9a6205fa4be7db52b6bfeadad1810cca64d6bbc01"
_PRELOAD_BEFORE = "7eb9f11985e260bfe0b18cf9b5b0d39093b579191e172ce6d5a82d02bc7921cb"


class _Client(_StoreClient):
    """A store that also renames, holds lists, and records every pipelined list read."""

    def __init__(self, rename_error=None):
        super().__init__()
        self.lists, self.reads, self.rename_error = {}, [], rename_error

    def rename(self, src, dst):
        if self.rename_error is not None:
            raise self.rename_error
        if src not in self.store:
            raise valkey.exceptions.ResponseError("no such key")
        self.store[dst] = self.store.pop(src)
        return True

    def rpush(self, key, *elements):
        self.lists.setdefault(key, []).extend(elements)
        return len(self.lists[key])

    def pipeline(self, transaction=False):
        batch = []

        def execute():
            replies = []
            for command in batch:
                self.reads.append(command)
                if command[0] == "lrange":
                    replies.append(self.lists[command[1]][command[2] : command[3] + 1])
                else:
                    replies.append(self.lists[command[1]][command[2]])
            return replies

        return SimpleNamespace(
            set=self.set,
            lrange=lambda key, start, stop: batch.append(("lrange", key, start, stop)),
            lindex=lambda key, index: batch.append(("lindex", key, index)),
            execute=lambda: execute() if batch else [True] * len(self.store),
        )


def _run(tmp_path, client, seed=7, **config):
    config = {"stable_keys": 3, "mutable_keys": 20, "rate": 5000, "max_ops": 300, "drain_on_stop": True, **config}
    spec = _component("valkey.workload.key-value", "writes", ComponentRole.WORKLOAD, config)
    context = fake_context(_standalone(client), spec, artifact_dir=str(tmp_path), seed=seed)
    workload = KeyValueWorkload()
    assert workload.start(context, spec).status.value == "PASS"
    result = workload.stop(context, spec)
    assert result.status.value == "PASS", result.summary
    return workload, result


def _digest(material):
    return hashlib.sha256(json.dumps(material, separators=(",", ":")).encode("utf-8")).hexdigest()


def test_the_defaults_draw_the_same_operations_and_values_as_before_renames_and_list_reads(tmp_path):
    workload, result = _run(tmp_path, _StoreClient())
    ledger = [[e["id"], e["op"], e["key"], e.get("value"), e["outcome"], e.get("reply")] for e in workload._ledger]
    assert len(ledger) == 300 and _digest(ledger) == _DRAWS_BEFORE
    assert _digest(sorted(workload._preload.items())) == _PRELOAD_BEFORE
    _, _, _, summary = load_ledger(result.evidence, "writes")
    assert (
        summary["list_keys"] == 0 and summary["lists_path"] is None and summary["list_reads"] == {"ok": 0, "failed": 0}
    )


def test_a_rename_is_a_del_of_its_source_and_a_set_of_its_destination_with_one_outcome(tmp_path):
    client = _Client()
    _, result = _run(tmp_path, client, mix={"rename": 1, "del": 0.2})
    preload, ledger, acks, _ = load_ledger(result.evidence, "writes")
    pairs = [(a, b) for a, b in zip(ledger, ledger[1:]) if a.get("rename") and a["rename"] == b.get("rename")]
    assert pairs, "renames ran"
    for src_entry, dst_entry in pairs:
        src, dst = src_entry["rename"]
        assert (src_entry["op"], src_entry["key"]) == ("del", src) and (dst_entry["op"], dst_entry["key"]) == (
            "set",
            dst,
        )
        assert dst_entry["id"] == src_entry["id"] + 1 and src_entry["outcome"] == dst_entry["outcome"] == "ok"
        assert len(src) == len(dst) and src != dst and "value" not in src_entry
        assert src_entry["t_end"] == dst_entry["t_end"] and src_entry["reply"] == dst_entry["reply"] == "OK"
    # The ledger's model of a rename is right: the store holds what it allows for every key.
    model = LedgerModel(preload, ledger, [], preload_acks=acks)
    assert all(client.store.get(key) in model.possible[key] for key in model.possible)
    assert all(model.possible[key] == {client.store.get(key)} for key in model.possible), "every outcome was certain"
    # A rename's destination existed: the workload only renames over a key whose value it knows.
    assert all(dst_entry["key"] in preload or dst_entry["key"] in client.store for _, dst_entry in pairs)
    assert sum(entry["op"] == "del" for entry in ledger) >= len(pairs)


def test_a_rename_whose_source_is_not_known_for_certain_becomes_a_set(tmp_path):
    client = _Client()
    # Nothing is preloaded for the mutable keys, so nothing is known until a SET is acknowledged.
    _, result = _run(tmp_path, client, mix={"rename": 1}, preload_mutable=False, max_ops=40)
    preload, ledger, _, _ = load_ledger(result.evidence, "writes")
    assert ledger[0]["op"] == "set" and "rename" not in ledger[0]
    renamed = [entry for entry in ledger if entry.get("rename")]
    assert renamed and all(entry["outcome"] == "ok" for entry in renamed)
    first_rename = ledger.index(renamed[0])
    acknowledged_before = {entry["key"] for entry in ledger[:first_rename] if entry["op"] == "set"}
    assert set(renamed[0]["rename"]) <= acknowledged_before, "both keys were known when the first rename ran"
    model = LedgerModel(preload, ledger, [])
    assert all(client.store.get(key) in model.possible[key] for key in client.store)


@pytest.mark.parametrize(
    ("error", "outcome"),
    [(valkey.exceptions.TimeoutError("late"), "ambiguous"), (valkey.exceptions.ResponseError("no such key"), "error")],
)
def test_an_ambiguous_rename_leaves_both_keys_uncertain_and_an_error_neither(error, outcome):
    client = _Client(rename_error=error)
    workload = KeyValueWorkload()
    workload._config = {}
    workload._target = SimpleNamespace(
        connect=lambda: client, instance_id="valkey-0", served_by=lambda key: "valkey-0", reset=lambda: None
    )
    workload._known = {"a": "1", "b": "2"}
    assert workload._execute(5, "rename", "a", "1", "b") == outcome
    assert [entry["outcome"] for entry in workload._ledger] == [outcome, outcome]
    assert workload._known == ({} if outcome == "ambiguous" else {"a": "1", "b": "2"})
    model = LedgerModel({"a": "1", "b": "2"}, workload._ledger, [])
    if outcome == "ambiguous":
        assert model.possible["a"] == {"1", None} and model.possible["b"] == {"2", "1"}
    else:
        assert model.possible["a"] == {"1"} and model.possible["b"] == {"2"}


def test_list_reads_are_pipelined_and_counted_and_the_lists_are_described_for_the_snapshot_oracle(tmp_path):
    client = _Client()
    _, result = _run(
        tmp_path,
        client,
        mix={"lrange": 1, "lindex": 1},
        list_keys=2,
        list_length=2500,
        list_element_size=40,
        list_read_batch=5,
        preload_batch=1000,
        max_ops=20,
    )
    _, ledger, _, summary = load_ledger(result.evidence, "writes")
    assert ledger == [] and summary["operations"] == 0, "reads are counted, not ledgered"
    assert summary["list_reads"] == {"ok": 20, "failed": 0} and summary["list_keys"] == 2
    assert len(client.reads) == 100 and {command[0] for command in client.reads} == {"lrange", "lindex"}
    assert all(command[3] - command[2] == 29 for command in client.reads if command[0] == "lrange")
    assert all(0 <= command[2] < 2500 for command in client.reads)
    prefix = summary["prefix"]
    assert set(client.lists) == {list_key(prefix, 0), list_key(prefix, 1)}
    assert all(len(elements) == 2500 and all(len(e) == 40 for e in elements) for elements in client.lists.values())
    assert (
        os.path.basename(summary["lists_path"]) == "lists.jsonl"
        and summary["lists_path"] in result.evidence[0].artifacts
    )
    with open(summary["lists_path"], encoding="utf-8") as handle:
        described = [json.loads(line) for line in handle]
    assert described == [
        {"key": list_key(prefix, index), "length": 2500, "digest": list_digest(list_elements(index, 2500, 40))}
        for index in range(2)
    ]
    assert all(item["digest"] == list_digest(client.lists[item["key"]]) for item in described)
    # Elements repeat a tag, so quicklist nodes compress; different lists and positions differ.
    elements = list(list_elements(3, 4, 40))
    assert elements[1].startswith("l3:1:l3:1:") and len(set(elements)) == 4


def test_the_snapshot_oracle_checks_a_reloaded_list_against_what_the_workload_wrote():
    lists = {"vf:writes:list:0": {"key": "vf:writes:list:0", "length": 3, "digest": list_digest(["a", "b", "c"])}}
    assert _list_problems("valkey-0", lists, {"vf:writes:list:0": ["list", ["a", "b", "c"]]}) == []
    assert _list_problems("valkey-0", lists, {"other": ["list", []]}) == [], "a list another shard holds"
    assert _list_problems("valkey-0", lists, {"vf:writes:list:0": ["list", ["a", "b"]]}) == [
        "valkey-0: vf:writes:list:0 reloaded with 2 element(s), the workload wrote 3, or with different elements"
    ]
    assert _list_problems("valkey-0", lists, {"vf:writes:list:0": ["list", ["a", "b", "x"]]})
    assert _list_problems("valkey-0", lists, {"vf:writes:list:0": ["string", "abc"]}) == [
        "valkey-0: vf:writes:list:0 reloaded as a string, not the list the workload wrote"
    ]


@pytest.mark.parametrize(
    ("config", "mode", "message"),
    [
        ({"mix": {"rename": 1}}, "cluster", "CROSSSLOT"),
        ({"mix": {"rename": 1}, "mutable_keys": 1}, "standalone", "at least 2 mutable_keys"),
        ({"mix": {"lrange": 1}}, "standalone", "set list_keys above 0"),
        ({"mix": {"lindex": 1}}, "standalone", "set list_keys above 0"),
        (
            {"mix": {"set": 0, "lindex": 0}},
            "standalone",
            "positive weight for one of set, del, get, rename, lrange, lindex",
        ),
    ],
)
def test_a_mix_the_deployment_or_the_lists_cannot_serve_is_rejected(config, mode, message):
    resolved = ConfigSchema(_WORKLOAD_FIELDS).resolve(config, "config")
    plan = SimpleNamespace(deployment_mode=mode, deployment={"replicas_per_primary": 0})
    with pytest.raises(ConfigError, match=message):
        _validate_workload(resolved, SimpleNamespace(id="writes"), plan)
    resolved = ConfigSchema(_WORKLOAD_FIELDS).resolve({"mix": {"rename": 1, "lrange": 1}, "list_keys": 1}, "config")
    _validate_workload(resolved, SimpleNamespace(id="writes"), SimpleNamespace(deployment_mode="standalone"))
