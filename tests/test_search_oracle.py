"""``valkey.oracle.search-results`` without Valkey processes: how it evaluates valkey-search's query language over
hashes, how it judges KNN answers, and how it judges an instance whose index answers from other data."""

import math
import random
import re
import struct

import valkey

from tests.support import VirtualClock
from tests.test_valkey_components import FakeHandle, _spec
from tests.valkey_experiments_support import fake_context
from valkey_fuzzer.components import polling
from valkey_fuzzer.components.search import (
    Documents,
    QueryGenerator,
    SearchResultsOracle,
    judge_filter,
    judge_knn,
    knn_text,
    render,
)
from valkey_fuzzer.results import ComponentStatus

CONFIG = {"tag_fields": ["tag"], "numeric_fields": ["n"], "vector_field": "v", "dim": 2}


def _vector(*values):
    return struct.pack(f"<{len(values)}f", *values)


def _documents(hashes):
    encoded = {
        key: {name.encode(): value if isinstance(value, bytes) else value.encode() for name, value in fields.items()}
        for key, fields in hashes.items()
    }
    return Documents(encoded, CONFIG)


def test_a_tag_matches_case_insensitively_any_of_the_comma_separated_tags_and_a_trailing_star_is_a_prefix():
    documents = _documents({"a": {"tag": "Blue, green"}, "b": {"tag": "black"}, "c": {"tag": "beige,GREEN"}, "d": {}})

    assert documents.matches(("tag", "tag", ("green",))) == {"a", "c"}
    assert documents.matches(("tag", "tag", ("BLUE", "black"))) == {"a", "b"}
    assert documents.matches(("tag", "tag", ("b*",))) == {"a", "b", "c"}
    assert documents.matches(("tag", "tag", ("absent0",))) == set()


def test_numeric_bounds_are_inclusive_unless_open_and_may_be_infinite():
    documents = _documents({"a": {"n": "1"}, "b": {"n": "2.5"}, "c": {"n": "-3"}, "d": {"tag": "x"}})

    assert documents.matches(("numeric", "n", (False, 1.0), (False, 2.5))) == {"a", "b"}
    assert documents.matches(("numeric", "n", (True, 1.0), (False, 2.5))) == {"b"}
    assert documents.matches(("numeric", "n", (False, -math.inf), (True, 1.0))) == {"c"}


def test_a_negation_selects_the_keys_without_the_field_too_and_and_or_combine_the_sets():
    documents = _documents({"a": {"tag": "x", "n": "1"}, "b": {"tag": "y", "n": "2"}, "c": {"n": "3"}})
    x = ("tag", "tag", ("x",))
    two_up = ("numeric", "n", (False, 2.0), (False, math.inf))

    assert documents.matches(("not", x)) == {"b", "c"}
    assert documents.matches(("and", (("not", x), two_up))) == {"b", "c"}
    assert documents.matches(("or", (x, ("numeric", "n", (False, 3.0), (False, 3.0))))) == {"a", "c"}


def test_a_combination_inside_another_is_parenthesized_and_bounds_render_as_the_query_language_writes_them():
    query = (
        "and",
        (
            ("tag", "tag", ("a", "b*")),
            ("not", ("or", (("numeric", "n", (True, 1.5), (False, math.inf)), ("tag", "tag", ("c",))))),
        ),
    )

    assert render(query) == "@tag:{a | b*} -(@n:[(1.5 +inf] | @tag:{c})"
    assert render(("not", ("not", ("tag", "tag", ("a",))))) == "-(-@tag:{a})", "the parser reads one '-' per term"
    knn = {"k": 3, "vector": (0.0, 0.0), "filter": ("tag", "tag", ("a",)), "policy": "ADHOC_BF"}
    assert knn_text(knn, "v") == "(@tag:{a})=>[KNN 3 @v $q HYBRID_POLICY ADHOC_BF]"
    assert knn_text({**knn, "filter": None, "policy": None}, "v") == "*=>[KNN 3 @v $q]"


def test_a_hash_the_oracle_cannot_evaluate_as_valkey_search_would_is_not_judged():
    for fields in ({"n": "many"}, {"n": "nan"}, {"v": b"\x00" * 3}):
        try:
            _documents({"a": fields})
        except Exception as exc:
            assert type(exc).__name__ == "_UnmodeledError"
        else:
            raise AssertionError(f"{fields} was modeled")


def test_a_knn_answer_may_break_ties_either_way_but_not_return_a_farther_key_or_the_wrong_count():
    documents = _documents(
        {"a": {"v": _vector(1, 0)}, "b": {"v": _vector(0, 1)}, "c": {"v": _vector(3, 0)}, "d": {"tag": "x"}}
    )
    query = {"k": 1, "vector": (0.0, 0.0), "filter": None, "policy": None}

    assert judge_knn(documents, query, [1, b"a"]) is None
    assert judge_knn(documents, query, [1, b"b"]) is None, "a and b are both at distance 1"
    assert "farther" in judge_knn(documents, query, [1, b"c"])["problems"][0]
    assert "returned 2 key(s)" in judge_knn(documents, query, [2, b"a", b"b"])["problems"][0]
    assert "not candidates" in judge_knn(documents, query, [1, b"d"])["problems"][0]
    assert judge_knn(documents, {**query, "k": 10}, [3, b"c", b"a", b"b"]) is None, "K beyond the candidates"


def test_a_filter_answer_must_hold_exactly_the_matching_keys_and_their_total():
    documents = _documents({"a": {"tag": "x"}, "b": {"tag": "x"}, "c": {"tag": "y"}})
    query = ("tag", "tag", ("x",))

    assert judge_filter(documents, query, [2, b"b", b"a"]) is None
    assert judge_filter(documents, query, [2, b"a"]) == {"expected": 2, "total": 2, "missing": ["b"], "unexpected": []}
    assert judge_filter(documents, query, [3, b"a", b"b", b"c"])["unexpected"] == ["c"]


def test_generated_queries_use_the_values_the_hashes_hold_and_render_in_the_query_language():
    documents = _documents({f"k{i}": {"tag": f"t{i % 3}", "n": str(i), "v": _vector(i, -i)} for i in range(20)})
    generator = QueryGenerator(random.Random(7), documents)

    filters = [generator.filter() for _ in range(200)]
    texts = [render(query) for query in filters]
    assert (
        any(" | " in text for text in texts)
        and any("-" in text for text in texts)
        and any("(" in text for text in texts)
    )
    assert any("@tag:{t" in text for text in texts) and any("@n:[" in text for text in texts)
    assert "--" not in " ".join(texts)
    ranges = re.findall(r"\[(\(?)(\S+) (\(?)(\S+)\]", " ".join(texts))
    assert ranges and all(low != high or not (low_open or high_open) for low_open, low, high_open, high in ranges), (
        "valkey-search rejects an empty range"
    )
    prefixes = re.findall(r"([^{|\s]+)\*", " ".join(texts))
    assert prefixes and min(len(stem) for stem in prefixes) >= 2, "valkey-search rejects a shorter prefix"
    for query in filters:
        documents.matches(query)
    knns = [generator.knn() for _ in range(50)]
    assert {query["policy"] for query in knns} == {None, "ADHOC_BF", "BATCHES"}


class SearchClient:
    """An instance's hashes, and an index that answers FT.SEARCH from ``indexed`` (the hashes by default): an
    index that lags or holds other data answers from other hashes than SCAN reads."""

    def __init__(self, hashes, indexed=None):
        self.hashes = {
            key.encode(): {name.encode(): value.encode() for name, value in fields.items()}
            for key, fields in hashes.items()
        }
        self.indexed = indexed
        self.searches = 0

    def scan_iter(self, match=None, count=1000):
        prefix = match.rstrip(b"*")
        return iter(sorted(key for key in self.hashes if key.startswith(prefix)))

    def hgetall(self, key):
        return self.hashes.get(key, {})

    def execute_command(self, *args):
        assert args[0] == "FT.SEARCH" and args[2:3] != ("",)
        self.searches += 1
        source = self.indexed(self.searches) if callable(self.indexed) else self.indexed
        documents = Documents(source or self.hashes_by_name(), CONFIG)
        text = args[2]
        if "=>[KNN" in text:
            raise valkey.exceptions.ResponseError("this fake answers filter queries only")
        query = _parse(text)
        keys = sorted(documents.matches(query))
        return [len(keys), *(key.encode() for key in keys)]

    def hashes_by_name(self):
        return {key.decode(): fields for key, fields in self.hashes.items()}

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        pass


def _parse(text):
    """The single tag matchers the tests' oracle draws (``knn_share`` and depth keep them simple)."""

    field, words = text[1:].split(":{")
    return ("tag", field, tuple(word.strip() for word in words.rstrip("}").split("|")))


def _oracle(handle, **config):
    spec = _spec(
        "valkey.oracle.search-results",
        "search",
        index="idx",
        prefix="d:",
        tag_fields=["tag"],
        queries=40,
        knn_share=0,
        converge_timeout_s=config.pop("converge_timeout_s", 0),
        **config,
    )
    return SearchResultsOracle().evaluate(fake_context(handle, spec), spec, ())


def _tag_only(monkeypatch):
    """Draw only single tag matchers, which the fake index parses."""

    monkeypatch.setattr(QueryGenerator, "filter", lambda self, depth=3: self._matcher())
    monkeypatch.setattr(QueryGenerator, "_matcher", _tag_matcher)


def _tag_matcher(self):
    return ("tag", "tag", (self.rng.choice(self.words["tag"] + ["absent0"]),))


HASHES = {f"d:{i}": {"tag": f"t{i % 4}"} for i in range(12)}


def test_every_instance_whose_index_answers_from_its_own_hashes_passes(monkeypatch):
    _tag_only(monkeypatch)
    handle = FakeHandle()
    handle.clients = {"valkey-0": SearchClient(HASHES), "valkey-1": SearchClient(HASHES)}

    result = _oracle(handle)

    assert result.status == ComponentStatus.PASS, result.summary
    assert result.evidence[0].value["queries"] == 40


def test_a_replica_whose_index_holds_a_tag_its_hash_no_longer_has_fails(monkeypatch):
    _tag_only(monkeypatch)
    stale = {key.encode(): {b"tag": fields["tag"].encode()} for key, fields in HASHES.items()}
    stale[b"d:3"] = {b"tag": b"t0"}
    handle = FakeHandle()
    handle.clients = {
        "valkey-0": SearchClient(HASHES),
        "valkey-1": SearchClient(HASHES, indexed={key.decode(): fields for key, fields in stale.items()}),
    }

    result = _oracle(handle)

    assert result.status == ComponentStatus.FAIL
    assert "valkey-1" in result.summary and "valkey-0" not in result.summary
    mismatch = result.diagnostics["instances"]["valkey-1"]["mismatches"][0]
    assert list(mismatch["missing"]) == ["d:3"] or list(mismatch["unexpected"]) == ["d:3"]


def test_an_index_still_catching_up_passes_once_it_answers_as_the_hashes_say(monkeypatch):
    VirtualClock().install(monkeypatch, polling)
    _tag_only(monkeypatch)
    handle = FakeHandle(instances=("valkey-0",))
    # The first 30 searches answer from no hashes at all, as an index still backfilling.
    handle.clients = {
        "valkey-0": SearchClient(HASHES, indexed=lambda searches: {"d:none": {}} if searches <= 30 else None)
    }

    result = _oracle(handle, converge_timeout_s=30)

    assert result.status == ComponentStatus.PASS, result.summary


def test_a_rejected_query_is_a_mismatch_with_the_servers_error(monkeypatch):
    _tag_only(monkeypatch)
    handle = FakeHandle(instances=("valkey-0",))

    class Rejecting(SearchClient):
        def execute_command(self, *args):
            raise valkey.exceptions.ResponseError("Invalid filter expression")

    handle.clients = {"valkey-0": Rejecting(HASHES)}

    result = _oracle(handle)

    assert result.status == ComponentStatus.FAIL
    assert result.diagnostics["instances"]["valkey-0"]["mismatches"][0]["rejected"] == "Invalid filter expression"
