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
    STOP_WORDS,
    Documents,
    QueryGenerator,
    SearchResultsOracle,
    edit_distance,
    judge_filter,
    judge_knn,
    knn_text,
    render,
    text_words,
)
from valkey_fuzzer.results import ComponentStatus

CONFIG = {
    "tag_fields": ["tag"],
    "numeric_fields": ["n"],
    "text_fields": ["t", "u"],
    "suffix_fields": ["t"],
    "vector_field": "v",
    "dim": 2,
}


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


def test_text_splits_at_whitespace_and_punctuation_folds_case_and_drops_stop_words_from_the_positions():
    assert text_words("Hello,World  alpha-beta\tGAMMA/x1;the") == ("hello", "world", "alpha", "beta", "gamma", "x1")
    assert text_words("Alpha of a beta AND The gamma") == ("alpha", "beta", "gamma"), "the is a stop word too"
    assert text_words("...") == ()


def test_the_edit_distance_counts_a_swap_of_adjacent_characters_once_but_never_edits_a_substring_twice():
    assert edit_distance("alpha", "alpha") == 0
    assert edit_distance("alpha", "lapha") == 1
    assert edit_distance("alpha", "alp") == 2
    assert edit_distance("ca", "abc") == 3, "optimal string alignment, not unrestricted Damerau-Levenshtein (2)"


def test_text_matchers_find_words_in_one_field_or_in_every_one_and_a_phrase_only_within_a_field():
    documents = _documents(
        {
            "a": {"t": "alpha beta gamma", "u": "delta"},
            "b": {"t": "beta of alpha", "u": "gamma"},
            "c": {"t": "alphabet", "u": "alpha"},
            "d": {"tag": "x"},
        }
    )

    assert documents.matches(("text", "term", "t", "alpha")) == {"a", "b"}
    assert documents.matches(("text", "term", None, "alpha")) == {"a", "b", "c"}
    assert documents.matches(("text", "prefix", "t", "alph")) == {"a", "b", "c"}
    assert documents.matches(("text", "suffix", None, "ta")) == {"a", "b"}
    assert documents.matches(("text", "suffix", None, "pha")) == {"a", "b"}, "u has no suffix trie"
    assert documents.matches(("text", "suffix", "u", "pha")) == {"c"}
    assert documents.matches(("text", "fuzzy", "t", ("alpah", 1))) == {"a", "b"}
    assert documents.matches(("text", "fuzzy", "t", ("alpah", 3))) == {"a", "b"}, "alphabet is 4 edits away"
    assert documents.matches(("text", "fuzzy", None, ("alpah", 1))) == {"a", "b", "c"}
    assert documents.matches(("text", "phrase", None, ("beta", "alpha"))) == {"b"}, "the stop word takes no position"
    assert documents.matches(("text", "phrase", None, ("delta", "gamma"))) == set(), "not across fields"
    assert documents.matches(("not", ("text", "term", "u", "alpha"))) == {"a", "b", "d"}


def test_a_text_matcher_renders_in_its_own_spelling_with_one_percent_sign_per_edit():
    assert render(("text", "term", "t", "alpha", "Alpha")) == "@t:Alpha"
    assert render(("text", "prefix", None, "al", "AL")) == "AL*"
    assert render(("text", "suffix", "u", "ha", "ha")) == "@u:*ha"
    assert render(("text", "fuzzy", "t", ("alpah", 2), "alpah")) == "@t:%%alpah%%"
    assert render(("text", "phrase", None, ("a1", "b2"), ("A1", "b2"))) == '"A1 b2"'


def test_a_hash_the_oracle_cannot_evaluate_as_valkey_search_would_is_not_judged():
    for fields in ({"n": "many"}, {"n": "nan"}, {"v": b"\x00" * 3}, {"t": "caf\u00e9"}, {"t": "a\\,b"}):
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


def test_generated_text_matchers_draw_every_operator_from_the_words_the_hashes_hold():
    vocabulary = ["alpha", "alpine", "beta", "gamma", "x12"]
    documents = _documents(
        {f"k{i}": {"t": " ".join(vocabulary[(i + j) % 5] for j in range(4)), "u": vocabulary[i % 5]} for i in range(10)}
    )
    generator = QueryGenerator(random.Random(3), documents)

    matchers = [generator._text() for _ in range(400)]

    assert {matcher[1] for matcher in matchers} == {"term", "prefix", "suffix", "fuzzy", "phrase"}
    assert {matcher[2] for matcher in matchers} == {"t", "u", None}
    assert {matcher[2] for matcher in matchers if matcher[1] == "suffix"} == {"t", None}, "u has no suffix trie"
    texts = [render(matcher) for matcher in matchers]
    assert any(text != text.lower() for text in texts), "some words are spelled in another case"
    for matcher, text in zip(matchers, texts):
        if matcher[1] == "fuzzy":
            assert 1 <= matcher[3][1] <= 3 and matcher[3][0]
            assert re.fullmatch(r"(@\w+:)?(%+)[^%]+\2", text), text
        elif matcher[1] == "phrase":
            assert text.endswith('"') and len(matcher[3]) in (2, 3)
        elif matcher[1] == "term":
            assert matcher[3] not in STOP_WORDS
    phrases = [matcher for matcher in matchers if matcher[1] == "phrase"]
    assert any(documents.matches(matcher) for matcher in phrases), "phrases are drawn from the words held"


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


def test_with_text_fields_every_query_is_verbatim_so_a_term_matches_only_the_word_itself(monkeypatch):
    _tag_only(monkeypatch)
    handle = FakeHandle(instances=("valkey-0",))
    sent = []

    class Recording(SearchClient):
        def execute_command(self, *args):
            sent.append(args)
            return super().execute_command(*args)

    handle.clients = {"valkey-0": Recording(HASHES)}

    assert _oracle(handle, text_fields=["t"]).status == ComponentStatus.PASS
    assert sent and all(args[-1] == "VERBATIM" for args in sent)
    sent.clear()
    assert _oracle(handle).status == ComponentStatus.PASS
    assert sent and all("VERBATIM" not in args for args in sent)


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


def test_a_batches_knn_query_filters_on_no_text_while_its_inline_filter_skips_text_in_an_and():
    documents = _documents(
        {f"k{i}": {"tag": f"t{i % 3}", "t": f"alpha w{i % 4}", "v": _vector(i, -i)} for i in range(20)}
    )
    generator = QueryGenerator(random.Random(5), documents)

    knns = [query for query in (generator.knn() for _ in range(300)) if query["filter"] is not None]

    assert all("'text'" not in repr(q["filter"]) for q in knns if q["policy"] == "BATCHES"), (
        "valkey-io/valkey-search#1424"
    )
    assert any("'text'" in repr(q["filter"]) for q in knns if q["policy"] != "BATCHES")


def test_an_and_holds_at_most_one_exact_phrase_and_holds_it_last():
    # valkey-search adds what an AND puts after a phrase to the phrase itself (fuzzer#116).
    documents = _documents({f"k{i}": {"tag": f"t{i % 3}", "t": f"alpha beta w{i % 4} gamma"} for i in range(20)})
    generator = QueryGenerator(random.Random(11), documents)

    def ands(query):
        if query[0] in ("and", "or"):
            if query[0] == "and":
                yield query
            for child in query[1]:
                yield from ands(child)
        elif query[0] == "not":
            yield from ands(query[1])

    found = [node for _ in range(400) for node in ands(generator.filter())]
    with_phrase = [node for node in found if any(child[1:2] == ("phrase",) for child in node[1])]
    assert with_phrase, "phrases are drawn into ANDs"
    for node in with_phrase:
        phrases = [index for index, child in enumerate(node[1]) if child[1:2] == ("phrase",)]
        assert phrases == [len(node[1]) - 1], render(node)


def test_in_an_or_an_and_of_a_tag_or_numeric_matcher_holds_no_phrase():
    # valkey-search aborts on an AND around an OR of such branches (fuzzer#118).
    documents = _documents(
        {f"k{i}": {"tag": f"t{i % 3}", "n": str(i), "t": f"alpha beta w{i % 4} gamma"} for i in range(20)}
    )
    generator = QueryGenerator(random.Random(13), documents)

    def offending(query, under_or=False):
        kind = query[0]
        if kind == "not":
            return offending(query[1], under_or)
        if kind not in ("and", "or"):
            return False
        children = query[1]
        if (
            kind == "and"
            and under_or
            and any(child[0] in ("tag", "numeric") for child in children)
            and any(child[1:2] == ("phrase",) for child in children)
        ):
            return True
        return any(offending(child, under_or or kind == "or") for child in children)

    queries = [generator.filter() for _ in range(600)]
    assert not [render(query) for query in queries if offending(query)]
    assert any('"' in render(query) and " | " in render(query) for query in queries), "phrases still meet ORs"
