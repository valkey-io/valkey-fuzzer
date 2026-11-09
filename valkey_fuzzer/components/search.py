"""``valkey.oracle.search-results``: valkey-search's query results against the hashes they index.

Each instance's hashes under the index's key prefix are read back, and random queries, drawn from the run's seed,
are answered twice: by ``FT.SEARCH`` on that instance, and by evaluating the query over the hashes in the
client. The queries combine tag and numeric matchers with AND, OR and negation, and some are KNN queries of a
``FLAT`` vector field, whose search is exact, with each hybrid policy. An instance passes when every answer is
the client's; its index is built in the background, so the comparison is retried until it agrees or the time
runs out. The query language is valkey-search's ``docs/topics/search-query.md``.
"""

import math
import random
import struct
from collections.abc import Sequence
from typing import Any, Optional

import valkey

from valkey_fuzzer.declarations import Declarations
from valkey_fuzzer.plan import ComponentSpec
from valkey_fuzzer.results import ComponentStatus, Evidence, make_evidence, make_result
from valkey_fuzzer.schema import ConfigField

from .instances import deployment, stopped_on_purpose, unreachable_instances
from .polling import poll_each
from .registration import component_registration
from .schemas import INTEGER, MAPPING, TEXT, declared, mapping_of

# A result page larger than any index the oracle reads: FT.SEARCH returns 10 keys by default.
_PAGE = 100_000
# How much a FLAT distance computed in float32 by the module may differ from the client's in float64.
_DISTANCE_TOLERANCE = 1e-4


class _UnmodeledError(Exception):
    """A hash whose field the oracle cannot evaluate as valkey-search would, so no answer can be judged."""


class Documents:
    """The hashes under an index's prefix, as the oracle evaluates queries over them: each key's tags by tag
    field (lowercased, as tags compare without CASESENSITIVE), numbers by numeric field, and vector."""

    def __init__(self, hashes: dict[str, dict[bytes, bytes]], config: dict):
        self.keys = frozenset(hashes)
        self.tags: dict[str, dict[str, frozenset]] = {field: {} for field in config["tag_fields"]}
        self.numbers: dict[str, dict[str, float]] = {field: {} for field in config["numeric_fields"]}
        self.vectors: dict[str, tuple[float, ...]] = {}
        vector_field, dim = config["vector_field"], config["dim"]
        for key, fields in hashes.items():
            for field in self.tags:
                value = fields.get(field.encode())
                if value is not None:
                    words = (word.strip().lower() for word in value.decode("utf-8", "replace").split(","))
                    self.tags[field][key] = frozenset(word for word in words if word)
            for field in self.numbers:
                value = fields.get(field.encode())
                if value is not None:
                    try:
                        number = float(value)
                    except ValueError:
                        raise _UnmodeledError(f"{key!r} field {field!r} is not a number: {value!r}") from None
                    if math.isnan(number):
                        raise _UnmodeledError(f"{key!r} field {field!r} is NaN")
                    self.numbers[field][key] = number
            if vector_field is not None:
                value = fields.get(vector_field.encode())
                if value is not None:
                    if len(value) != 4 * dim:
                        raise _UnmodeledError(f"{key!r} field {vector_field!r} holds {len(value)} bytes, not {4 * dim}")
                    self.vectors[key] = struct.unpack(f"<{dim}f", value)

    def matches(self, query: tuple) -> frozenset:
        """The keys ``query`` selects: a negation selects the keys without the field too."""

        kind = query[0]
        if kind == "tag":
            _, field, words = query
            wanted = [word.lower() for word in words]
            return frozenset(
                key
                for key, tags in self.tags[field].items()
                if any(tag.startswith(w[:-1]) if w.endswith("*") else tag == w for w in wanted for tag in tags)
            )
        if kind == "numeric":
            _, field, low, high = query
            return frozenset(key for key, number in self.numbers[field].items() if _within(number, low, high))
        if kind == "and":
            return frozenset.intersection(*(self.matches(part) for part in query[1]))
        if kind == "or":
            return frozenset.union(*(self.matches(part) for part in query[1]))
        return self.keys - self.matches(query[1])

    def distances(self, candidates: frozenset, vector: Sequence[float]) -> dict[str, float]:
        """The squared Euclidean (L2) distance of every candidate with a vector to ``vector``."""

        return {
            key: sum((a - b) ** 2 for a, b in zip(self.vectors[key], vector))
            for key in candidates
            if key in self.vectors
        }


def _within(number: float, low: tuple[bool, float], high: tuple[bool, float]) -> bool:
    (low_open, low_bound), (high_open, high_bound) = low, high
    above = number > low_bound if low_open else number >= low_bound
    below = number < high_bound if high_open else number <= high_bound
    return above and below


def render(query: tuple) -> str:
    """``query`` in valkey-search's query language; a combination inside another is parenthesized."""

    def part(inner: tuple) -> str:
        text = render(inner)
        return f"({text})" if inner[0] in ("and", "or") else text

    kind = query[0]
    if kind == "tag":
        return f"@{query[1]}:{{{' | '.join(query[2])}}}"
    if kind == "numeric":
        (low_open, low), (high_open, high) = query[2], query[3]
        return f"@{query[1]}:[{'(' if low_open else ''}{_number(low)} {'(' if high_open else ''}{_number(high)}]"
    if kind == "and":
        return " ".join(part(inner) for inner in query[1])
    if kind == "or":
        return " | ".join(part(inner) for inner in query[1])
    # valkey-search's parser reads one '-' before a term, so a negation of a negation is parenthesized.
    inner = query[1]
    return f"-({render(inner)})" if inner[0] == "not" else f"-{part(inner)}"


def _number(value: float) -> str:
    return "+inf" if value == math.inf else "-inf" if value == -math.inf else repr(value)


class QueryGenerator:
    """Random filter expressions over the values ``documents`` holds, and KNN queries of its vectors."""

    def __init__(self, rng: random.Random, documents: Documents):
        self.rng = rng
        self.words = {
            field: sorted({tag for tags in held.values() for tag in tags}) for field, held in documents.tags.items()
        }
        self.numbers = {field: sorted(set(held.values())) for field, held in documents.numbers.items()}
        self.vectors = [documents.vectors[key] for key in sorted(documents.vectors)]

    def filter(self, depth: int = 3) -> tuple:
        rng = self.rng
        if depth == 0 or rng.random() < 0.4:
            return self._matcher()
        kind = rng.choice(("and", "or", "not"))
        if kind == "not":
            return ("not", self.filter(depth - 1))
        return (kind, tuple(self.filter(depth - 1) for _ in range(rng.randint(2, 3))))

    def _matcher(self) -> tuple:
        rng = self.rng
        kinds = [kind for kind, fields in (("tag", self.words), ("numeric", self.numbers)) if fields]
        if rng.choice(kinds) == "tag":
            field = rng.choice(sorted(self.words))
            # A value no hash holds, beside the ones they do, and now and then a prefix of one: of at least two
            # characters, as valkey-search's search.tag-min-prefix-length defaults to.
            pool = self.words[field] + ["absent0"]
            words = [rng.choice(pool) for _ in range(rng.randint(1, 3))]
            words = [
                word[: rng.randint(2, len(word))] + "*" if len(word) >= 2 and rng.random() < 0.2 else word
                for word in words
            ]
            return ("tag", field, tuple(words))
        field = rng.choice(sorted(self.numbers))
        held = self.numbers[field] or [0.0]

        def bound(infinite: float) -> tuple[bool, float]:
            if rng.random() < 0.15:
                return False, infinite
            # repr, which renders the bound, round-trips the float exactly.
            return rng.random() < 0.3, rng.choice(held) + rng.choice((0.0, 0.0, 0.5, -0.5))

        low, high = sorted((bound(-math.inf), bound(math.inf)), key=lambda item: item[1])
        if low[1] == high[1]:
            # valkey-search rejects a range that is empty by its bounds, such as [1 (1].
            low, high = (False, low[1]), (False, high[1])
        return ("numeric", field, low, high)

    def knn(self) -> Optional[dict]:
        """A KNN query of the vector field, filtered or over every key, or None without vectors."""

        if not self.vectors:
            return None
        rng = self.rng
        base = rng.choice(self.vectors)
        vector = tuple(struct.unpack("<f", struct.pack("<f", x + rng.uniform(-2, 2)))[0] for x in base)
        query = {"k": rng.randint(1, 20), "vector": vector, "filter": None, "policy": None}
        if rng.random() < 0.7:
            query["filter"] = self.filter(2)
            query["policy"] = rng.choice((None, "ADHOC_BF", "BATCHES"))
        return query


def knn_text(query: dict, field: str) -> str:
    policy = f" HYBRID_POLICY {query['policy']}" if query["policy"] else ""
    head = "*" if query["filter"] is None else f"({render(query['filter'])})"
    return f"{head}=>[KNN {query['k']} @{field} $q{policy}]"


def _keys(reply: Any) -> tuple[int, list[str]]:
    """``(total, keys)`` of a NOCONTENT reply ``[total, key, ...]``."""

    return int(reply[0]), [key.decode() if isinstance(key, bytes) else str(key) for key in reply[1:]]


def judge_filter(documents: Documents, query: tuple, reply: Any) -> Optional[dict]:
    """A filter query's mismatch, or None: the total and the keys are exactly the client's."""

    expected = documents.matches(query)
    total, keys = _keys(reply)
    if total == len(expected) and set(keys) == expected and len(keys) == len(expected):
        return None
    return {
        "expected": len(expected),
        "total": total,
        "missing": sorted(expected - set(keys))[:10],
        "unexpected": sorted(set(keys) - expected)[:10],
    }


def judge_knn(documents: Documents, query: dict, reply: Any) -> Optional[dict]:
    """A KNN query's mismatch, or None: it returns min(K, candidates) distinct candidates, none farther than
    the Kth nearest (ties may return either key)."""

    candidates = documents.keys if query["filter"] is None else documents.matches(query["filter"])
    distances = documents.distances(candidates, query["vector"])
    nearest = sorted(distances.values())
    want = min(query["k"], len(nearest))
    _, keys = _keys(reply)
    problems = []
    if len(keys) != want or len(set(keys)) != len(keys):
        problems.append(f"returned {len(keys)} key(s), {len(set(keys))} distinct, of {want}")
    outside = sorted(set(keys) - set(distances))
    if outside:
        problems.append(f"returned keys that are not candidates: {outside[:10]}")
    if want:
        limit = nearest[want - 1]
        farther = sorted(
            key for key in set(keys) & set(distances) if distances[key] > limit + _DISTANCE_TOLERANCE * (1 + limit)
        )
        if farther:
            problems.append(f"returned keys farther than the Kth nearest ({limit:.6g}): {farther[:10]}")
    return {"problems": problems, "candidates": len(distances)} if problems else None


_SEARCH_FIELDS = {
    "index": TEXT,
    # The index's key prefix: the hashes it indexes are the ones whose names start with it.
    "prefix": TEXT,
    "tag_fields": ConfigField(list, default=list, items=TEXT),
    "numeric_fields": ConfigField(list, default=list, items=TEXT),
    # A FLAT, FLOAT32, L2 vector field of ``dim`` dimensions, whose KNN search is exact.
    "vector_field": ConfigField(str, allow_none=True, default=None),
    "dim": ConfigField(int, default=0, minimum=0, maximum=4096),
    "queries": ConfigField(int, default=200, minimum=1, maximum=10_000),
    "knn_share": ConfigField(float, default=0.3, minimum=0, maximum=1),
    "converge_timeout_s": ConfigField(float, default=60.0, minimum=0, maximum=600),
}


class SearchResultsOracle:
    """Judge every reachable instance's ``FT.SEARCH`` answers against its own hashes (see the module)."""

    def evaluate(self, context: Any, spec: ComponentSpec, evidence: Sequence[Evidence]):
        config = spec.config
        handle = deployment(context)
        stopped = stopped_on_purpose(handle, evidence)
        unreachable = unreachable_instances(handle, stopped)
        if unreachable:
            return make_result(spec, ComponentStatus.ERROR, f"unreachable instance(s): {', '.join(unreachable)}")
        instances = [instance.instance_id for instance in handle.instances if instance.instance_id not in stopped]
        queries: list = []

        def check(instance_id: str) -> tuple[bool, dict[str, Any]]:
            try:
                documents = Documents(self._hashes(handle, instance_id, config["prefix"]), config)
            except _UnmodeledError as exc:
                return True, {"unmodeled": str(exc)}
            except (valkey.exceptions.ValkeyError, OSError) as exc:
                return False, {"error": f"{type(exc).__name__}: {exc}"}
            if not queries:
                # One set of queries for every instance, drawn from the first one's values.
                queries.extend(self._queries(context.rng(spec.id, "queries"), documents, config))
            try:
                mismatches = self._mismatches(handle, instance_id, documents, queries, config)
            except (valkey.exceptions.ConnectionError, valkey.exceptions.TimeoutError, OSError) as exc:
                return False, {"error": f"{type(exc).__name__}: {exc}"}
            return not mismatches, {"keys": len(documents.keys), "mismatches": mismatches}

        polled = poll_each(context, instances, check, timeout_s=config["converge_timeout_s"], interval_s=1.0)
        report = {instance_id: value for instance_id, (_, value) in polled.items()}
        unmodeled = [
            f"{instance_id}: {value['unmodeled']}" for instance_id, value in report.items() if "unmodeled" in value
        ]
        errors = [f"{instance_id}: {value['error']}" for instance_id, value in report.items() if "error" in value]
        wrong = {instance_id: value["mismatches"] for instance_id, value in report.items() if value.get("mismatches")}
        record = {"index": config["index"], "queries": len(queries), "instances": report}
        if wrong:
            first = next(iter(wrong.items()))
            return make_result(
                spec,
                ComponentStatus.FAIL,
                f"{sum(len(found) for found in wrong.values())} FT.SEARCH answer(s) on {', '.join(sorted(wrong))} "
                f"differ from the hashes they index, first on {first[0]}: {first[1][0]['query']!r}",
                diagnostics=record,
            )
        if errors or unmodeled:
            return make_result(
                spec,
                ComponentStatus.ERROR,
                f"cannot judge the answers: {'; '.join(errors + unmodeled)}",
                diagnostics=record,
            )
        return make_result(
            spec,
            ComponentStatus.PASS,
            f"{len(queries)} quer(ies) of {config['index']} answered as the hashes say on {len(instances)} instance(s)",
            evidence=(make_evidence(spec, "valkey.search.results-verified", "oracle.completed", record),),
        )

    @staticmethod
    def _hashes(handle: Any, instance_id: str, prefix: str) -> dict[str, dict[bytes, bytes]]:
        with handle.new_client(instance_id, timeout=10.0, decode_responses=False) as client:
            hashes = {}
            for key in client.scan_iter(match=prefix.encode() + b"*", count=1000):
                fields = client.hgetall(key)
                if fields:
                    hashes[key.decode("utf-8", "replace")] = fields
            return hashes

    @staticmethod
    def _queries(rng: random.Random, documents: Documents, config: dict) -> list:
        generator = QueryGenerator(rng, documents)
        found = []
        for _ in range(config["queries"]):
            knn = generator.knn() if config["vector_field"] and rng.random() < config["knn_share"] else None
            found.append(("knn", knn) if knn else ("filter", generator.filter()))
        return found

    @staticmethod
    def _mismatches(handle: Any, instance_id: str, documents: Documents, queries: list, config: dict) -> list[dict]:
        found = []
        with handle.new_client(instance_id, timeout=10.0, decode_responses=False) as client:
            for kind, query in queries:
                if kind == "knn":
                    text = knn_text(query, config["vector_field"])
                    params = ("PARAMS", "2", "q", struct.pack(f"<{len(query['vector'])}f", *query["vector"]))
                else:
                    text, params = render(query), ()
                try:
                    reply = client.execute_command(
                        "FT.SEARCH",
                        config["index"],
                        text,
                        *params,
                        "NOCONTENT",
                        "LIMIT",
                        "0",
                        str(_PAGE),
                        "DIALECT",
                        "2",
                    )
                except valkey.exceptions.ResponseError as exc:
                    found.append({"query": text, "rejected": str(exc)})
                    continue
                mismatch = (
                    judge_knn(documents, query, reply) if kind == "knn" else judge_filter(documents, query, reply)
                )
                if mismatch is not None:
                    entry = {"query": text, **mismatch}
                    if kind == "knn":
                        entry["vector"] = list(query["vector"])
                    found.append(entry)
        return found


_DECLARES = Declarations(
    evidence={
        "valkey.search.results-verified": declared(
            mapping_of({"index": TEXT, "queries": INTEGER, "instances": MAPPING})
        )
    }
)

REGISTRATIONS = (
    component_registration(
        "valkey.oracle.search-results",
        SearchResultsOracle,
        _SEARCH_FIELDS,
        deployment_modes=("standalone",),
        declares=_DECLARES,
    ),
)
