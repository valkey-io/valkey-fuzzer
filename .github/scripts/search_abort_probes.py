#!/usr/bin/env python3
"""Reproduction probes for client-triggerable process aborts in valkey-search.

Audit commit: valkey-io/valkey-search 9d1d60af542abe6264bc67e9149cd5e993fcb460
Target: a standalone Valkey server (cluster mode off) with valkey-search loaded.

Each probe builds what it needs under unique index/key names, sends the
commands that *might* make a CHECK/LOG(FATAL) condition false, and then issues
PING. If the server aborted, PING (or the preceding command) raises a
connection error; the runner prints `ABORT <name>` and exits 3. A probe whose
commands all return error replies or sane results prints `ok <name>`.

IMPORTANT HONESTY NOTE: none of this was run. By code reading, every site these
probes target is *guarded* in this commit (the parser or a validation layer
rejects the bad input before the CHECK). They are kept as cheap regression
probes and as shapes for the fuzzer: the parser-vs-evaluator boundary is where
the known bugs (#118, #77, #1424, #1599) lived, so re-exercising it is useful.
The docstring of each probe names the abort site (file:line) it aims at and the
reasoning in two lines.

Usage:
    python3 repros.py <port> [--only NAME] [--skip NAME[,NAME...]]
"""

import struct
import sys

import redis

DIM = 4


def vec(*xs):
    """Pack floats as little-endian float32 bytes (the on-wire vector format)."""
    xs = list(xs) + [0.0] * (DIM - len(xs))
    return struct.pack(f"<{DIM}f", *xs[:DIM])


BLOB = vec(0.1, 0.2, 0.3, 0.4)


def _ignore(fn):
    """Run fn, swallowing command-level error replies (ResponseError).

    A ResponseError means the server rejected the input and stayed up -- the
    guarded outcome. A ConnectionError propagates: that is an abort.
    """
    try:
        fn()
    except redis.exceptions.ResponseError:
        pass


# --------------------------------------------------------------------------
# Text-proximity / OR-AND evaluator family (the #118 neighbourhood).
# Abort site pattern: src/indexes/text/proximity.cc:187,
# src/indexes/text/orproximity.cc:112 CHECK(current_position_.has_value()),
# proximity.cc:370 / orproximity.cc:185 CHECK(current_field_mask_ != 0),
# proximity.cc:78 / term.cc:150 CHECK(current_key_).
# The evaluator assumes the parser rejected position-less proximity shapes.
# --------------------------------------------------------------------------


def text_and_or_phrase_tag(r):
    """proximity.cc:187 current_position_.has_value() -- #118 shape variant.
    AND around an OR whose branches AND an exact phrase with a tag predicate;
    the proximity iterator may be primed with no position for a tag branch."""
    r.execute_command("FT.CREATE", "a06_t1", "ON", "HASH", "PREFIX", "1", "t1:", "SCHEMA", "body", "TEXT", "cat", "TAG")
    r.hset("t1:1", mapping={"body": "alpha beta gamma", "cat": "x"})
    _ignore(
        lambda: r.execute_command(
            "FT.SEARCH", "a06_t1", '(@body:"alpha beta" @cat:{x}) | (@body:"beta gamma" @cat:{y})'
        )
    )


def text_and_or_phrase_numeric(r):
    """proximity.cc:187 current_position_.has_value() -- #118 shape variant.
    Same AND/OR nesting but with a NUMERIC predicate ANDed to each phrase."""
    r.execute_command(
        "FT.CREATE", "a06_t2", "ON", "HASH", "PREFIX", "1", "t2:", "SCHEMA", "body", "TEXT", "n", "NUMERIC"
    )
    r.hset("t2:1", mapping={"body": "alpha beta gamma", "n": "5"})
    _ignore(
        lambda: r.execute_command(
            "FT.SEARCH", "a06_t2", '(@body:"alpha beta" @n:[0 10]) | (@body:"gamma delta" @n:[0 10])'
        )
    )


def text_inorder_slop_phrase(r):
    """proximity.cc:44 CHECK(slop_ || in_order_) / 187 current_position_.
    INORDER with SLOP on a multi-term phrase inside an AND/OR; probes the
    positional-check priming path."""
    r.execute_command("FT.CREATE", "a06_t3", "ON", "HASH", "PREFIX", "1", "t3:", "SCHEMA", "body", "TEXT")
    r.hset("t3:1", mapping={"body": "one two three four"})
    _ignore(
        lambda: r.execute_command("FT.SEARCH", "a06_t3", '@body:"one two" | @body:"three four"', "INORDER", "SLOP", "2")
    )


def text_negate_with_vector(r):
    """predicate.cc:461 case 2 (text+negate universal set) / search.cc:562
    CHECK(index_schema != nullptr) for text+negate.
    A negated text term ANDed as the prefilter of a KNN query."""
    r.execute_command(
        "FT.CREATE",
        "a06_t4",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "t4:",
        "SCHEMA",
        "body",
        "TEXT",
        "v",
        "VECTOR",
        "HNSW",
        "6",
        "TYPE",
        "FLOAT32",
        "DIM",
        str(DIM),
        "DISTANCE_METRIC",
        "COSINE",
    )
    r.hset("t4:1", mapping={"body": "hello world", "v": vec(0.1, 0.2)})
    _ignore(
        lambda: r.execute_command(
            "FT.SEARCH", "a06_t4", "(-@body:nothere)=>[KNN 2 @v $B]", "PARAMS", "2", "B", BLOB, "DIALECT", "2"
        )
    )


def text_or_nontext_and_text_vector(r):
    """predicate.cc:461 case 1 (AND contains OR with non-text children and an
    AND child containing text -- 'not solved in entries fetcher yet').
    Builds exactly that: AND[ OR[ numeric , AND[text phrase, tag] ] ] => KNN."""
    r.execute_command(
        "FT.CREATE",
        "a06_t5",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "t5:",
        "SCHEMA",
        "body",
        "TEXT",
        "n",
        "NUMERIC",
        "cat",
        "TAG",
        "v",
        "VECTOR",
        "FLAT",
        "6",
        "TYPE",
        "FLOAT32",
        "DIM",
        str(DIM),
        "DISTANCE_METRIC",
        "L2",
    )
    r.hset("t5:1", mapping={"body": "alpha beta", "n": "3", "cat": "x", "v": vec(0.3, 0.1)})
    _ignore(
        lambda: r.execute_command(
            "FT.SEARCH",
            "a06_t5",
            '(@n:[0 10] | (@body:"alpha beta" @cat:{x}))=>[KNN 2 @v $B]',
            "PARAMS",
            "2",
            "B",
            BLOB,
            "DIALECT",
            "2",
        )
    )


def text_three_term_or_proximity(r):
    """orproximity.cc:112 current_position_ / 185 current_field_mask_.
    An OR of three exact phrases over two fields; the OR-proximity iterator
    unions per-branch positions that may be absent for a non-matching field."""
    r.execute_command(
        "FT.CREATE", "a06_t6", "ON", "HASH", "PREFIX", "1", "t6:", "SCHEMA", "title", "TEXT", "body", "TEXT"
    )
    r.hset("t6:1", mapping={"title": "red green", "body": "blue yellow"})
    _ignore(
        lambda: r.execute_command(
            "FT.SEARCH", "a06_t6", '@title:"red green" | @body:"blue yellow" | @title:"green blue"'
        )
    )


# --------------------------------------------------------------------------
# Suffix / prefix / fuzzy wildcard text.
# --------------------------------------------------------------------------


def suffix_query_with_knn(r):
    """text.cc:321 CHECK(suffix_tree) "Suffix estimation not supported".
    A suffix term (*word) prefiltering a KNN query drives SuffixPredicate::
    EstimateSize(is_vec_query=true), which reads the schema suffix tree."""
    r.execute_command(
        "FT.CREATE",
        "a06_s1",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "s1:",
        "SCHEMA",
        "body",
        "TEXT",
        "WITHSUFFIXTRIE",
        "NOSTEM",
        "v",
        "VECTOR",
        "HNSW",
        "6",
        "TYPE",
        "FLOAT32",
        "DIM",
        str(DIM),
        "DISTANCE_METRIC",
        "COSINE",
    )
    r.hset("s1:1", mapping={"body": "searching", "v": vec(0.2, 0.2)})
    _ignore(
        lambda: r.execute_command(
            "FT.SEARCH", "a06_s1", "(@body:*ing)=>[KNN 2 @v $B]", "PARAMS", "2", "B", BLOB, "DIALECT", "2"
        )
    )


def suffix_multifield_mask(r):
    """predicate.cc:190 CHECK(suffix.has_value()) via a mixed schema.
    One text field WITHSUFFIXTRIE, one without; an unfielded suffix query
    (*word) must only touch the suffix-enabled field's mask."""
    r.execute_command(
        "FT.CREATE",
        "a06_s2",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "s2:",
        "SCHEMA",
        "a",
        "TEXT",
        "WITHSUFFIXTRIE",
        "NOSTEM",
        "b",
        "TEXT",
        "NOSTEM",
    )
    r.hset("s2:1", mapping={"a": "running", "b": "jumping"})
    _ignore(lambda: r.execute_command("FT.SEARCH", "a06_s2", "*ing"))


def infix_query(r):
    """predicate.cc:220 InfixPredicate CHECK(false) "Infix - Not implemented".
    An infix wildcard (*word*). The parser rejects it with
    'Unsupported query operation'; this confirms it never reaches evaluation."""
    r.execute_command(
        "FT.CREATE", "a06_i1", "ON", "HASH", "PREFIX", "1", "i1:", "SCHEMA", "body", "TEXT", "WITHSUFFIXTRIE", "NOSTEM"
    )
    r.hset("i1:1", mapping={"body": "searching"})
    _ignore(lambda: r.execute_command("FT.SEARCH", "a06_i1", "*arch*"))


def fuzzy_over_multibyte(r):
    """fuzzy.h:93 CHECK(cp != kInvalidCp) "Fuzzy decoded invalid UTF-8".
    Index multibyte UTF-8 text, then a fuzzy term (%word%). If a radix edge
    boundary splits a UTF-8 sequence, DecodeAndAdvance sees an invalid cp."""
    r.execute_command(
        "FT.CREATE", "a06_f1", "ON", "HASH", "PREFIX", "1", "f1:", "SCHEMA", "body", "TEXT", "LANGUAGE", "RUSSIAN"
    )
    r.hset("f1:1", mapping={"body": "привет мир"})
    _ignore(lambda: r.execute_command("FT.SEARCH", "a06_f1", "%%привет%%"))


def fuzzy_distance_boundary(r):
    """filter_parser.cc:74 fuzzy max-distance callback / fuzzy.h decode.
    Fuzzy with the maximum allowed '%' count over a short multibyte term."""
    r.execute_command(
        "FT.CREATE", "a06_f2", "ON", "HASH", "PREFIX", "1", "f2:", "SCHEMA", "body", "TEXT", "LANGUAGE", "GERMAN"
    )
    r.hset("f2:1", mapping={"body": "straße grüße"})
    _ignore(lambda: r.execute_command("FT.SEARCH", "a06_f2", "%%%straße%%%"))


# --------------------------------------------------------------------------
# Vector range: NaN / Inf radius and epsilon (upstream #1599 neighbourhood).
# Guard: VectorBase::ValidateRangeSearchInputs (vector_base.cc:650) and
# VectorRangePredicate::GetSearchEpsilon (predicate.h:257).
# Abort site: vector_hnsw.cc:323-326 ComputeRangeShell CHECKs.
# --------------------------------------------------------------------------


def vector_range_inf_radius(r):
    """vector_hnsw.cc:324 CHECK(!IsInf(radius)). VECTOR_RANGE radius 'inf'
    passes the parser's `radius < 0` test; ValidateRangeSearchInputs should
    convert it to FLT_MAX before the shell CHECK."""
    r.execute_command(
        "FT.CREATE",
        "a06_vr1",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "vr1:",
        "SCHEMA",
        "v",
        "VECTOR",
        "HNSW",
        "6",
        "TYPE",
        "FLOAT32",
        "DIM",
        str(DIM),
        "DISTANCE_METRIC",
        "L2",
    )
    r.hset("vr1:1", mapping={"v": vec(1, 2, 3, 4)})
    _ignore(
        lambda: r.execute_command(
            "FT.SEARCH", "a06_vr1", "@v:[VECTOR_RANGE inf $B]", "PARAMS", "2", "B", BLOB, "DIALECT", "2"
        )
    )


def vector_range_nan_epsilon(r):
    """vector_hnsw.cc:325 CHECK(!IsNaN(epsilon)). $epsilon:nan is accepted by
    SimpleAtod and IsNegativeNonZero(nan)==false; GetSearchEpsilon should fold
    it to FLT_MAX / ValidateRangeSearchInputs should reject it."""
    r.execute_command(
        "FT.CREATE",
        "a06_vr2",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "vr2:",
        "SCHEMA",
        "v",
        "VECTOR",
        "HNSW",
        "6",
        "TYPE",
        "FLOAT32",
        "DIM",
        str(DIM),
        "DISTANCE_METRIC",
        "COSINE",
    )
    r.hset("vr2:1", mapping={"v": vec(0.5, 0.5)})
    _ignore(
        lambda: r.execute_command(
            "FT.SEARCH", "a06_vr2", "@v:[VECTOR_RANGE 0.5 $B]=>{$epsilon:nan}", "PARAMS", "2", "B", BLOB, "DIALECT", "2"
        )
    )


def vector_range_inf_epsilon(r):
    """vector_hnsw.cc:326 CHECK(!IsInf(epsilon)). $epsilon:inf path."""
    r.execute_command(
        "FT.CREATE",
        "a06_vr3",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "vr3:",
        "SCHEMA",
        "v",
        "VECTOR",
        "FLAT",
        "6",
        "TYPE",
        "FLOAT32",
        "DIM",
        str(DIM),
        "DISTANCE_METRIC",
        "L2",
    )
    r.hset("vr3:1", mapping={"v": vec(1, 1, 1, 1)})
    _ignore(
        lambda: r.execute_command(
            "FT.SEARCH", "a06_vr3", "@v:[VECTOR_RANGE 1.0 $B]=>{$epsilon:inf}", "PARAMS", "2", "B", BLOB, "DIALECT", "2"
        )
    )


def knn_nonfinite_vector(r):
    """vector_hnsw.cc:323-357 / scorer NaN checks. A KNN query whose query
    vector carries NaN / Inf components (raw float32 bytes)."""
    r.execute_command(
        "FT.CREATE",
        "a06_vk1",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "vk1:",
        "SCHEMA",
        "v",
        "VECTOR",
        "HNSW",
        "6",
        "TYPE",
        "FLOAT32",
        "DIM",
        str(DIM),
        "DISTANCE_METRIC",
        "COSINE",
    )
    r.hset("vk1:1", mapping={"v": vec(0.1, 0.1, 0.1, 0.1)})
    nan_blob = struct.pack("<4f", float("nan"), float("inf"), 0.0, float("-inf"))
    _ignore(
        lambda: r.execute_command(
            "FT.SEARCH", "a06_vk1", "*=>[KNN 2 @v $B]", "PARAMS", "2", "B", nan_blob, "DIALECT", "2"
        )
    )


# --------------------------------------------------------------------------
# KNN / LIMIT / result-range bookkeeping.
# Abort: search.cc:1936 CHECK(!ShouldReturnNoResults), 1943 CHECK_GT(k, offset).
# Guard: SearchCommand::SendReply checks ShouldReturnNoResults and returns.
# --------------------------------------------------------------------------


def knn_limit_offset_ge_k(r):
    """search.cc:1943 CHECK_GT(k, limit.first_index). KNN K=2 with a LIMIT
    whose offset (5) exceeds K; SendReply's no-results guard must fire first."""
    r.execute_command(
        "FT.CREATE",
        "a06_k1",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "k1:",
        "SCHEMA",
        "v",
        "VECTOR",
        "HNSW",
        "6",
        "TYPE",
        "FLOAT32",
        "DIM",
        str(DIM),
        "DISTANCE_METRIC",
        "L2",
    )
    for i in range(4):
        r.hset(f"k1:{i}", mapping={"v": vec(float(i), 0, 0, 0)})
    _ignore(
        lambda: r.execute_command(
            "FT.SEARCH", "a06_k1", "*=>[KNN 2 @v $B]", "PARAMS", "2", "B", BLOB, "LIMIT", "5", "10", "DIALECT", "2"
        )
    )


def knn_limit_number_zero(r):
    """search.cc:1936 CHECK(!ShouldReturnNoResults). LIMIT 0 0 on a KNN query
    (number==0 path of ShouldReturnNoResults)."""
    r.execute_command(
        "FT.CREATE",
        "a06_k2",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "k2:",
        "SCHEMA",
        "v",
        "VECTOR",
        "FLAT",
        "6",
        "TYPE",
        "FLOAT32",
        "DIM",
        str(DIM),
        "DISTANCE_METRIC",
        "COSINE",
    )
    r.hset("k2:1", mapping={"v": vec(0.3, 0.3)})
    _ignore(
        lambda: r.execute_command(
            "FT.SEARCH", "a06_k2", "*=>[KNN 2 @v $B]", "PARAMS", "2", "B", BLOB, "LIMIT", "0", "0", "DIALECT", "2"
        )
    )


def knn_k_huge(r):
    """search.cc:2479 To<unsigned>(k) / planner. A KNN K far beyond the index
    size and near unsigned limits; parser should clamp or error, not abort."""
    r.execute_command(
        "FT.CREATE",
        "a06_k3",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "k3:",
        "SCHEMA",
        "v",
        "VECTOR",
        "HNSW",
        "6",
        "TYPE",
        "FLOAT32",
        "DIM",
        str(DIM),
        "DISTANCE_METRIC",
        "L2",
    )
    r.hset("k3:1", mapping={"v": vec(1, 0, 0, 0)})
    _ignore(
        lambda: r.execute_command(
            "FT.SEARCH", "a06_k3", "*=>[KNN 4294967295 @v $B]", "PARAMS", "2", "B", BLOB, "DIALECT", "2"
        )
    )


# --------------------------------------------------------------------------
# Scorer selection.
# Abort: scorer.cc:57 CHECK(false) "TFIDF scorer not yet implemented".
# Guard: kScorerByStr maps only BM25STD, so SCORER TFIDF fails to parse.
# --------------------------------------------------------------------------


def scorer_tfidf(r):
    """scorer.cc:57 CHECK(false) TFIDF. SCORER TFIDF on a text query; the enum
    parser should reject the unregistered name before GetScorer is reached."""
    r.execute_command("FT.CREATE", "a06_sc1", "ON", "HASH", "PREFIX", "1", "sc1:", "SCHEMA", "body", "TEXT")
    r.hset("sc1:1", mapping={"body": "hello"})
    _ignore(lambda: r.execute_command("FT.SEARCH", "a06_sc1", "hello", "SCORER", "TFIDF", "WITHSCORES"))


def scorer_tfidf_aggregate(r):
    """scorer.cc:57 via FT.AGGREGATE SCORER TFIDF (same enum map)."""
    r.execute_command("FT.CREATE", "a06_sc2", "ON", "HASH", "PREFIX", "1", "sc2:", "SCHEMA", "body", "TEXT")
    r.hset("sc2:1", mapping={"body": "world"})
    _ignore(lambda: r.execute_command("FT.AGGREGATE", "a06_sc2", "world", "SCORER", "TFIDF", "ADDSCORES"))


# --------------------------------------------------------------------------
# FT.AGGREGATE APPLY / FILTER expression evaluator.
# Abort: expr.cc:177/192/208 arity CHECKs (guarded by min/max_argc),
# value.cc:249 GetArrayElement OOB, value.cc:281 operator<< CHECK(false).
# --------------------------------------------------------------------------


def aggregate_apply_arity(r):
    """expr.cc:177 MonadicFunctionProxy CHECK(params.size()==1). Call a
    1-arg function with 2 args; LookUpAndValidate should reject on max_argc."""
    r.execute_command("FT.CREATE", "a06_ag1", "ON", "HASH", "PREFIX", "1", "ag1:", "SCHEMA", "n", "NUMERIC")
    r.hset("ag1:1", mapping={"n": "9"})
    _ignore(lambda: r.execute_command("FT.AGGREGATE", "a06_ag1", "*", "APPLY", "sqrt(@n,@n)", "AS", "x"))


def aggregate_apply_substr_arity(r):
    """expr.cc:208 TriadicFunctionProxy CHECK(params.size()==3). substr with
    one arg; arity validation should reject before the proxy runs."""
    r.execute_command("FT.CREATE", "a06_ag2", "ON", "HASH", "PREFIX", "1", "ag2:", "SCHEMA", "s", "TEXT")
    r.hset("ag2:1", mapping={"s": "hello"})
    _ignore(
        lambda: r.execute_command("FT.AGGREGATE", "a06_ag2", "*", "LOAD", "1", "@s", "APPLY", "substr(@s)", "AS", "x")
    )


def aggregate_filter_nested(r):
    """value.cc:281 operator<<(CHECK(false)) / value.cc:249 GetArrayElement.
    A FILTER expression mixing types and a missing field, forcing the
    evaluator through Nil/Missing/Array value branches."""
    r.execute_command(
        "FT.CREATE", "a06_ag3", "ON", "HASH", "PREFIX", "1", "ag3:", "SCHEMA", "n", "NUMERIC", "s", "TEXT"
    )
    r.hset("ag3:1", mapping={"n": "3", "s": "hi"})
    _ignore(
        lambda: r.execute_command(
            "FT.AGGREGATE", "a06_ag3", "*", "LOAD", "2", "@n", "@s", "FILTER", "@missing > @n || @s == 'hi'"
        )
    )


def aggregate_groupby_reduce(r):
    """ft_aggregate_exec.cc:500/511 group/reducer size CHECKs and expr arity.
    GROUPBY with a REDUCE over a field absent from some records."""
    r.execute_command(
        "FT.CREATE", "a06_ag4", "ON", "HASH", "PREFIX", "1", "ag4:", "SCHEMA", "cat", "TAG", "n", "NUMERIC"
    )
    r.hset("ag4:1", mapping={"cat": "a", "n": "1"})
    r.hset("ag4:2", mapping={"cat": "a"})
    _ignore(
        lambda: r.execute_command(
            "FT.AGGREGATE", "a06_ag4", "*", "GROUPBY", "1", "@cat", "REDUCE", "SUM", "1", "@n", "AS", "total"
        )
    )


def aggregate_apply_parsetime(r):
    """expr.cc:271 ProxyParsetime CHECK(!params.empty()) (min_argc guards).
    parsetime with an extra arg and a non-date string."""
    r.execute_command("FT.CREATE", "a06_ag5", "ON", "HASH", "PREFIX", "1", "ag5:", "SCHEMA", "s", "TEXT")
    r.hset("ag5:1", mapping={"s": "notadate"})
    _ignore(
        lambda: r.execute_command(
            "FT.AGGREGATE", "a06_ag5", "*", "LOAD", "1", "@s", "APPLY", "parsetime(@s,'%FT%TZ')", "AS", "t"
        )
    )


# --------------------------------------------------------------------------
# FT.HYBRID fusion.
# Abort: ft_hybrid.cc:125 CHECK(alpha && beta) for LINEAR, 140 CHECK(fn) for
# COMBINE FUNCTION, 1159 CHECK(vector_index). Guard: the parser fills weights.
# --------------------------------------------------------------------------


def hybrid_linear_no_weights(r):
    """ft_hybrid.cc:125 CHECK(alpha && beta). LINEAR fusion without ALPHA/BETA;
    the parser is expected to fill both (or default them)."""
    r.execute_command(
        "FT.CREATE",
        "a06_h1",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "h1:",
        "SCHEMA",
        "title",
        "TEXT",
        "vec",
        "VECTOR",
        "HNSW",
        "6",
        "TYPE",
        "FLOAT32",
        "DIM",
        str(DIM),
        "DISTANCE_METRIC",
        "COSINE",
    )
    r.hset("h1:1", mapping={"title": "hello", "vec": vec(0.1, 0.2)})
    _ignore(
        lambda: r.execute_command(
            "FT.HYBRID",
            "a06_h1",
            "SEARCH",
            "@title:hello",
            "VSIM",
            "@vec",
            "$q",
            "KNN",
            "2",
            "K",
            "3",
            "COMBINE",
            "LINEAR",
            "PARAMS",
            "2",
            "q",
            BLOB,
        )
    )


def hybrid_combine_function(r):
    """ft_hybrid.cc:140 CHECK(fn) COMBINE FUNCTION without compiled expr.
    COMBINE FUNCTION with a user expression referencing arm score aliases."""
    r.execute_command(
        "FT.CREATE",
        "a06_h2",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "h2:",
        "SCHEMA",
        "title",
        "TEXT",
        "vec",
        "VECTOR",
        "FLAT",
        "6",
        "TYPE",
        "FLOAT32",
        "DIM",
        str(DIM),
        "DISTANCE_METRIC",
        "L2",
    )
    r.hset("h2:1", mapping={"title": "hello", "vec": vec(0.2, 0.1)})
    _ignore(
        lambda: r.execute_command(
            "FT.HYBRID",
            "a06_h2",
            "SEARCH",
            "@title:hello",
            "VSIM",
            "@vec",
            "$q",
            "KNN",
            "2",
            "K",
            "3",
            "COMBINE",
            "FUNCTION",
            "@__score + @__vector_score",
            "PARAMS",
            "2",
            "q",
            BLOB,
        )
    )


def hybrid_vsim_range(r):
    """ft_hybrid_parser.cc:1282 UnimplementedError VSIM RANGE. Confirms RANGE
    is rejected with an error reply, not an abort downstream."""
    r.execute_command(
        "FT.CREATE",
        "a06_h3",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "h3:",
        "SCHEMA",
        "title",
        "TEXT",
        "vec",
        "VECTOR",
        "HNSW",
        "6",
        "TYPE",
        "FLOAT32",
        "DIM",
        str(DIM),
        "DISTANCE_METRIC",
        "COSINE",
    )
    r.hset("h3:1", mapping={"title": "hi", "vec": vec(0.1, 0.1)})
    _ignore(
        lambda: r.execute_command(
            "FT.HYBRID",
            "a06_h3",
            "SEARCH",
            "@title:hi",
            "VSIM",
            "@vec",
            "$q",
            "RANGE",
            "5",
            "RADIUS",
            "1.0",
            "PARAMS",
            "2",
            "q",
            BLOB,
        )
    )


# --------------------------------------------------------------------------
# Lifecycle interactions: a wrong-type key under an indexed prefix, DROPINDEX
# during use, expiry, JSON-vs-HASH mismatch.
# Abort: index_schema.cc:624/747/788 CHECK, attribute_data_type JSON paths,
# keyspace_event_manager.cc:146/149 CHECK(moved_src_key_).
# --------------------------------------------------------------------------


def wrong_type_under_prefix(r):
    """index_schema.cc:788 CHECK(!all_records_deleted || !invalid_data).
    A non-HASH key (a plain string) written under the index prefix, then a
    search; indexing must skip the mistyped key, not abort."""
    r.execute_command("FT.CREATE", "a06_w1", "ON", "HASH", "PREFIX", "1", "w1:", "SCHEMA", "body", "TEXT")
    r.set("w1:str", "iamnotahash")
    r.hset("w1:1", mapping={"body": "hello"})
    _ignore(lambda: r.execute_command("FT.SEARCH", "a06_w1", "hello"))


def rename_into_prefix(r):
    """keyspace_event_manager.cc:146/149 CHECK(moved_src_key_) on RENAME.
    RENAME a hash from outside the prefix to inside it (and back)."""
    r.execute_command("FT.CREATE", "a06_w2", "ON", "HASH", "PREFIX", "1", "w2:", "SCHEMA", "body", "TEXT")
    r.hset("other_w2_src", mapping={"body": "moved text"})
    _ignore(lambda: r.rename("other_w2_src", "w2:moved"))
    _ignore(lambda: r.execute_command("FT.SEARCH", "a06_w2", "moved"))


def json_field_on_hash_index(r):
    """attribute_data_type.cc:180 JSON-not-supported / index_schema.cc:747.
    FT.CREATE ON JSON when the JSON module is absent should error cleanly; if
    present, a HASH written under a JSON index prefix must be skipped."""
    _ignore(
        lambda: r.execute_command(
            "FT.CREATE", "a06_w3", "ON", "JSON", "PREFIX", "1", "w3:", "SCHEMA", "$.name", "AS", "name", "TEXT"
        )
    )
    _ignore(lambda: r.hset("w3:1", mapping={"name": "hash-not-json"}))
    _ignore(lambda: r.execute_command("FT.SEARCH", "a06_w3", "*"))


def dropindex_then_search(r):
    """index_schema.cc:624 CHECK(vector_base) / cursor.cc CHECKs. Create, open
    a cursor, drop the index, then read the cursor / search the dropped name."""
    r.execute_command("FT.CREATE", "a06_w4", "ON", "HASH", "PREFIX", "1", "w4:", "SCHEMA", "body", "TEXT")
    r.hset("w4:1", mapping={"body": "hello"})
    _ignore(lambda: r.execute_command("FT.DROPINDEX", "a06_w4"))
    _ignore(lambda: r.execute_command("FT.SEARCH", "a06_w4", "hello"))


def ft_info_midbackfill(r):
    """index_schema.h:199/353/377 info CHECKs, index_schema.cc:1203 DCHECK.
    FT.INFO and FT._LIST immediately after creating an index over existing
    keys (backfill in flight)."""
    for i in range(50):
        r.hset(f"w5:{i}", mapping={"body": f"word{i}"})
    r.execute_command("FT.CREATE", "a06_w5", "ON", "HASH", "PREFIX", "1", "w5:", "SCHEMA", "body", "TEXT")
    _ignore(lambda: r.execute_command("FT.INFO", "a06_w5"))
    _ignore(lambda: r.execute_command("FT._LIST"))


# --------------------------------------------------------------------------
# FT.INFO scope options and FT.CREATE option edges.
# Abort: ft_info_parser.cc:114 CHECK(false), ft_create_parser.cc:644 CHECK.
# --------------------------------------------------------------------------


def ft_info_primary_standalone(r):
    """ft_info_parser.cc:114 CHECK(false) default scope. PRIMARY/CLUSTER info
    scope in a non-cluster config must return an error, not hit the default."""
    r.execute_command("FT.CREATE", "a06_n1", "ON", "HASH", "PREFIX", "1", "n1:", "SCHEMA", "body", "TEXT")
    _ignore(lambda: r.execute_command("FT.INFO", "a06_n1", "PRIMARY"))
    _ignore(lambda: r.execute_command("FT.INFO", "a06_n1", "CLUSTER"))


def multi_exec_search(r):
    """commands.cc:337/343 IsMainThread CHECKs; MULTI/EXEC path. FT.SEARCH
    inside MULTI/EXEC (CME dispatch constraint)."""
    r.execute_command("FT.CREATE", "a06_n2", "ON", "HASH", "PREFIX", "1", "n2:", "SCHEMA", "body", "TEXT")
    r.hset("n2:1", mapping={"body": "hello"})

    def run():
        p = r.pipeline(transaction=True)
        p.execute_command("FT.SEARCH", "a06_n2", "hello")
        p.execute()

    _ignore(run)


# --------------------------------------------------------------------------
# OR-proximity family (variant analysis of #118): the OR-proximity iterator has
# the same CHECK(current_position_) as proximity.cc:187. Two controls reduce
# #118; two hypotheses nest a phrase in an OR branch so the OrProximityIterator
# sees an un-positioned phrase (orproximity.cc:112). Each builds a HASH index
# with TEXT/TAG/NUMERIC and WITHSUFFIXTRIE, then runs one FT.SEARCH.
# --------------------------------------------------------------------------
def _vp_index(r, index):
    _ignore(lambda: r.execute_command("FT.DROPINDEX", index))
    r.execute_command(
        "FT.CREATE",
        index,
        "ON",
        "HASH",
        "PREFIX",
        "1",
        f"{index}:d:",
        "SCHEMA",
        "title",
        "TEXT",
        "w",
        "TEXT",
        "NOSTEM",
        "WITHSUFFIXTRIE",
        "c",
        "TAG",
        "n",
        "NUMERIC",
    )
    r.hset(f"{index}:d:1", mapping={"title": "hello word3", "w": "lulu gama alp alpine", "c": "d2", "n": 1})
    r.hset(f"{index}:d:2", mapping={"title": "late doc", "w": "alpine gama", "c": "d3", "n": 2})
    r.hset(f"{index}:d:3", mapping={"title": "hello word1", "w": "bitter lulu", "c": "d2", "n": 3})


def orprox_control_and_or_nontext_phrase(r):
    """proximity.cc:187 control: AND around an OR whose branches AND a non-text predicate with an exact phrase."""
    _vp_index(r, "vpa")
    _ignore(
        lambda: r.execute_command(
            "FT.SEARCH", "vpa", '((@n:[-inf +inf] "lulu gama") | (@c:{d2} "gama alp")) @n:[-inf +inf]', "DIALECT", "2"
        )
    )


def orprox_control_outer_text(r):
    """proximity.cc:187 control variant: the outer AND uses a text predicate."""
    _vp_index(r, "vpb")
    _ignore(
        lambda: r.execute_command(
            "FT.SEARCH", "vpb", '((@n:[-inf +inf] "lulu gama") | (@c:{d2} "gama alp")) @w:alpine', "DIALECT", "2"
        )
    )


def orprox_phrases_under_and(r):
    """orproximity.cc:112 hypothesis: an OR of two (phrase AND non-text) branches, ANDed with a phrase in w."""
    _vp_index(r, "vpc")
    _ignore(
        lambda: r.execute_command(
            "FT.SEARCH", "vpc", '(("lulu gama" @n:[-inf +inf]) | ("gama alp" @c:{d2})) @w:"alpine gama"', "DIALECT", "2"
        )
    )


def orprox_negated_phrase_in_branch(r):
    """orproximity.cc:112 hypothesis: a negated phrase nested in the OR-of-nontext-AND shape (the kContainsNegate
    exception of the skip-text guard)."""
    _vp_index(r, "vpd")
    _ignore(
        lambda: r.execute_command(
            "FT.SEARCH",
            "vpd",
            '((@n:[-inf +inf] "lulu gama") | (@c:{d2} -"gama alp")) @w:"alpine gama"',
            "DIALECT",
            "2",
        )
    )


PROBES = [
    orprox_control_and_or_nontext_phrase,
    orprox_control_outer_text,
    orprox_phrases_under_and,
    orprox_negated_phrase_in_branch,
    text_and_or_phrase_tag,
    text_and_or_phrase_numeric,
    text_inorder_slop_phrase,
    text_negate_with_vector,
    text_or_nontext_and_text_vector,
    text_three_term_or_proximity,
    suffix_query_with_knn,
    suffix_multifield_mask,
    infix_query,
    fuzzy_over_multibyte,
    fuzzy_distance_boundary,
    vector_range_inf_radius,
    vector_range_nan_epsilon,
    vector_range_inf_epsilon,
    knn_nonfinite_vector,
    knn_limit_offset_ge_k,
    knn_limit_number_zero,
    knn_k_huge,
    scorer_tfidf,
    scorer_tfidf_aggregate,
    aggregate_apply_arity,
    aggregate_apply_substr_arity,
    aggregate_filter_nested,
    aggregate_groupby_reduce,
    aggregate_apply_parsetime,
    hybrid_linear_no_weights,
    hybrid_combine_function,
    hybrid_vsim_range,
    wrong_type_under_prefix,
    rename_into_prefix,
    json_field_on_hash_index,
    dropindex_then_search,
    ft_info_midbackfill,
    ft_info_primary_standalone,
    multi_exec_search,
]


def alive(port):
    try:
        redis.Redis(host="127.0.0.1", port=port, socket_timeout=5).ping()
        return True
    except Exception:
        return False


def main():
    if len(sys.argv) < 2:
        print("usage: python3 repros.py <port> [--only NAME] [--skip NAME[,NAME...]]")
        sys.exit(2)
    port = int(sys.argv[1])
    only = None
    skip = set()
    args = sys.argv[2:]
    i = 0
    while i < len(args):
        if args[i] == "--only" and i + 1 < len(args):
            only = args[i + 1]
            i += 2
        elif args[i] == "--skip" and i + 1 < len(args):
            skip.update(s for s in args[i + 1].split(",") if s)
            i += 2
        else:
            print(f"unknown arg: {args[i]}")
            sys.exit(2)

    for probe in PROBES:
        name = probe.__name__
        if only is not None and name != only:
            continue
        if name in skip:
            continue
        r = redis.Redis(host="127.0.0.1", port=port, socket_timeout=30)
        try:
            probe(r)
            r.ping()
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError):
            # The server stopped responding mid-probe: confirm it is really
            # down before declaring an abort (a timeout alone is not proof).
            if not alive(port):
                print(f"ABORT {name}")
                sys.exit(3)
            print(f"ok {name} (transient)")
            continue
        except redis.exceptions.ResponseError:
            # An error reply at the top level (e.g. PING unaffected) is fine.
            pass
        print(f"ok {name}")

    print("done")


if __name__ == "__main__":
    main()
