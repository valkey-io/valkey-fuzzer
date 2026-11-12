"""A coverage-guided batch's corpus and code coverage, with fake coverage data."""

import collections
import json
import random
import sys

from valkey_fuzzer.corpus import CORPUS_FILE, Corpus, mutation
from valkey_fuzzer.coverage import gcov_features


def test_a_run_that_reaches_new_code_becomes_an_entry_with_what_it_reached_first(tmp_path):
    corpus = Corpus()
    assert corpus.add("a", "e1", ["x.c:1", "x.c:2"]) == 2
    assert corpus.add("b", "e1", ["x.c:2", "x.c:3", "x.c:3:0"]) == 2
    # Nothing new: no entry. A job the corpus holds (run again after a resume) adds nothing.
    assert corpus.add("c", "e2", ["x.c:1"]) == 0
    assert corpus.add("b", "e1", ["y.c:9"]) == 0
    assert [(entry["job"], entry["new"]) for entry in corpus.entries] == [
        ("a", ["x.c:1", "x.c:2"]),
        ("b", ["x.c:3", "x.c:3:0"]),
    ]
    corpus.save(str(tmp_path))
    assert json.loads((tmp_path / CORPUS_FILE).read_text())["covered"] == 4
    loaded = Corpus.load(str(tmp_path))
    assert loaded.entries == corpus.entries and loaded.covered == corpus.covered
    assert Corpus.load(str(tmp_path / "none")).entries == []


def test_an_entry_is_picked_by_what_it_added_and_less_each_time_it_was_picked():
    corpus = Corpus()
    corpus.add("big", "e1", [f"x.c:{line}" for line in range(9)])
    corpus.add("small", "e1", ["y.c:1"])
    corpus.add("other", "e2", [f"z.c:{line}" for line in range(50)])
    rng = random.Random(0)

    def picks(picked):
        return collections.Counter(
            corpus.pick(rng, lambda entry: entry["experiment"] == "e1", picked)["job"] for _ in range(2000)
        )

    fresh = picks({})
    assert set(fresh) == {"big", "small"} and 8 < fresh["big"] / fresh["small"] < 11
    # Picked 8 times before, big weighs as much as small.
    worn = picks({"big": 8})
    assert 0.8 < worn["big"] / worn["small"] < 1.25
    assert corpus.pick(rng, lambda entry: False, {}) is None


def test_a_mutation_draws_one_dimension_again_or_only_the_seed():
    rng = random.Random(1)
    seen = collections.Counter(mutation(rng, ["inputs.a", "deployment.mode"]) for _ in range(900))
    assert set(seen) == {None, "inputs.a", "deployment.mode"} and min(seen.values()) > 250


def test_gcov_reads_the_counters_every_process_wrote_under_the_prefix(tmp_path):
    prefix, build = tmp_path / "prefix", tmp_path / "valkey" / "src"
    build.mkdir(parents=True)
    (build / "server.gcno").write_text("notes")
    data = prefix / build.relative_to("/")
    data.mkdir(parents=True)
    (data / "server.gcda").write_text("counters")
    (data / "t_string.gcda").write_text("counters")
    # A fake gcov: one JSON document per data file on stdout, as gcov --json-format --stdout prints them.
    gcov = tmp_path / "gcov"
    documents = [
        {"files": [{"file": "server.c", "lines": [
            {"line_number": 10, "count": 3, "branches": [{"count": 3}, {"count": 0}]},
            {"line_number": 11, "count": 0, "branches": []},
        ]}]},
        {"files": [{"file": "t_string.c", "lines": [{"line_number": 5, "count": 1}]},
                   {"file": "server.h", "lines": [{"line_number": 2, "count": 7}]}]},
    ]  # fmt: skip
    gcov.write_text(
        f"#!{sys.executable}\nimport json, sys\n"
        f"open({str(tmp_path / 'args')!r}, 'w').write(json.dumps(sys.argv[1:]))\n"
        f"for document in {documents!r}: print(json.dumps(document))\n"
    )
    gcov.chmod(0o755)
    assert gcov_features(str(prefix), str(gcov)) == ["server.c:10", "server.c:10:0", "server.h:2", "t_string.c:5"]
    args = json.loads((tmp_path / "args").read_text())
    assert args[:2] == ["--json-format", "--stdout"] and args[2:] == [
        str(data / "server.gcda"),
        str(data / "t_string.gcda"),
    ]
    # gcov finds each object's notes next to its counters: linked to where the build left them.
    assert (data / "server.gcno").resolve() == (build / "server.gcno").resolve()
    assert gcov_features(str(tmp_path / "empty"), str(gcov)) == []
