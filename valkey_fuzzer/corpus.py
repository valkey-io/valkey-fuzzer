"""The corpus of a coverage-guided batch: the runs that reached Valkey code no earlier run reached.

A batch with a ``guided`` random source collects every run's code coverage
(:func:`~valkey_fuzzer.coverage.gcov_features`). A run that reaches a line or
branch no earlier run of the batch reached becomes a corpus entry, with the
features it was first to reach. The batch then derives new scenarios from the
entries, as AFL derives inputs from its queue: :meth:`Corpus.pick` chooses an
entry, weighted by the features it added and less each time a scenario was
derived from it, and :func:`mutation` chooses what the new scenario changes.
The corpus lives in the batch folder's ``corpus.json``, so a resumed batch
continues from it, and a new batch may start from an earlier batch's
(``corpus_from``): its entries come along with the run folder each names and
what that run drew, and only features new to the combined corpus make entries.
"""

import json
import os
import random
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

from .reports import write_document

CORPUS_FILE = "corpus.json"


@dataclass
class Corpus:
    """The corpus entries in the order runs added them, and every feature they reached."""

    entries: list[dict[str, Any]] = field(default_factory=list)
    covered: set[str] = field(default_factory=set)

    @classmethod
    def load(cls, folder: str) -> "Corpus":
        """The corpus in batch folder ``folder``; empty when it has none."""

        path = os.path.join(folder, CORPUS_FILE)
        if not os.path.exists(path):
            return cls()
        with open(path, encoding="utf-8") as handle:
            return cls.of(json.load(handle)["entries"])

    @classmethod
    def of(cls, entries: list[dict[str, Any]]) -> "Corpus":
        """The corpus of ``entries``: what they reached first is everything it covers."""

        return cls(entries, {feature for entry in entries for feature in entry["new"]})

    def save(self, folder: str) -> None:
        write_document(os.path.join(folder, CORPUS_FILE), {"covered": len(self.covered), "entries": self.entries})

    def add(self, job_id: str, experiment: Optional[str], features: Sequence[str]) -> int:
        """Record a run's features; a run that reached any new one becomes an entry. The count of new ones.

        A job already in the corpus (a resumed batch runs again a job whose result was not written) adds nothing.
        """

        if any(parent(entry) == job_id for entry in self.entries):
            return 0
        new = sorted(set(features) - self.covered)
        if new:
            self.covered.update(new)
            self.entries.append({"job": job_id, "experiment": experiment, "new": new})
        return len(new)

    def pick(
        self, rng: random.Random, eligible: Callable[[Mapping[str, Any]], bool], picked: Mapping[str, int]
    ) -> Optional[dict[str, Any]]:
        """An ``eligible`` entry, weighted by the features it added divided by one more than the times it was
        ``picked`` (by :func:`parent`) before; None when none is eligible."""

        candidates = [entry for entry in self.entries if eligible(entry)]
        if not candidates:
            return None
        weights = [len(entry["new"]) / (1 + picked.get(parent(entry), 0)) for entry in candidates]
        return rng.choices(candidates, weights)[0]


def parent(entry: Mapping[str, Any]) -> str:
    """What a scenario derived from ``entry`` names as its parent: the job ID of a run of this batch, the run
    folder of an entry an earlier batch's corpus brought (``run``)."""

    return entry.get("run", entry["job"])


def mutation(rng: random.Random, dimensions: Sequence[str]) -> Optional[str]:
    """What a scenario derived from another changes: one of its drawn ``dimensions``, or None for only its seed.

    Every derived scenario has a new seed, which is what the workload's values and operations and the runtime
    choices of faults draw from; a dimension (a deployment field, a server config key, an input such as a fault
    delay or the waves of a chaos schedule) is drawn again within the experiment's declared space.
    """

    return rng.choice([None, *dimensions])
