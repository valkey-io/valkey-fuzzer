"""Generated ``valkey.cluster-chaos`` scenarios, for tests that compare against them.

``run --seed N --experiment valkey.cluster-chaos`` draws its scenario from the
experiment's generator block; :func:`generate_scenario` draws the same
scenario without touching ports or binaries.
"""

import functools
import random
from typing import Any, Optional

from valkey_fuzzer.service import RunService

from .valkey_experiments_support import valkey_service

CLUSTER_EXPERIMENT = "valkey.cluster-chaos"
# Cluster chaos seeds whose clusters fit one host, at most ten instances (seed 7: 3x2,
# 35: 5x1, 110: 3x1); the live random test runs them, and test_generator checks they fit.
SMALL_CLUSTER_SEEDS = (7, 35, 110)


@functools.lru_cache(maxsize=1)
def generator_service() -> RunService:
    return valkey_service()


def generate_scenario(seed: Optional[int] = None) -> dict[str, Any]:
    """The ``valkey.cluster-chaos`` scenario ``run --seed`` generates (a random seed when None)."""

    seed = random.randint(0, 2**32 - 1) if seed is None else seed
    return generator_service().generate(seed, experiment=CLUSTER_EXPERIMENT).scenario
