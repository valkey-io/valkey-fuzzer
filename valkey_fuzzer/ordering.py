"""Dependency order: declarations placed after what they depend on, otherwise in the order given."""

from collections.abc import Mapping, Sequence
from typing import Any, TypeVar


class DependencyCycleError(ValueError):
    """Declarations depend on each other in a circle; ``cycle`` is its path, its first node repeated last."""

    def __init__(self, what: str, cycle: Sequence[Any]):
        self.cycle = tuple(cycle)
        super().__init__(f"{what} dependency cycle: {' -> '.join(str(node) for node in self.cycle)}")


N = TypeVar("N")


def stable_order(nodes: Sequence[N], edges: Mapping[N, Sequence[N]], what: str) -> tuple[N, ...]:
    """``nodes`` with each after the nodes ``edges`` says it depends on, otherwise in their given order.

    The next node is always the first whose dependencies are placed, so an
    author's order survives wherever the dependencies allow it. Every
    dependency must be one of ``nodes``.
    """

    placed: set[N] = set()
    ordered: list[N] = []
    pending = list(nodes)
    while pending:
        ready = next((node for node in pending if all(item in placed for item in edges.get(node, ()))), None)
        if ready is None:
            # Every pending node waits on another pending one: follow them until one repeats.
            path = [pending[0]]
            while True:
                after = next(item for item in edges.get(path[-1], ()) if item not in placed)
                if after in path:
                    raise DependencyCycleError(what, (*path[path.index(after) :], after))
                path.append(after)
        pending.remove(ready)
        placed.add(ready)
        ordered.append(ready)
    return tuple(ordered)
