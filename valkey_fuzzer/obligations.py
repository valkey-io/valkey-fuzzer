"""Fault ownership: every change a component makes to host or server state is owned until it is undone.

A component that stops a process, partitions the network, fills a disk, or
changes a server setting it will restore opens an :class:`Obligation` with
``context.obligations.open(spec, kind, target, undo)`` the moment it applies
the change, and discharges it (``obligation.discharge()``) when it undoes the
change, at the latest in its ``recover`` or ``stop``. ``undo`` must be
idempotent: a failed undo leaves the obligation open, and it is called again.

The runner undoes, best effort and newest first, every obligation still open
once the components are torn down, and again after the deployment is cleaned
up, which closes the ledger. A start call the runner gave up on that is still
running may still be applying changes, so neither pass touches what it owns:
that is undone the moment the call returns, if the ledger is closed by then.
A change opened after the ledger closed (by an abandoned call, or a thread a
component left behind) is undone at once, and ``open`` raises. An obligation
its owner did not undo on its first try, or at all, makes the run ``ERROR``:
the state the oracles judged was not the state the plan describes. A change
meant to last until the deployment is removed (a kill with no restart, a
``CONFIG SET`` an action makes) is the deployment's to clean up and needs no
obligation.
"""

import threading
import time
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Optional

from ._data import error_record
from .fields import require_name, require_text

if TYPE_CHECKING:
    # The protocols module imports this one for RunContext.obligations.
    from .plan import ComponentSpec


@dataclass(eq=False)
class Obligation:
    """One applied change and the idempotent ``undo`` that reverts it."""

    number: int
    owner: str
    kind: str
    target: str
    undo: Callable[[], Any] = field(repr=False)
    opened_at: float
    ledger: "ObligationLedger" = field(repr=False)
    undone_at: Optional[float] = None
    undone_by: Optional[str] = None
    # Each failed undo: who tried it (``by``) and its error, as a report records one (``error_record``).
    errors: list[Mapping[str, str]] = field(default_factory=list)
    lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    @property
    def open(self) -> bool:
        return self.undone_at is None

    def discharge(self) -> None:
        """Undo the change now, as its owner; a no-op once undone. Raises if the undo fails, and stays open."""

        self.ledger.undo(self, "owner")

    def to_data(self) -> dict[str, Any]:
        data: dict[str, Any] = {
            "number": self.number,
            "owner": self.owner,
            "kind": self.kind,
            "target": self.target,
            "opened_at": self.opened_at,
        }
        if self.undone_at is not None:
            data.update(undone_at=self.undone_at, undone_by=self.undone_by)
        if self.errors:
            data["errors"] = list(self.errors)
        return data


class ObligationLedger:
    """The obligations of one run: who changed what, and whether it was undone.

    ``check(owner, kind)``, when given, raises for a change the owner may not
    make (the runner's evidence contract checks it against the owner's
    declared ``changes``); the obligation is recorded first, so it is undone
    either way.
    """

    def __init__(self, check: Optional[Callable[[str, str], None]] = None):
        self._check = check
        self._lock = threading.Lock()
        self._obligations: list[Obligation] = []
        self._planned: dict[str, tuple[str, ...]] = {}
        self._applying: set[str] = set()
        self._closed = False

    def plan(self, planned: Mapping[str, tuple[str, ...]]) -> None:
        """The kinds of change each component of the plan declares, for the report."""

        self._planned = {owner: tuple(kinds) for owner, kinds in planned.items() if kinds}

    def open(self, spec: "ComponentSpec", kind: str, target: str, undo: Callable[[], Any]) -> Obligation:
        """Record that component ``spec`` applied a change of ``kind`` to ``target``, undone by ``undo``."""

        require_name(kind, "change kind")
        require_text(target, "change target")
        with self._lock:
            obligation = Obligation(len(self._obligations) + 1, spec.id, kind, target, undo, time.time(), self)
            self._obligations.append(obligation)
            closed = self._closed
        if closed:
            try:
                self.undo(obligation, "runner")
            except Exception:
                pass
            raise RuntimeError(
                f"component {spec.id!r} changed {kind} on {target} after its run ended; the change was undone at once"
            )
        if self._check is not None:
            self._check(spec.id, kind)
        return obligation

    @contextmanager
    def applying(self, owner: str) -> Iterator[None]:
        """While ``owner``'s call runs, sweeps leave its obligations alone: undoing them would race the call.

        Once the call returns, a closed ledger undoes them at once.
        """

        with self._lock:
            self._applying.add(owner)
        try:
            yield
        finally:
            with self._lock:
                self._applying.discard(owner)
                closed = self._closed
            if closed:
                self.sweep()

    def undo(self, obligation: Obligation, by: str) -> None:
        """Undo ``obligation`` unless it is undone; record and re-raise a failure."""

        with obligation.lock:
            if not obligation.open:
                return
            try:
                obligation.undo()
            except BaseException as exc:
                obligation.errors.append({"by": by, **error_record(exc)})
                raise
            obligation.undone_at, obligation.undone_by = time.time(), by

    def sweep(self) -> None:
        """Undo every open obligation, newest first, as the runner, except those of a call still applying;
        failures stay open for the next sweep.

        An interrupt (Ctrl-C) during one undo does not skip the rest: it is re-raised once all were tried.
        """

        with self._lock:
            pending = [item for item in self._obligations if item.open and item.owner not in self._applying]
        interrupt: Optional[BaseException] = None
        for obligation in reversed(pending):
            try:
                self.undo(obligation, "runner")
            except Exception:
                pass
            except BaseException as exc:
                interrupt = interrupt or exc
        if interrupt is not None:
            raise interrupt

    def close(self) -> None:
        """The run's final sweep: from now on a new obligation, or one a returning call leaves, is undone at once."""

        with self._lock:
            self._closed = True
        self.sweep()

    def failures(self) -> tuple[dict[str, Any], ...]:
        """Obligations their owner did not undo on the first try: undone by the runner, retried, or still open."""

        with self._lock:
            obligations = list(self._obligations)
        return tuple(item.to_data() for item in obligations if item.undone_by != "owner" or item.errors)

    def report(self) -> dict[str, Any]:
        """``planned`` (declared kinds by component), and the obligations ``undone`` and ``still_active``.

        Every obligation opened is in exactly one of the two.
        """

        with self._lock:
            obligations = list(self._obligations)
        return {
            "planned": [{"owner": owner, "kinds": list(kinds)} for owner, kinds in self._planned.items()],
            "undone": [item.to_data() for item in obligations if not item.open],
            "still_active": [item.to_data() for item in obligations if item.open],
        }

    @property
    def empty(self) -> bool:
        with self._lock:
            return not self._obligations and not self._planned
