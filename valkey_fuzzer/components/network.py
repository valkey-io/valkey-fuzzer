"""Network faults between deployment instances (Linux iptables, per-instance loopback).

``valkey.fault.network-partition`` isolates selected instances from the rest of
the deployment: it drops every packet between each isolated instance's address
and each other instance's address.  Client traffic from the harness (127.0.0.1)
is unaffected, so clients can keep talking to both sides — the classic
split-brain setup.

Rules live in a dedicated iptables chain jumped to from INPUT, so healing is
atomic and recovery never touches unrelated rules.  The chain is owned by an
undo obligation from the moment it exists: installing is all-or-nothing (a
failed step removes the chain again), a heal discharges it early, and a chain
that could not be removed stays open for recovery and then the runner.
Every iptables call is bounded (``run_iptables``), so a trigger never blocks
its event's dispatch on a held xtables lock. The chain's name carries this
process as its owner (``chain_name``), so ``valkey-fuzzer sweep`` finds and
removes a chain a killed run left (a batch sweeps each worker after its run and
reports it as that run's leak).  The deployment must use
``addressing: per_instance_loopback``; the ``valkey.capability.host-probe``
probe with ``requires: [linux, root, iptables, per_instance_loopback]`` turns
hosts that cannot do this into ``SKIP_UNSUPPORTED`` before anything starts.
"""

import threading
import time
import uuid
from collections.abc import Mapping, Sequence
from typing import Any, Optional

from valkey_fuzzer.hoststate import chain_name, remove_chain, run_iptables
from valkey_fuzzer.plan import ComponentSpec, ExecutionPlan
from valkey_fuzzer.schema import ConfigError, ConfigField, ConfigSchema, plain_config

from .faults import EventFault, fault_declares
from .instances import deployment, select_instances
from .registration import component_registration
from .schemas import LIST, MAPPING, TEXT, optional


class PartitionError(Exception):
    """An iptables step of a partition failed."""


def create_chain(chain: str) -> None:
    """Create the partition's own chain; whoever partitions owns it (``network.partition``) from then on."""

    result = run_iptables("-N", chain)
    if result.returncode != 0:
        raise PartitionError(f"cannot create chain: {result.stderr.strip()}")


def drop_between(chain: str, addresses: Mapping[str, str], isolated: Sequence[str], peers: Sequence[str]) -> list[str]:
    """Add a DROP rule each way between every isolated instance and every peer to ``chain``, then jump to it
    from INPUT; the rules (``source->destination``), or :class:`PartitionError` at the first failed step."""

    rules = []
    for one in isolated:
        for peer in peers:
            a, b = addresses[one], addresses[peer]
            for source, destination in ((a, b), (b, a)):
                rule = ("-A", chain, "-s", source, "-d", destination, "-j", "DROP")
                added = run_iptables(*rule)
                if added.returncode != 0:
                    raise PartitionError(f"iptables {' '.join(rule)}: {added.stderr.strip()}")
                rules.append(f"{source}->{destination}")
    jump = run_iptables("-I", "INPUT", "1", "-j", chain)
    if jump.returncode != 0:
        raise PartitionError(f"cannot install jump: {jump.stderr.strip()}")
    return rules


def remove_partition(chain: str) -> None:
    """The undo of a partition: remove its chain and the jump to it (idempotent); raise while it remains."""

    errors = remove_chain(chain, run_iptables)
    if errors:
        raise RuntimeError(f"iptables chain {chain} remains: {errors}")


_PARTITION_FIELDS = {
    "isolate": ConfigField((str, list)),
    "peers": ConfigField((str, list), default="others"),
    "heal_event": ConfigField(str, allow_none=True, default=None),
    "duration_s": ConfigField(float, allow_none=True, default=None, minimum=0.01, maximum=3600),
}


def _validate_partition(config: dict, spec: ComponentSpec, plan: ExecutionPlan) -> None:
    document = plain_config(plan.deployment)
    if document.get("addressing") != "per_instance_loopback":
        raise ConfigError("valkey.fault.network-partition requires deployment.addressing: per_instance_loopback")
    if config["heal_event"] is not None and config["duration_s"] is not None:
        raise ConfigError("set heal_event or duration_s, not both")


class PartitionFault(EventFault):
    """Cut the selected instances off from their peers at a lifecycle event, until ``heal_event``,
    ``duration_s``, or recovery.

    Clients on 127.0.0.1 still reach both sides, so writes can land on a primary that is about to lose them.
    """

    def _prepare(self, context: Any, spec: ComponentSpec) -> None:
        handle = deployment(context)
        isolated = select_instances(handle, self._config["isolate"])
        if self._config["peers"] == "others":
            peers = [instance.instance_id for instance in handle.instances if instance.instance_id not in isolated]
        else:
            peers = [
                instance_id
                for instance_id in select_instances(handle, self._config["peers"])
                if instance_id not in isolated
            ]
        if not isolated or not peers:
            raise ValueError("a partition needs at least one isolated instance and one peer")
        self._isolated = isolated
        self._peers = peers
        self._addresses = {instance.instance_id: instance.host for instance in handle.instances}
        if len(set(self._addresses.values())) != len(self._addresses):
            raise ValueError("instances share an address; use deployment.addressing: per_instance_loopback")
        # Unique to this fault instance, so concurrent runs in one process never share a chain.
        self._chain = chain_name(f"{spec.id}/{uuid.uuid4().hex}")
        # Reentrant: a subscriber of <id>.healed, which is published under it, may heal again.
        self._lock_heal = threading.RLock()
        # Set once recovery begins: a late timer or heal event no longer acts.
        self._closed = False
        self._heal_reason: Optional[str] = None
        # The chain's obligation, from its creation; the jump to it is installed.
        self._partition: Any = None
        self._installed = False
        self._healed_at = None
        self._heal_errors: list[str] = []
        self._timer = None
        if self._config["heal_event"]:
            self._subscribe(self._config["heal_event"], self._heal_on_event)

    def _fire(self, context: Any, spec: ComponentSpec, event: Any) -> dict[str, Any]:
        try:
            rules = self._install()
        except PartitionError as exc:
            record: dict[str, Any] = {"applied": False, "error": str(exc)}
            error = self._remove()
            if error:
                record["cleanup_errors"] = [error]
            return record
        except BaseException:
            self._remove()
            raise
        self._installed = True
        context.event_bus.publish(
            f"{spec.id}.partitioned",
            source=spec.id,
            payload={"isolated": self._isolated, "peers": self._peers},
        )
        if self._config["duration_s"] is not None:
            self._timer = threading.Timer(self._config["duration_s"], self._heal, kwargs={"reason": "duration"})
            self._timer.daemon = True
            self._timer.start()
        return {
            "applied": True,
            "targets": self._isolated,
            "isolated": self._isolated,
            "peers": self._peers,
            "addresses": self._addresses,
            "chain": self._chain,
            "rules": rules,
        }

    def _install(self) -> list[str]:
        """Create the chain, add every DROP rule, and jump to it from INPUT; raise on the first failure."""

        create_chain(self._chain)
        self._partition = self._own("network.partition", self._chain, lambda: remove_partition(self._chain))
        return drop_between(self._chain, self._addresses, self._isolated, self._peers)

    def _remove(self) -> Optional[str]:
        """Discharge the chain's obligation if it was opened; the error, if the chain remains."""

        if self._partition is None:
            return None
        try:
            self._partition.discharge()
        except Exception as exc:
            return f"{type(exc).__name__}: {exc}"
        return None

    def _heal(self, reason: str) -> None:
        with self._lock_heal:
            if self._closed or not self._installed or self._healed_at is not None:
                return
            error = self._undo_early(self._partition, "healed", {"reason": reason, "isolated": self._isolated})
            if error:
                self._heal_errors = [error]
                return
            self._healed_at = time.time()
            self._heal_reason = reason

    def _heal_on_event(self, event: Any) -> None:
        self._heal("event")

    def _quiesce(self) -> None:
        if self._timer is not None:
            self._timer.cancel()
        # Waits for a heal in progress; recovery removes the chain from here on, and publishes nothing.
        with self._lock_heal:
            self._closed = True

    def _settled(self, context: Any, spec: ComponentSpec, record: dict[str, Any]) -> None:
        with self._lock_heal:
            if self._installed and self._healed_at is None and not self._partition.open:
                self._healed_at = time.time()
                self._heal_reason = "recover"
        if self._heal_errors:
            record["heal_errors"] = self._heal_errors
            raise RuntimeError(f"partition heal failed, so it lasted until recovery: {self._heal_errors}")
        if self._healed_at is not None:
            record["healed_at"] = self._healed_at
            record["heal_reason"] = self._heal_reason
            record["partition_seconds"] = round(self._healed_at - record.get("applied_at", self._healed_at), 3)


_PARTITION_DECLARES = fault_declares(
    {
        "targets": LIST,
        "isolated": LIST,
        "peers": LIST,
        "addresses": MAPPING,
        "chain": TEXT,
        "rules": LIST,
        "healed_at": optional(float),
        "heal_reason": optional(str, allow_none=True),
        "partition_seconds": optional(float),
        "heal_errors": optional(list),
        "cleanup_errors": optional(list),
    },
    events={
        "{id}.partitioned": ConfigSchema({"isolated": LIST, "peers": LIST}),
        "{id}.healed": ConfigSchema({"reason": TEXT, "isolated": LIST}),
    },
    changes=("network.partition",),
)

REGISTRATIONS = (
    component_registration(
        "valkey.fault.network-partition",
        PartitionFault,
        _PARTITION_FIELDS,
        check=_validate_partition,
        selectors={"isolate": (), "peers": ("others",)},
        triggered=True,
        declares=_PARTITION_DECLARES,
    ),
)
