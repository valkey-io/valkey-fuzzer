"""Client and cluster bus ports for local instances, and checks that a port is free."""

import errno
import fcntl
import os
import socket
import subprocess
from collections.abc import Iterable, Mapping
from typing import Any, NamedTuple, Optional

from ...hoststate import open_shared_lock, shared_directory
from .config import MAX_TCP_PORT, deployment_config_from_document
from .models import CLUSTER_BUS_PORT_OFFSET, DeploymentMode
from .planner import plan_deployment

# Host-wide, whatever TMPDIR a batch worker has: every run on this host locks the same files. It sits beside, not
# inside, the default data directory: a root run creating that directory would leave it unwritable for other users.
LOCK_DIRECTORY = "/tmp/valkey-fuzzer-locks"


class PortUnavailableError(RuntimeError):
    """A planned instance port (or loopback block) is already in use by a process we did not start."""


def claim_loopback_block(block: int) -> int:
    """Hold loopback block ``block`` (``127.0.<block>.0/24``) for one deployment; closing the descriptor releases it.

    A partition's DROP rules match addresses only, so two runs on one block
    would cut each other's traffic, and the other run would see a wrong
    FAIL. The claim is an exclusive ``flock`` on
    ``<LOCK_DIRECTORY>/loopback-<block>``, which the kernel releases if the
    process dies. Raises :class:`PortUnavailableError` while another run holds it.
    """

    path = os.path.join(LOCK_DIRECTORY, f"loopback-{block}")
    try:
        shared_directory(LOCK_DIRECTORY)  # Every user's runs share the blocks, and so the locks.
        handle = open_shared_lock(path)
    except OSError as exc:
        raise PortUnavailableError(f"Cannot lock loopback block {block} at {path}: {exc}") from exc
    try:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        os.close(handle)
        raise PortUnavailableError(
            f"Cannot provision the local deployment: loopback block {block} (127.0.{block}.0/24) is in use by "
            "another run on this host, and a partition in either would cut the other's traffic. Give concurrent "
            "runs different deployment.loopback_block values, or run them one after the other."
        ) from None
    return handle


def port_unavailable_reason(host: str, port: int) -> Optional[str]:
    """Return why ``host:port`` cannot be used for a new server, or None if it is free.

    A connect probe catches listeners on any local address (including wildcard
    binds). A bind probe with SO_REUSEADDR, which Valkey itself uses, catches
    sockets bound to the exact address while ignoring TIME_WAIT leftovers.
    """
    try:
        with socket.create_connection((host, port), timeout=0.25):
            return "a server is already accepting connections"
    except OSError:
        pass

    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        probe.bind((host, port))
    except OSError as exc:
        return errno.errorcode.get(exc.errno, str(exc)) if exc.errno else str(exc)
    finally:
        probe.close()
    return None


def ephemeral_port_range() -> Optional[tuple[int, int]]:
    """This host's ephemeral port range ``(first, last)``, or None where it cannot be read.

    The kernel hands these ports to outgoing connections, so one can take a
    planned port between the free-port check and Valkey's bind, and the
    server aborts ("Failed listening on port").
    """
    try:
        with open("/proc/sys/net/ipv4/ip_local_port_range", encoding="ascii") as handle:
            first, last = handle.read().split()
        return int(first), int(last)
    except (OSError, ValueError):
        pass
    try:
        result = subprocess.run(
            ["sysctl", "-n", "net.inet.ip.portrange.first", "net.inet.ip.portrange.last"],
            capture_output=True,
            text=True,
            timeout=5,
            check=True,
        )
        first, last = result.stdout.split()
        return int(first), int(last)
    except Exception:  # no sysctl, or not these keys: the range is unknown
        return None


def highest_base_port(width: int, *, cluster: bool) -> int:
    """The highest base port whose ``width`` client ports, and in cluster mode their bus ports, sit below this
    host's ephemeral port range (and port 65536)."""

    span = ephemeral_port_range()
    ceiling = MAX_TCP_PORT + 1 if span is None else min(span[0], MAX_TCP_PORT + 1)
    return ceiling - width - (CLUSTER_BUS_PORT_OFFSET if cluster else 0)


def ephemeral_overlap(base: int, width: int, *, cluster: bool, field: str) -> Optional[str]:
    """Why the ``width`` ports from ``base`` (and their cluster bus ports) cannot be used here, or None.

    None also when the host's ephemeral range cannot be read. ``field`` names
    the setting to change in the message.
    """
    span = ephemeral_port_range()
    if span is None:
        return None
    first, last = span
    blocks = [("client", base)] + ([("cluster bus", base + CLUSTER_BUS_PORT_OFFSET)] if cluster else [])
    inside = [(kind, low) for kind, low in blocks if low <= last and low + width - 1 >= first]
    if not inside:
        return None
    ports = " and ".join(f"{kind} ports {low}..{low + width - 1}" for kind, low in inside)
    offset = CLUSTER_BUS_PORT_OFFSET if cluster else 0
    return (
        f"{ports} overlap this host's ephemeral port range {first}-{last}, where an outgoing connection can "
        f"take one before Valkey binds it: pick a {field} of at most {first - width - offset}"
    )


class BusyPort(NamedTuple):
    """A planned instance port that is already in use."""

    instance_id: str
    kind: str  # "client" or "cluster bus"
    host: str
    port: int
    reason: str

    def __str__(self) -> str:
        return f"{self.instance_id}: {self.kind} port {self.port} on {self.host} is unavailable ({self.reason})"


def busy_instance_ports(instances: Iterable[Any], *, cluster: bool) -> list[BusyPort]:
    """The client (and, in cluster mode, cluster bus) ports of ``instances`` that are in use.

    Each instance needs ``instance_id``, ``host``, ``port``, and ``bus_port``, as an
    InstancePlan or InstanceInfo has.
    """
    busy = []
    for instance in instances:
        ports = [("client", instance.port)]
        if cluster:
            ports.append(("cluster bus", instance.bus_port))
        for kind, port in ports:
            reason = port_unavailable_reason(instance.host, port)
            if reason is not None:
                busy.append(BusyPort(instance.instance_id, kind, instance.host, port, reason))
    return busy


def ensure_ports_free(instances: Iterable[Any], *, cluster: bool, action: str) -> None:
    """Raise :class:`PortUnavailableError` naming every busy port of ``instances``.

    A server we did not start on a planned port would answer readiness checks
    and receive formation commands such as ``CLUSTER RESET HARD``, so nothing
    starts while one is taken.
    """
    busy = busy_instance_ports(instances, cluster=cluster)
    if busy:
        raise PortUnavailableError(
            f"Cannot {action}, planned ports are in use: {'; '.join(map(str, busy))}. "
            "Stop the processes using them or choose a different base_port."
        )


def busy_planned_ports(deployment: Mapping[str, Any]) -> list[BusyPort]:
    """The ports that local ``deployment`` (a deployment document) plans and that are already in use.

    The deployment is planned exactly as provisioning plans it, without starting
    anything. An empty list means every planned port is free right now. Raises
    ValueError or TypeError when the document is not a valid local deployment.
    """
    config = deployment_config_from_document(deployment)
    plans = plan_deployment(config)
    return busy_instance_ports(plans, cluster=config.mode is DeploymentMode.CLUSTER)
