"""Builders shared by the local deployment tests."""

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any, Optional
from unittest.mock import Mock

import pytest

from tests.live_support import BINARY
from valkey_fuzzer.deployment.local.cluster_nodes import parse_cluster_nodes
from valkey_fuzzer.deployment.local.formation import Formation
from valkey_fuzzer.deployment.local.models import DeploymentConfig, DeploymentMode, InstanceInfo
from valkey_fuzzer.deployment.local.planner import plan_deployment
from valkey_fuzzer.deployment.local.processes import ProcessManager
from valkey_fuzzer.deployment.local.queries import parse_cluster_info, parse_replication
from valkey_fuzzer.deployment.local.topology import DeploymentConnection, TopologyNode
from valkey_fuzzer.deployment.local.validation.snapshot import Snapshot

PROCESSES = "valkey_fuzzer.deployment.local.processes"


def instance(
    instance_id: str = "valkey-0", role: str = "primary", port: int = 7000, *, pid: Optional[int] = None, **fields: Any
) -> InstanceInfo:
    """A running instance in shard 0 whose process is a ``Mock`` with ``pid`` (default: the port)."""
    process = Mock(pid=port if pid is None else pid)
    process.poll.return_value = None
    fields.setdefault("shard_id", 0)
    fields.setdefault("log_file", "")
    return InstanceInfo(
        instance_id=instance_id,
        role=role,
        port=port,
        bus_port=port + 10000,
        pid=process.pid,
        process=process,
        data_dir=f"/tmp/{instance_id}",
        **fields,
    )


def node_row(
    node_id: str,
    port: int,
    role: str = "primary",
    shard_id: int = 0,
    status: str = "connected",
    primary_id: Optional[str] = None,
    instance_id: Optional[str] = None,
    **fields: Any,
) -> TopologyNode:
    """One entry of a deployment's current view, as ``DeploymentConnection.get_current_nodes`` returns it.

    ``status`` "failed" is the cluster's FAIL flag; "pfail" is failed in the viewer's eyes only (``fail?``).
    """
    return TopologyNode(
        node_id=node_id,
        instance_id=instance_id,
        host="127.0.0.1",
        port=port,
        role=role,
        primary_id=primary_id,
        shard_id=shard_id,
        failed=status in ("failed", "pfail"),
        marked_fail=status == "failed",
        **fields,
    )


def deployment_view(*rows: TopologyNode, mode: DeploymentMode = DeploymentMode.CLUSTER) -> Mock:
    """A deployment connection whose current view is ``rows``."""
    deployment = Mock(mode=mode, instances=[])
    deployment.get_current_nodes.return_value = list(rows)
    return deployment


def client_context(client: Mock) -> Mock:
    """A ``valkey_client(...)`` context manager that yields ``client``."""
    context = Mock()
    context.__enter__ = Mock(return_value=client)
    context.__exit__ = Mock(return_value=False)
    return context


def fake_client(info: Optional[dict] = None, replies: Any = None, read: Any = None) -> Mock:
    """A client answering INFO with ``info``, commands with ``replies``, and pipelined GETs with ``read(key)``."""
    client = Mock()
    client.info.return_value = info or {}
    client.execute_command.side_effect = replies
    queued: list[str] = []

    def execute(raise_on_error: bool = True) -> list:
        values = [read(key) for key in queued]
        queued.clear()
        return values

    client.pipeline.return_value.get.side_effect = queued.append
    client.pipeline.return_value.execute.side_effect = execute
    return client


def process_manager(
    tmp_path=None, *, mode=DeploymentMode.STANDALONE, replicas_per_primary=0, seed=None, **config
) -> ProcessManager:
    """A process manager for a one-primary deployment, with its data under ``tmp_path`` when given."""
    if tmp_path is not None:
        config.setdefault("base_data_dir", str(tmp_path))
    config.setdefault("valkey_binary", "/opt/valkey-server")
    config.setdefault("primaries", 1)
    return ProcessManager(DeploymentConfig(mode=mode, replicas_per_primary=replicas_per_primary, **config), seed)


@contextmanager
def formed_cluster(primaries: int, replicas_per_primary: int, base_port: int, **config: Any) -> Iterator[Any]:
    """A formed cluster of real valkey-server processes (``processes``, ``formation``, ``connection``, ``nodes``).

    Without a ``valkey_binary`` it uses the live tests' build, and skips when there is none: the deployment's own
    fallback would clone and build Valkey into a directory every user of the host shares.
    """
    if "valkey_binary" not in config:
        if not BINARY:
            pytest.skip("valkey-server is not available (set VALKEY_FUZZER_BINARY or put it on PATH)")
        config["valkey_binary"] = BINARY
    deployment = DeploymentConfig(
        primaries=primaries, replicas_per_primary=replicas_per_primary, base_port=base_port, **config
    )
    processes = ProcessManager(deployment)
    formation = Formation()
    nodes = processes.spawn_all(plan_deployment(deployment))
    try:
        formation.form(nodes)
        connection = DeploymentConnection(nodes, processes.deployment_id)
        yield SimpleNamespace(processes=processes, formation=formation, connection=connection, nodes=nodes)
    finally:
        formation.close_connections()
        processes.cleanup_deployment(nodes)


# Seed 38 of cluster_chaos.yaml: four shards of three, valkey-3k planned as the primary of shard k. valkey-0 and
# valkey-1 were killed, valkey-2 took shard 0, replica migration moved valkey-4 from shard 1 into shard 0 and a FORCE
# failover promoted it; valkey-9 was killed and valkey-11 took shard 3.
MIGRATED_SLOTS = {0: (0, 4095), 1: (4096, 8191), 2: (8192, 12287), 3: (12288, 16383)}
MIGRATED_TOPOLOGY = """n0 127.0.0.1:7000@17000 master,fail - 0 0 1 disconnected
n1 127.0.0.1:7001@17001 slave,fail n0 0 0 1 disconnected
n2 127.0.0.1:7002@17002 slave n4 0 0 14 connected
n3 127.0.0.1:7003@17003 myself,master - 0 0 2 connected 4096-8191
n4 127.0.0.1:7004@17004 master - 0 0 14 connected 0-4095
n5 127.0.0.1:7005@17005 slave n3 0 0 2 connected
n6 127.0.0.1:7006@17006 master - 0 0 3 connected 8192-12287
n7 127.0.0.1:7007@17007 slave n6 0 0 3 connected
n8 127.0.0.1:7008@17008 slave n6 0 0 3 connected
n9 127.0.0.1:7009@17009 master,fail - 0 0 4 disconnected
n10 127.0.0.1:7010@17010 slave n11 0 0 15 connected
n11 127.0.0.1:7011@17011 master - 0 0 15 connected 12288-16383"""


def planned_cluster_instances() -> list[InstanceInfo]:
    """The planned instances of the four-shard cluster ``MIGRATED_TOPOLOGY`` started as."""
    instances = []
    for index in range(12):
        shard, position = divmod(index, 3)
        start, end = MIGRATED_SLOTS[shard]
        slots = {"slot_start": start, "slot_end": end} if position == 0 else {}
        role = "primary" if position == 0 else "replica"
        instances.append(instance(f"valkey-{index}", role, port=7000 + index, shard_id=shard, **slots))
    return instances


def migrated_connection() -> DeploymentConnection:
    """A connection to the planned cluster; patch ``query_cluster_nodes`` to give it a live topology."""
    return DeploymentConnection(instances=planned_cluster_instances(), deployment_id="seed-38")


class FakeCluster:
    """Valkey servers for ``valkey.Valkey`` to connect to: a formed cluster of primaries and their replicas.

    ``nodes`` maps each port to ``(node_id, primary port or None, slots)``. Every
    node answers PING, INFO replication, CLUSTER INFO, and CLUSTER NODES as a
    healthy cluster does, and ``reads`` counts each port's commands.
    """

    def __init__(self, nodes: dict[int, tuple[str, Optional[int], str]]):
        self.nodes = nodes
        self.reads: dict[tuple[int, str], int] = {}

    def _cluster_nodes(self, me: int) -> str:
        lines = []
        for port, (node_id, primary, slots) in self.nodes.items():
            flags = ("myself," if port == me else "") + ("slave" if primary else "master")
            primary_id = self.nodes[primary][0] if primary else "-"
            lines.append(
                f"{node_id} 127.0.0.1:{port}@{port + 10000} {flags} {primary_id} 0 0 1 connected {slots}".rstrip()
            )
        return "\n".join(lines) + "\n"

    def client(self, host: str = "127.0.0.1", port: int = 0, **kwargs: Any) -> Mock:
        cluster = self

        def count(what: str) -> None:
            cluster.reads[(port, what)] = cluster.reads.get((port, what), 0) + 1

        def info(section: str = "") -> dict[str, Any]:
            count(f"INFO {section}")
            _, primary, _ = cluster.nodes[port]
            if primary:
                return {"role": "slave", "master_host": "127.0.0.1", "master_port": primary, "master_link_status": "up",
                        "master_last_io_seconds_ago": 0, "master_sync_in_progress": 0, "slave_repl_offset": 100,
                        "master_repl_offset": 100}  # fmt: skip
            replicas = sum(other == port for _, other, _ in cluster.nodes.values())
            return {"role": "master", "connected_slaves": replicas, "master_repl_offset": 100}

        def execute_command(*args: Any) -> Any:
            count(" ".join(str(arg) for arg in args[:2]))
            if args[:2] == ("CLUSTER", "NODES"):
                return cluster._cluster_nodes(port)
            if args[:2] == ("CLUSTER", "INFO"):
                return "cluster_state:ok\r\ncluster_slots_assigned:16384\r\ncluster_known_nodes:6\r\n"
            raise AssertionError(f"unexpected command {args}")

        client = Mock(info=Mock(side_effect=info), execute_command=Mock(side_effect=execute_command))
        client.ping.return_value = True
        return client


def snapshot_of(
    *rows: TopologyNode,
    mode: DeploymentMode = DeploymentMode.CLUSTER,
    instances: Sequence[InstanceInfo] = (),
    replication: Optional[dict[int, dict[str, Any]]] = None,
    cluster_info: Optional[dict[int, str]] = None,
    cluster_nodes: Optional[dict[int, str]] = None,
    primaries: Optional[dict[int, str]] = None,
) -> Snapshot:
    """A validation snapshot of ``rows``; replies are by port. ``primaries`` maps a replica's port to the node ID
    of the primary its own CLUSTER NODES names."""
    views = {port: parse_cluster_nodes(text) for port, text in (cluster_nodes or {}).items()}
    for port, primary_id in (primaries or {}).items():
        row = next(row for row in rows if row.port == port)
        views[port] = parse_cluster_nodes(f"{row.node_id} 127.0.0.1:{port}@1 myself,slave {primary_id} 0 0 1 connected")
    return Snapshot(
        mode=mode,
        instances=list(instances),
        rows=list(rows),
        replication={f"127.0.0.1:{port}": parse_replication(info) for port, info in (replication or {}).items()},
        cluster_info={f"127.0.0.1:{port}": parse_cluster_info(text) for port, text in (cluster_info or {}).items()},
        cluster_nodes={f"127.0.0.1:{port}": view for port, view in views.items()},
    )
