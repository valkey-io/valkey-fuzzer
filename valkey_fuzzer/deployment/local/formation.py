"""Formation: replication and cluster setup for spawned instances, and their health checks.

A :class:`Formation` lives for one provision: its clients are closed when :meth:`Formation.form` ends,
and its waits end at the run's deadline.
"""

import logging
from typing import Callable, Optional

import valkey

from ...protocols import RunContext
from .cluster_nodes import HASH_SLOTS, parse_cluster_nodes
from .models import DeploymentMode, InstanceInfo, LocalDeploymentError
from .processes import verify_instance_process_identity
from .queries import WaitInterruptedError, pause, query_cluster_info, query_replication, wait_until

logger = logging.getLogger(__name__)

# How long a formed deployment may take to look healthy. Under a soak's load a slow peer is marked down
# (cluster-node-timeout 1000) and a node reports cluster_state fail well past 30 s although every slot is
# assigned; the health wait outlasts that window, as the meet, slot and instance-start waits do, and a healthy
# deployment is no slower for the margin.
SETTLE_TIMEOUT_S = 60.0


class Formation:
    """Sets up replication or cluster mode for the instances a ProcessManager spawned, and checks their health."""

    def __init__(self, context: Optional[RunContext] = None):
        self.connections: dict[str, valkey.Valkey] = {}
        self._context = context

    def get_client(self, instance: InstanceInfo) -> valkey.Valkey:
        """Get or create the cached client for an instance."""
        if instance.instance_id not in self.connections:
            self.connections[instance.instance_id] = valkey.Valkey(
                host=instance.host, port=instance.port, socket_timeout=5, decode_responses=True
            )
        return self.connections[instance.instance_id]

    def cluster_meet(self, nodes: list[InstanceInfo], timeout: float = SETTLE_TIMEOUT_S) -> None:
        """Connect cluster nodes and wait for convergence"""
        if len(nodes) < 2:
            return

        first_node = nodes[0]
        first_node_client = self.get_client(first_node)

        logger.info(
            "Connecting %s nodes with CLUSTER MEET using %s as starting node", len(nodes), first_node.instance_id
        )

        for node in nodes[1:]:
            logger.debug("Meeting %s (port %s)", node.instance_id, node.port)
            first_node_client.execute_command("CLUSTER", "MEET", node.host, node.port)
            pause(self._context, 0.1, "CLUSTER MEET")

        logger.info("CLUSTER MEET complete")

        expected_count = len(nodes)
        logger.info("Waiting for cluster convergence for all %s nodes (timeout: %.2fs)", expected_count, timeout)

        def converged() -> bool:
            return all(
                int(query_cluster_info(self.get_client(node)).get("cluster_known_nodes", 0)) >= expected_count
                for node in nodes
            )

        retry = (valkey.ValkeyError, OSError)
        if not wait_until(
            converged, timeout=timeout, interval=1.0, what="cluster meet", retry=retry, context=self._context
        ):
            raise LocalDeploymentError(
                f"the {expected_count} cluster nodes did not all learn of each other within {timeout:.2f}s of "
                "CLUSTER MEET; check the instance logs for cluster bus errors"
            )
        logger.info("All nodes see each other and are a part of the cluster")

    def reset_cluster_state(self, nodes: list[InstanceInfo]) -> None:
        """Reset cluster state on all nodes"""
        logger.debug("Resetting cluster state")
        for node in nodes:
            client = self.get_client(node)
            # CLUSTER RESET HARD wipes a node's identity and data. Only ever send
            # it to a process this deployment started.
            verify_instance_process_identity(node, client)
            client.execute_command("CLUSTER", "RESET", "HARD")
        pause(self._context, 1.0, "CLUSTER RESET")

    def assign_and_verify_slots(self, nodes: list[InstanceInfo]) -> dict[str, str]:
        """Assign hash slots to primary nodes and verify assignment"""
        primary_nodes = [node for node in nodes if node.role == "primary"]

        if not primary_nodes:
            logger.info("No primary nodes to assign slots to")
            return {}

        logger.info("Assigning slots to %s primaries", len(primary_nodes))

        node_ids = {}

        for primary in primary_nodes:
            client = self.get_client(primary)

            slots = list(range(primary.slot_start, primary.slot_end + 1))
            logger.debug("Assigning slots %s-%s to %s", primary.slot_start, primary.slot_end, primary.instance_id)

            client.execute_command("CLUSTER", "ADDSLOTS", *slots)

            cluster_node_id = client.execute_command("CLUSTER", "MYID")
            node_ids[primary.instance_id] = cluster_node_id
            primary.cluster_node_id = cluster_node_id

            logger.debug("%s cluster ID: %s", primary.instance_id, cluster_node_id[:8])

        logger.info("Slot assignment complete")

        # Every slot assigned and none failed, as the first node sees it: checked every 2 s for 60 s. Under a
        # soak's load a primary can look down for a while (cluster-node-timeout 1000), leaving its slots marked
        # failing well past 20 s although every slot is assigned; a healthy cluster is no slower for the margin.
        seen: dict[str, str] = {}

        def assigned() -> bool:
            seen.update(query_cluster_info(self.get_client(nodes[0])))
            return seen.get("cluster_slots_assigned") == str(HASH_SLOTS) and seen.get("cluster_slots_fail") == "0"

        if not wait_until(assigned, timeout=60.0, interval=2.0, what="slot assignment", context=self._context):
            raise LocalDeploymentError(
                f"slot assignment did not propagate within 60s: {seen.get('cluster_slots_assigned')}/{HASH_SLOTS} "
                f"assigned, {seen.get('cluster_slots_fail')} failed; check the instance logs"
            )
        logger.info("Slot assignment verified")
        return node_ids

    def setup_and_sync_replication(
        self, nodes: list[InstanceInfo], primary_ids: dict[str, str], timeout: int = 60
    ) -> None:
        """Configure replication and wait for replicas to sync"""
        replicas = [node for node in nodes if node.role == "replica"]

        if not replicas:
            logger.info("No replicas to configure")
            return

        logger.info("Configuring %s replicas", len(replicas))

        for replica in replicas:
            primary_cluster_id = primary_ids.get(replica.primary_id)
            if not primary_cluster_id:
                raise LocalDeploymentError(f"{replica.instance_id}'s primary {replica.primary_id!r} has no cluster ID")

            logger.debug("Configuring %s to replicate %s", replica.instance_id, replica.primary_id)

            client = self.get_client(replica)
            client.execute_command("CLUSTER", "REPLICATE", primary_cluster_id)
            replica.cluster_node_id = client.execute_command("CLUSTER", "MYID")

            logger.debug("Replication configured")

        logger.info("Waiting for %s replicas to sync (timeout: %.2fs)", len(replicas), timeout)
        # Each replica is judged on its own: one seen with its link up is done, so a slow INFO from another
        # (an overloaded host) does not throw away every link already seen up and fail the next round too.
        pending = {replica.instance_id: replica for replica in replicas}

        def linked() -> bool:
            for instance_id, replica in list(pending.items()):
                try:
                    if query_replication(self.get_client(replica)).link_up:
                        del pending[instance_id]
                except (valkey.ValkeyError, OSError) as exc:
                    logger.debug("%s: link not seen up yet: %s", instance_id, exc)
            return not pending

        if not wait_until(linked, timeout=timeout, interval=1.0, what="replica links", context=self._context):
            raise LocalDeploymentError(
                f"the replicas' links to their primaries did not come up within {timeout:.2f}s "
                f"({', '.join(sorted(pending))}); check the instance logs for replication errors"
            )
        logger.info("All replicas are in sync with their primary")

    @staticmethod
    def _primary_replica_layout(instances: list[InstanceInfo]) -> tuple[InstanceInfo, list[InstanceInfo]]:
        primaries = [instance for instance in instances if instance.role == "primary"]
        replicas = [instance for instance in instances if instance.role == "replica"]

        if len(primaries) != 1:
            raise ValueError(
                f"A standalone deployment with replicas requires exactly one primary, found {len(primaries)}"
            )
        if len(primaries) + len(replicas) != len(instances):
            raise ValueError("A standalone deployment contains an instance with an unsupported role")

        primary = primaries[0]
        for replica in replicas:
            if replica.primary_id and replica.primary_id != primary.instance_id:
                raise ValueError(f"Replica {replica.instance_id} targets unknown primary {replica.primary_id!r}")
        return primary, replicas

    def setup_primary_replica_replication(self, instances: list[InstanceInfo]) -> None:
        """Configure non-cluster replication using REPLICAOF."""
        primary, replicas = self._primary_replica_layout(instances)
        self.get_client(primary).execute_command("REPLICAOF", "NO", "ONE")

        for replica in replicas:
            logger.debug(
                "Configuring %s to replicate %s at %s:%s",
                replica.instance_id,
                primary.instance_id,
                primary.host,
                primary.port,
            )
            self.get_client(replica).execute_command(
                "REPLICAOF",
                primary.host,
                primary.port,
            )

    def _settled(
        self, problem: Callable[[], Optional[str]], what: str, timeout: float, interval: float
    ) -> Optional[str]:
        """Poll ``problem`` until it reports none (None) or ``timeout`` passes; then the last problem it reported."""
        last: list[Optional[str]] = ["not checked"]

        def healthy() -> bool:
            try:
                last[0] = problem()
            except Exception as exc:
                last[0] = f"{type(exc).__name__}: {exc}"
            return last[0] is None

        wait_until(healthy, timeout=timeout, interval=interval, what=what, context=self._context)
        return last[0]

    def _standalone_problem(self, instance: InstanceInfo) -> Optional[str]:
        role = query_replication(self.get_client(instance)).role
        return None if role == "primary" else f"{instance.instance_id} reports role {role}, not primary"

    def _primary_replica_problem(self, primary: InstanceInfo, replicas: list[InstanceInfo]) -> Optional[str]:
        """Why the primary does not report every replica connected, or a replica's link to it is not up."""
        primary_info = query_replication(self.get_client(primary))
        if primary_info.role != "primary":
            return f"{primary.instance_id} reports role {primary_info.role}, not primary"
        if (primary_info.connected_replicas or 0) < len(replicas):
            return f"{primary.instance_id} reports {primary_info.connected_replicas or 0} of {len(replicas)} replicas connected"
        for replica in replicas:
            info = query_replication(self.get_client(replica))
            if info.role != "replica" or not info.link_up:
                return f"{replica.instance_id} is not a replica with its link up"
            if (info.primary_host, info.primary_port) != (primary.host, primary.port):
                return f"{replica.instance_id} replicates {info.primary_host}:{info.primary_port}, not {primary.instance_id}"
        return None

    def check_replication_links(self, nodes: list[InstanceInfo]) -> bool:
        """Whether every replica's link to its primary is up."""
        for node in nodes:
            try:
                info = query_replication(self.get_client(node))
                if info.role == "replica" and not info.link_up:
                    return False
            except (valkey.ValkeyError, OSError):
                return False

        return True

    def _cluster_problem(self, nodes: list[InstanceInfo]) -> Optional[str]:
        """What keeps the cluster from being healthy right now; None when it is healthy."""
        reports = {}
        for node in nodes:
            info = query_cluster_info(self.get_client(node))
            reports[node.instance_id] = tuple(
                info.get(field) for field in ("cluster_state", "cluster_slots_assigned", "cluster_slots_fail")
            )
        if len(set(reports.values())) > 1:
            views = "; ".join(
                f"{node}: {state}, {assigned}/{HASH_SLOTS}, fail={fail}"
                for node, (state, assigned, fail) in reports.items()
            )
            return f"the nodes disagree ({views})"
        state, assigned, failed = next(iter(reports.values()))
        if (state, assigned, failed) != ("ok", str(HASH_SLOTS), "0"):
            return f"cluster_state is {state} with {assigned}/{HASH_SLOTS} slots assigned and {failed} failed"
        if not self.check_replication_links(nodes):
            return "a replica's link to its primary is down"
        return self._replica_view_problem(nodes)

    def _replica_view_problem(self, nodes: list[InstanceInfo]) -> Optional[str]:
        """A node whose CLUSTER NODES does not yet show a replica as a replica of its primary; None when all do.

        A replica's link is up before the cluster bus has told every node its
        role (a Valkey 7.2 replica's can take seconds), and a failover that
        reads one node's view would find no replica to promote.
        """
        names = {node.cluster_node_id: node.instance_id for node in nodes}
        ids = {node.instance_id: node.cluster_node_id for node in nodes}
        expected = {node.cluster_node_id: ids.get(node.primary_id) for node in nodes if node.role == "replica"}
        for node in nodes:
            rows = {
                row["node_id"]: row
                for row in parse_cluster_nodes(self.get_client(node).execute_command("CLUSTER", "NODES"))
            }
            for replica_id, primary_id in expected.items():
                row = rows.get(replica_id)
                if row is None or not row["is_replica"] or row["primary_id"] != primary_id:
                    return f"{node.instance_id} does not yet see {names[replica_id]} as a replica of {names.get(primary_id)}"
        return None

    def _form(self, nodes: list[InstanceInfo], mode: DeploymentMode) -> Optional[str]:
        """Set up ``mode`` on ``nodes``; then what kept it from becoming healthy, or None."""
        if mode is DeploymentMode.STANDALONE:
            if any(node.role == "replica" for node in nodes):
                self.setup_primary_replica_replication(nodes)
                primary, replicas = self._primary_replica_layout(nodes)
                return self._settled(
                    lambda: self._primary_replica_problem(primary, replicas), "replicas", SETTLE_TIMEOUT_S, 1.0
                )
            if len(nodes) != 1 or nodes[0].role != "primary":
                return "a standalone deployment without replicas is one primary"
            return self._settled(lambda: self._standalone_problem(nodes[0]), "standalone", SETTLE_TIMEOUT_S, 1.0)

        self.reset_cluster_state(nodes)
        self.cluster_meet(nodes)
        primary_ids = self.assign_and_verify_slots(nodes)
        self.setup_and_sync_replication(nodes, primary_ids)
        return self._settled(lambda: self._cluster_problem(nodes), "cluster", SETTLE_TIMEOUT_S, 1.0)

    def form(self, nodes: list[InstanceInfo], mode: DeploymentMode = DeploymentMode.CLUSTER) -> None:
        """Form ``mode`` on the spawned ``nodes`` and wait up to :data:`SETTLE_TIMEOUT_S` for it to be healthy.

        Raises :class:`LocalDeploymentError` naming what failed, or
        :class:`~.queries.WaitInterruptedError` at the run's deadline. The
        clients it opened are closed when it returns or raises.
        """
        mode = DeploymentMode(mode)
        try:
            problem = self._form(nodes, mode)
        except (LocalDeploymentError, WaitInterruptedError):
            raise
        except Exception as exc:
            raise LocalDeploymentError(
                f"could not form the {mode.value} deployment: {type(exc).__name__}: {exc}"
            ) from exc
        finally:
            self.close_connections()
        if problem:
            raise LocalDeploymentError(
                f"the {mode.value} deployment was not healthy within {SETTLE_TIMEOUT_S:.0f}s: {problem}; "
                "check the instance logs"
            )

    def close_connections(self) -> None:
        """Close and discard every cached client, preserving interruptions."""
        failures: list[BaseException] = []
        clients = list(self.connections.values())
        self.connections.clear()

        for client in clients:
            try:
                client.close()
            except BaseException as exc:
                failures.append(exc)
                logger.error("Failed to close Valkey client: %s", exc)

        interrupt = next((exc for exc in failures if not isinstance(exc, Exception)), None)
        if interrupt is not None:
            raise interrupt
