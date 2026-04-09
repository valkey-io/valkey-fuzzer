"""
Unit tests for ChaosTargetSelector shard-safety logic.

Covers: shard protection, thread-safety (TOCTOU race), cluster scoping,
topology reconciliation on restart, and unrecord_kill on failed kills.
"""
import threading
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock

from src.chaos_engine.base import ChaosTargetSelector
from src.models import NodeInfo, TargetSelection


def _node(node_id, shard_id, role="replica", port=7000):
    return NodeInfo(
        node_id=node_id,
        role=role,
        shard_id=shard_id,
        port=port,
        bus_port=port + 10000,
        pid=1000 + port,
        process=None,
        data_dir=f"/tmp/{node_id}",
        log_file=f"/tmp/{node_id}.log",
        cluster_node_id=f"cluster-{node_id}",
    )


CLUSTER = "test-cluster"


# ── Basic shard protection ─────────────────────────────────────────────

def test_blocks_last_member_of_shard():
    """Selecting the last surviving member of a shard must be prevented."""
    selector = ChaosTargetSelector()
    nodes = [
        _node("n0", 0, "primary", 7000),
        _node("n1", 0, "replica", 7001),
        _node("n2", 1, "primary", 7002),
        _node("n3", 1, "replica", 7003),
    ]
    selector.update_cluster_topology(CLUSTER, nodes)

    # Kill n0 (shard 0 primary) — n1 is still alive
    target = selector.select_target(CLUSTER, TargetSelection(strategy="random"))
    # Whatever was selected, record it and remove from live nodes
    # Simulate: kill both members of shard 0 except the safety check
    selector.unrecord_kill(CLUSTER, target.node_id)  # undo eager record

    # Manually set up: n0 killed, only n1 left in shard 0
    selector._killed_node_ids[CLUSTER] = {"n0"}
    live_nodes = [_node("n1", 0, "replica", 7001), _node("n2", 1, "primary", 7002), _node("n3", 1, "replica", 7003)]
    selector.update_cluster_topology(CLUSTER, live_nodes)

    # Now try to select with replica_only — n1 is the last member of shard 0
    # It should be skipped; only n3 (shard 1) should be selectable
    for _ in range(20):  # repeat to account for randomness
        selected = selector.select_target(CLUSTER, TargetSelection(strategy="replica_only"))
        assert selected is not None
        assert selected.node_id == "n3", f"Expected n3 but got {selected.node_id}"
        # Undo the eager record for next iteration
        selector.unrecord_kill(CLUSTER, selected.node_id)


def test_returns_none_when_all_candidates_unsafe():
    """When every candidate would kill the last shard member, return None."""
    selector = ChaosTargetSelector()
    nodes = [
        _node("n0", 0, "primary", 7000),
        _node("n1", 0, "replica", 7001),
    ]
    selector.update_cluster_topology(CLUSTER, nodes)

    # Kill n0
    selector._killed_node_ids[CLUSTER] = {"n0"}
    live = [_node("n1", 0, "replica", 7001)]
    selector.update_cluster_topology(CLUSTER, live)

    # n1 is the only candidate and last member of shard 0
    result = selector.select_target(CLUSTER, TargetSelection(strategy="random"))
    assert result is None


def test_offline_members_do_not_count_as_shard_survivors():
    """Nodes already missing from the live topology must not keep a shard "safe"."""
    selector = ChaosTargetSelector()
    nodes = [
        _node("n0", 0, "primary", 7000),
        _node("n1", 0, "replica", 7001),
    ]
    selector.update_cluster_topology(CLUSTER, nodes)

    # Replica is already offline for non-chaos reasons, so n0 is the only live member left.
    selector.update_cluster_topology(CLUSTER, [_node("n0", 0, "primary", 7000)])

    result = selector.select_target(CLUSTER, TargetSelection(strategy="random"))
    assert result is None


# ── Thread-safety: concurrent selection ────────────────────────────────

def test_concurrent_threads_cannot_kill_entire_shard():
    """Two threads selecting simultaneously must not both pick from the
    same shard when it would leave zero survivors.

    Shard 0: n0 (primary) + n1 (replica) — only 2 members.
    Shard 1: n2 (primary) + n3 (replica) — only 2 members.

    If both threads pick from shard 0, both members die.  The eager
    reservation in select_target must prevent this.
    """
    selector = ChaosTargetSelector()
    nodes = [
        _node("n0", 0, "primary", 7000),
        _node("n1", 0, "replica", 7001),
        _node("n2", 1, "primary", 7002),
        _node("n3", 1, "replica", 7003),
    ]
    selector.update_cluster_topology(CLUSTER, nodes)

    results = []
    barrier = threading.Barrier(2)

    def pick():
        barrier.wait()  # force both threads to call select_target ~simultaneously
        target = selector.select_target(CLUSTER, TargetSelection(strategy="random"))
        if target:
            results.append(target)

    # Run many times to catch races
    for _ in range(50):
        # Reset state
        selector._killed_node_ids[CLUSTER] = set()
        selector.cluster_nodes[CLUSTER] = list(nodes)
        results.clear()

        with ThreadPoolExecutor(max_workers=2) as pool:
            pool.submit(pick)
            pool.submit(pick)
            pool.shutdown(wait=True)

        # Both should have gotten a target
        assert len(results) == 2, f"Expected 2 results, got {len(results)}"

        # Check: no shard has both members selected
        shard_selections = {}
        for r in results:
            shard_selections.setdefault(r.shard_id, []).append(r.node_id)

        for shard_id, selected_nodes in shard_selections.items():
            assert len(selected_nodes) <= 1, (
                f"Shard {shard_id} had both members selected: {selected_nodes}"
            )


# ── Cluster scoping ───────────────────────────────────────────────────

def test_kills_scoped_per_cluster():
    """Kills in cluster A must not affect target selection in cluster B."""
    selector = ChaosTargetSelector()

    nodes_a = [_node("n0", 0, "primary", 7000), _node("n1", 0, "replica", 7001)]
    nodes_b = [_node("n0", 0, "primary", 8000), _node("n1", 0, "replica", 8001)]

    selector.update_cluster_topology("cluster-a", nodes_a)
    selector.update_cluster_topology("cluster-b", nodes_b)

    # Kill n0 in cluster-a
    selector.record_kill("cluster-a", "n0")

    # cluster-b should still allow selecting n0
    selected = selector.select_target("cluster-b", TargetSelection(strategy="random"))
    assert selected is not None
    # n0 should be selectable in cluster-b
    selector.unrecord_kill("cluster-b", selected.node_id)


# ── Topology update does NOT clear eager reservations ──────────────────

def test_topology_update_preserves_eager_reservations():
    """update_cluster_topology must NOT clear killed entries, because
    select_target eagerly reserves nodes before the actual kill.  A
    reserved-but-not-yet-killed node still appears in the live list."""
    selector = ChaosTargetSelector()
    nodes = [
        _node("n0", 0, "primary", 7000),
        _node("n1", 0, "replica", 7001),
        _node("n2", 1, "primary", 7002),
    ]
    selector.update_cluster_topology(CLUSTER, nodes)

    # Eagerly reserve n0 (simulates select_target)
    selector.record_kill(CLUSTER, "n0")
    assert "n0" in selector._killed_node_ids[CLUSTER]

    # Topology refresh still shows n0 alive (kill hasn't happened yet)
    selector.update_cluster_topology(CLUSTER, nodes)
    # Reservation must be preserved
    assert "n0" in selector._killed_node_ids[CLUSTER]


# ── unrecord_kill on failed kill ───────────────────────────────────────

def test_unrecord_kill_releases_reservation():
    """After select_target eagerly records a kill, unrecord_kill must
    release it so the node becomes selectable again."""
    selector = ChaosTargetSelector()
    nodes = [
        _node("n0", 0, "primary", 7000),
        _node("n1", 0, "replica", 7001),
    ]
    selector.update_cluster_topology(CLUSTER, nodes)

    # select_target eagerly records the selected node
    selected = selector.select_target(CLUSTER, TargetSelection(strategy="random"))
    assert selected is not None
    assert selected.node_id in selector._killed_node_ids[CLUSTER]

    # Simulate kill failure — release the reservation
    selector.unrecord_kill(CLUSTER, selected.node_id)
    assert selected.node_id not in selector._killed_node_ids[CLUSTER]


# ── Single-member shards (replicas_per_shard=0) ───────────────────────

def test_single_member_shard_blocked_on_first_kill():
    """With replicas_per_shard=0, each shard has only 1 member.
    The very first selection must block it — killing it leaves zero survivors."""
    selector = ChaosTargetSelector()
    nodes = [
        _node("n0", 0, "primary", 7000),
        _node("n1", 1, "primary", 7001),
        _node("n2", 2, "primary", 7002),
    ]
    selector.update_cluster_topology(CLUSTER, nodes)

    # Every node is the sole member of its shard — none should be selectable
    result = selector.select_target(CLUSTER, TargetSelection(strategy="random"))
    assert result is None, f"Expected None but got {result.node_id}"


# ── Reservation release when no chaos fires ────────────────────────────

def test_no_chaos_coordination_does_not_leak_reservations():
    """When all coordination flags are False, select_target is called but
    no chaos is injected.  The eager reservation must be released so
    subsequent operations can still select targets."""
    from unittest.mock import Mock, patch
    from src.fuzzer_engine.chaos_coordinator import ChaosCoordinator
    from src.models import (
        ChaosConfig, ChaosCoordination, ChaosType, ChaosTiming,
        ProcessChaosType, TargetSelection, Operation, OperationType,
        OperationTiming,
    )

    coordinator = ChaosCoordinator(seed=1)

    # 2 shards, 2 nodes each
    nodes = [
        _node("n0", 0, "primary", 7000),
        _node("n1", 0, "replica", 7001),
        _node("n2", 1, "primary", 7002),
        _node("n3", 1, "replica", 7003),
    ]

    # Register nodes
    coordinator.register_cluster_nodes("c1", nodes)

    # Mock cluster connection
    mock_conn = Mock()
    mock_conn.get_live_nodes.return_value = [
        {"node_id": n.node_id, "host": "127.0.0.1", "port": n.port,
         "role": n.role, "shard_id": n.shard_id}
        for n in nodes
    ]
    mock_conn.initial_nodes = nodes

    # All coordination flags False — no chaos should fire
    config = ChaosConfig(
        chaos_type=ChaosType.PROCESS_KILL,
        target_selection=TargetSelection(strategy="random"),
        timing=ChaosTiming(),
        coordination=ChaosCoordination(
            chaos_before_operation=False,
            chaos_during_operation=False,
            chaos_after_operation=False,
        ),
        process_chaos_type=ProcessChaosType.SIGKILL,
    )

    op = Operation(
        type=OperationType.FAILOVER,
        target_node="shard-0-primary",
        parameters={},
        timing=OperationTiming(),
    )

    # Run 10 operations — none should inject chaos, and reservations
    # must not accumulate.
    for _ in range(10):
        results = coordinator.coordinate_chaos_with_operation(
            operation=op,
            chaos_config=config,
            cluster_connection=mock_conn,
            cluster_id="c1",
        )
        # No ChaosResult should be produced (no chaos fired)
        actual = [r for r in results if not isinstance(r, dict)]
        assert len(actual) == 0, f"Expected no chaos results, got {actual}"

    # After 10 operations with no chaos, killed set should be empty
    killed = coordinator.chaos_engine.target_selector._killed_node_ids.get("c1", set())
    assert len(killed) == 0, f"Expected empty killed set, got {killed}"


def test_exception_during_chaos_releases_reservation():
    """If an exception occurs after select_target but before chaos fires,
    the eager reservation must be released."""
    from unittest.mock import Mock, patch, PropertyMock
    from src.fuzzer_engine.chaos_coordinator import ChaosCoordinator
    from src.models import (
        ChaosConfig, ChaosCoordination, ChaosType, ChaosTiming,
        ProcessChaosType, TargetSelection, Operation, OperationType,
        OperationTiming,
    )

    coordinator = ChaosCoordinator(seed=1)

    nodes = [
        _node("n0", 0, "primary", 7000),
        _node("n1", 0, "replica", 7001),
        _node("n2", 1, "primary", 7002),
        _node("n3", 1, "replica", 7003),
    ]
    coordinator.register_cluster_nodes("c1", nodes)

    # Mock connection that works for topology refresh but will cause
    # an exception when we access coordination timing
    mock_conn = Mock()
    mock_conn.get_live_nodes.return_value = [
        {"node_id": n.node_id, "host": "127.0.0.1", "port": n.port,
         "role": n.role, "shard_id": n.shard_id}
        for n in nodes
    ]
    mock_conn.initial_nodes = nodes

    # Config with a timing object that raises on attribute access
    bad_timing = Mock()
    bad_timing.delay_before_operation = PropertyMock(side_effect=RuntimeError("boom"))
    config = ChaosConfig(
        chaos_type=ChaosType.PROCESS_KILL,
        target_selection=TargetSelection(strategy="random"),
        timing=bad_timing,
        coordination=ChaosCoordination(chaos_before_operation=True),
        process_chaos_type=ProcessChaosType.SIGKILL,
    )

    op = Operation(
        type=OperationType.FAILOVER,
        target_node="shard-0-primary",
        parameters={},
        timing=OperationTiming(),
    )

    # This should not raise — the coordinator catches exceptions
    results = coordinator.coordinate_chaos_with_operation(
        operation=op, chaos_config=config,
        cluster_connection=mock_conn, cluster_id="c1",
    )

    # The reservation should have been released despite the exception
    killed = coordinator.chaos_engine.target_selector._killed_node_ids.get("c1", set())
    assert len(killed) == 0, f"Expected empty killed set after exception, got {killed}"


def test_no_safe_target_is_skipped_without_failed_chaos_result():
    """Shard-safety exhaustion should skip chaos instead of recording a failure."""
    from src.fuzzer_engine.chaos_coordinator import ChaosCoordinator
    from src.models import (
        ChaosConfig, ChaosCoordination, ChaosType, ChaosTiming,
        ProcessChaosType, TargetSelection, Operation, OperationType,
        OperationTiming,
    )

    coordinator = ChaosCoordinator(seed=1)
    nodes = [
        _node("n0", 0, "primary", 7000),
        _node("n1", 0, "replica", 7001),
    ]
    coordinator.register_cluster_nodes("c1", nodes)
    coordinator.chaos_engine.target_selector._killed_node_ids["c1"] = {"n1"}

    mock_conn = Mock()
    mock_conn.get_live_nodes.return_value = [
        {"node_id": "n0", "host": "127.0.0.1", "port": 7000, "role": "primary", "shard_id": 0}
    ]
    mock_conn.initial_nodes = nodes

    config = ChaosConfig(
        chaos_type=ChaosType.PROCESS_KILL,
        target_selection=TargetSelection(strategy="primary_only"),
        timing=ChaosTiming(),
        coordination=ChaosCoordination(chaos_during_operation=True),
        process_chaos_type=ProcessChaosType.SIGKILL,
    )
    op = Operation(
        type=OperationType.FAILOVER,
        target_node="shard-0-primary",
        parameters={},
        timing=OperationTiming(),
    )

    results = coordinator.coordinate_chaos_with_operation(
        operation=op,
        chaos_config=config,
        cluster_connection=mock_conn,
        cluster_id="c1",
    )

    assert results == []
    assert coordinator.get_chaos_history() == []


def test_parallel_executor_releases_deferred_reservation_on_operation_exception():
    """Deferred after-operation chaos should unreserve its target if the op raises."""
    from src.fuzzer_engine.chaos_coordinator import ChaosCoordinator
    from src.fuzzer_engine.parallel_executor import ParallelExecutor
    from src.models import (
        ChaosConfig, ChaosCoordination, ChaosType, ChaosTiming,
        ProcessChaosType, TargetSelection, Operation, OperationType,
        OperationTiming,
    )

    coordinator = ChaosCoordinator(seed=1)
    nodes = [
        _node("n0", 0, "primary", 7000),
        _node("n1", 0, "replica", 7001),
        _node("n2", 1, "primary", 7002),
        _node("n3", 1, "replica", 7003),
    ]
    coordinator.register_cluster_nodes("c1", nodes)

    mock_conn = Mock()
    mock_conn.get_live_nodes.return_value = [
        {"node_id": n.node_id, "host": "127.0.0.1", "port": n.port, "role": n.role, "shard_id": n.shard_id}
        for n in nodes
    ]
    mock_conn.initial_nodes = nodes

    operation_orchestrator = Mock()
    operation_orchestrator.execute_operation.side_effect = RuntimeError("boom")
    fuzzer_logger = Mock()
    executor = ParallelExecutor(operation_orchestrator, coordinator, fuzzer_logger)

    config = ChaosConfig(
        chaos_type=ChaosType.PROCESS_KILL,
        target_selection=TargetSelection(strategy="random"),
        timing=ChaosTiming(delay_after_operation=0.0),
        coordination=ChaosCoordination(
            chaos_before_operation=False,
            chaos_during_operation=False,
            chaos_after_operation=True,
        ),
        process_chaos_type=ProcessChaosType.SIGKILL,
    )
    op = Operation(
        type=OperationType.FAILOVER,
        target_node="shard-0-primary",
        parameters={},
        timing=OperationTiming(),
    )

    executor.execute_operations_parallel(
        operations=[op],
        chaos_config=config,
        cluster_connection=mock_conn,
        cluster_id="c1",
    )

    killed = coordinator.chaos_engine.target_selector._killed_node_ids.get("c1", set())
    assert killed == set(), f"Expected deferred reservation to be released, got {killed}"


def test_parallel_executor_preserves_reservation_after_immediate_chaos_success():
    """Operation exceptions must not clear a target already killed before the deferred phase."""
    from unittest.mock import patch
    from src.fuzzer_engine.chaos_coordinator import ChaosCoordinator
    from src.fuzzer_engine.parallel_executor import ParallelExecutor
    from src.models import (
        ChaosConfig, ChaosCoordination, ChaosResult, ChaosType, ChaosTiming,
        ProcessChaosType, TargetSelection, Operation, OperationType,
        OperationTiming,
    )

    coordinator = ChaosCoordinator(seed=1)
    nodes = [
        _node("n0", 0, "primary", 7000),
        _node("n1", 0, "replica", 7001),
        _node("n2", 1, "primary", 7002),
        _node("n3", 1, "replica", 7003),
    ]
    coordinator.register_cluster_nodes("c1", nodes)

    mock_conn = Mock()
    mock_conn.get_live_nodes.return_value = [
        {"node_id": n.node_id, "host": "127.0.0.1", "port": n.port, "role": n.role, "shard_id": n.shard_id}
        for n in nodes
    ]
    mock_conn.initial_nodes = nodes

    operation_orchestrator = Mock()
    operation_orchestrator.execute_operation.side_effect = RuntimeError("boom")
    fuzzer_logger = Mock()
    executor = ParallelExecutor(operation_orchestrator, coordinator, fuzzer_logger)

    config = ChaosConfig(
        chaos_type=ChaosType.PROCESS_KILL,
        target_selection=TargetSelection(strategy="specific", specific_nodes=["n0"]),
        timing=ChaosTiming(delay_before_operation=0.0, delay_after_operation=0.0),
        coordination=ChaosCoordination(
            chaos_before_operation=True,
            chaos_during_operation=False,
            chaos_after_operation=True,
        ),
        process_chaos_type=ProcessChaosType.SIGKILL,
    )
    op = Operation(
        type=OperationType.FAILOVER,
        target_node="shard-0-primary",
        parameters={},
        timing=OperationTiming(),
    )

    success_result = ChaosResult(
        chaos_id="before-success",
        chaos_type=ChaosType.PROCESS_KILL,
        target_node="n0",
        success=True,
        start_time=0.0,
        end_time=0.0,
    )

    with patch.object(coordinator, "_inject_chaos", return_value=success_result):
        executor.execute_operations_parallel(
            operations=[op],
            chaos_config=config,
            cluster_connection=mock_conn,
            cluster_id="c1",
        )

    killed = coordinator.chaos_engine.target_selector._killed_node_ids.get("c1", set())
    assert killed == {"n0"}, f"Expected immediate chaos reservation to remain, got {killed}"


def test_reset_cluster_topology_replaces_stale_snapshot_for_reused_cluster_id():
    """Fresh cluster registration under the same ID should replace stale shard members."""
    selector = ChaosTargetSelector()
    old_nodes = [
        _node("old-0", 0, "primary", 7000),
        _node("old-1", 0, "replica", 7001),
        _node("old-2", 0, "replica", 7002),
    ]
    selector.update_cluster_topology(CLUSTER, old_nodes)

    new_nodes = [
        _node("new-0", 0, "primary", 7100),
        _node("new-1", 0, "replica", 7101),
    ]
    selector.reset_cluster_topology(CLUSTER, new_nodes)
    selector.record_kill(CLUSTER, "new-0")
    selector.update_cluster_topology(CLUSTER, [new_nodes[1]])

    result = selector.select_target(CLUSTER, TargetSelection(strategy="random"))
    assert result is None, "Expected last live member to remain protected after cluster reset"


def test_cleanup_chaos_clears_selector_state():
    """Cluster cleanup should drop cached topology and reservations for that cluster."""
    from src.fuzzer_engine.chaos_coordinator import ChaosCoordinator

    coordinator = ChaosCoordinator()
    nodes = [
        _node("n0", 0, "primary", 7000),
        _node("n1", 0, "replica", 7001),
    ]
    coordinator.register_cluster_nodes("c1", nodes)
    coordinator.chaos_engine.target_selector.record_kill("c1", "n0")

    coordinator.cleanup_chaos("c1")

    assert "c1" not in coordinator.chaos_engine.target_selector.cluster_nodes
    assert "c1" not in coordinator.chaos_engine.target_selector._initial_topology
    assert "c1" not in coordinator.chaos_engine.target_selector._killed_node_ids
