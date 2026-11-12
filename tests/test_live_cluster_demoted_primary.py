"""Reproducer: a just-demoted primary never fails over if its new primary dies before the first sync.

``replicationUnsetPrimary()`` resets ``repl_down_since`` to 0 on demotion and
only a completed sync sets it again, so ``clusterHandleReplicaFailover()``
sees decades of data age and refuses ("Disconnected from primary for longer
than allowed"). With ``requirepass`` set on the promoted replica R, the
demoted primary P cannot authenticate its ``PSYNC``, then R is killed; the
control, first and on another shard, is the same failover without ``requirepass``. The reproducer is an
expected failure (``XFAIL``, with P's refusal as evidence) while the bug
exists. Run only these with ``pytest -m live -k demoted_primary``.
"""

import signal
import time

import pytest

from tests.deployment_support import formed_cluster
from tests.live_support import BINARY, free_port_block
from valkey_fuzzer.deployment.local.queries import valkey_client

pytestmark = [pytest.mark.live, pytest.mark.slow]

if not BINARY:
    pytest.skip("valkey-server is not available", allow_module_level=True)

REFUSAL = "Disconnected from primary for longer than allowed"
# Failure detection, the failover delay, and an election take about 3 s (cluster-node-timeout 1 s).
TAKEOVER_TIMEOUT_S = 20.0


def _query(node, *command):
    try:
        with valkey_client(node.host, node.port, timeout=1.0, decode_responses=True) as client:
            return client.info("replication") if command == ("INFO",) else client.execute_command(*command)
    except Exception:
        return {} if command == ("INFO",) else ""


def _owns_slots(node):
    myself = next((line.split() for line in _query(node, "CLUSTER", "NODES").splitlines() if "myself" in line), [])
    return len(myself) > 8 and "master" in myself[2]


def _wait(predicate, timeout_s):
    deadline = time.monotonic() + timeout_s
    while not predicate():
        if time.monotonic() > deadline:
            return False
        time.sleep(0.1)
    return True


def _fail_over_then_kill_the_new_primary(nodes, shard, *, block_first_sync):
    """Fail ``shard`` over to its replica R, wait until the old primary P follows R, SIGKILL R; P and R."""

    primary, replica = (
        next(n for n in nodes if n.role == role and n.shard_id == shard) for role in ("primary", "replica")
    )
    with valkey_client(replica.host, replica.port, timeout=5.0, decode_responses=True) as client:
        if block_first_sync:
            # P has no primaryauth: once demoted, its PSYNC to R gets NOAUTH.
            client.execute_command("CONFIG", "SET", "requirepass", "first-sync-blocker")
        client.execute_command("CLUSTER", "FAILOVER")
    info = lambda: _query(primary, "INFO")  # noqa: E731
    assert _wait(lambda: info().get("role") == "slave" and int(info().get("master_port") or 0) == replica.port, 30)
    assert block_first_sync or _wait(lambda: info().get("master_link_status") == "up", 30), (
        "the first sync never finished"
    )
    replica.process.send_signal(signal.SIGKILL)
    replica.process.wait(timeout=10)
    return primary, replica


def test_a_demoted_primary_takes_its_slots_back_when_its_new_primary_dies_before_the_first_sync(tmp_path):
    base_port = free_port_block(6)
    with formed_cluster(3, 1, base_port, valkey_binary=BINARY, base_data_dir=str(tmp_path)) as formed:
        # Control: a demoted primary that synced takes its slots back.
        primary, _ = _fail_over_then_kill_the_new_primary(formed.nodes, 1, block_first_sync=False)
        assert _wait(lambda: _owns_slots(primary), TAKEOVER_TIMEOUT_S), "the synced old primary did not fail over"

        primary, replica = _fail_over_then_kill_the_new_primary(formed.nodes, 0, block_first_sync=True)
        if _wait(lambda: _owns_slots(primary), TAKEOVER_TIMEOUT_S):
            return  # This Valkey build has the fix.
        with open(primary.log_file, encoding="utf-8", errors="replace") as log:
            refusals = [line.strip() for line in log if REFUSAL in line]
    # Not taking over for any other reason is a broken reproduction, not the bug.
    assert refusals, f"{primary.instance_id} did not fail over and did not log {REFUSAL!r}"
    pytest.xfail(f"Valkey bug reproduced: {primary.instance_id} did not take back the slots of {replica.instance_id} "
                 f"within {TAKEOVER_TIMEOUT_S:.0f}s of its SIGKILL; last refusal: {refusals[-1]}")  # fmt: skip
