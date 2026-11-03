"""Data consistency: test keys written before a fault, read back from primaries and replicas."""

import logging
import random
import string
from collections.abc import Mapping
from contextlib import ExitStack
from typing import Callable, Optional

import valkey
from valkey.cluster import ClusterNode
from valkey.crc import key_slot

from ..cluster_nodes import HASH_SLOTS
from ..models import DeploymentMode
from ..queries import valkey_client
from ..topology import DeploymentConnection, TopologyNode
from .config import DataConsistencyValidationConfig
from .results import DataConsistencyValidation, DataInconsistency
from .snapshot import Snapshot

logger = logging.getLogger(__name__)


def _find_primary(live_instances: list[TopologyNode]) -> Optional[TopologyNode]:
    return next((node for node in live_instances if node.role == "primary"), None)


def _slot_owners(rows: list[TopologyNode]) -> dict[int, str]:
    """Each assigned slot's primary, by address, from the topology's parsed CLUSTER NODES."""
    return {slot: row.address for row in rows if row.role == "primary" for slot in row.slots or ()}


def _cluster_test_keys(
    live_instances: list[TopologyNode], slot_owners: dict[int, str], config: DataConsistencyValidationConfig
) -> list[str]:
    """Deterministic test keys, at least one on every live primary, in the order of their addresses."""
    owners = sorted(node.address for node in live_instances if node.role == "primary")
    if config.num_test_keys < len(owners):
        raise ValueError(
            f"num_test_keys={config.num_test_keys} is insufficient for "
            f"{len(owners)} live cluster primaries; at least one key per primary is required"
        )
    slotless = sorted(set(owners) - set(slot_owners.values()))
    if slotless:
        raise RuntimeError(f"Live cluster primaries own no slots: {slotless}")
    elsewhere = sorted(set(slot_owners.values()) - set(owners))
    if elsewhere:
        raise RuntimeError(f"Cluster slots are assigned to non-live primaries: {elsewhere}")

    keys_by_owner: dict[str, str] = {}
    candidate_index = 0
    max_candidates = max(HASH_SLOTS, config.num_test_keys * 1024)
    while len(keys_by_owner) < len(owners) and candidate_index < max_candidates:
        candidate = f"{config.key_prefix}{candidate_index}"
        owner = slot_owners.get(key_slot(candidate.encode()))
        if owner in owners and owner not in keys_by_owner:
            keys_by_owner[owner] = candidate
        candidate_index += 1
    missing_owners = set(owners) - set(keys_by_owner)
    if missing_owners:
        raise RuntimeError(f"Unable to generate validation keys for cluster primaries: {sorted(missing_owners)}")

    selected_keys = [keys_by_owner[owner] for owner in owners]
    selected_key_set = set(selected_keys)
    candidate_index = 0
    while len(selected_keys) < config.num_test_keys:
        candidate = f"{config.key_prefix}{candidate_index}"
        if candidate not in selected_key_set:
            selected_keys.append(candidate)
            selected_key_set.add(candidate)
        candidate_index += 1
    return selected_keys


def write_test_keys(
    connection: DeploymentConnection, config: DataConsistencyValidationConfig, rng: random.Random
) -> dict[str, str]:
    """Write ``num_test_keys`` keys with values drawn from ``rng``; raise RuntimeError unless every write succeeded.

    In cluster mode, with the cross-replica check on, every live primary gets
    at least one key, so every replica has a key of its shard to hold.
    """
    rows = connection.get_current_nodes(include_failed=True)
    live_instances = [row for row in rows if not row.failed]
    if not live_instances:
        raise RuntimeError("no live nodes to write the data-consistency test keys to")
    mode = connection.mode
    written: dict[str, str] = {}
    with ExitStack() as client_stack:
        if mode == DeploymentMode.CLUSTER:
            test_keys = _cluster_test_keys(live_instances, _slot_owners(rows), config)
            data_client = valkey.ValkeyCluster(
                startup_nodes=[ClusterNode(host=node.host, port=node.port) for node in live_instances[:3]],
                decode_responses=True,
                skip_full_coverage_check=True,
                socket_timeout=config.timeout,
            )
            client_stack.callback(data_client.close)
        else:
            primary = _find_primary(live_instances)
            if primary is None:
                raise RuntimeError(f"no live primary in the {mode.value} deployment to write the test keys to")
            data_client = client_stack.enter_context(valkey_client(primary.host, primary.port, config.timeout))
            test_keys = [f"{config.key_prefix}{i}" for i in range(config.num_test_keys)]

        for key in test_keys:
            value = "".join(rng.choices(string.ascii_letters + string.digits, k=32))
            try:
                if not data_client.set(key, value):
                    raise RuntimeError("SET was not acknowledged")
                written[key] = value
            except Exception as e:
                logger.warning("Failed to write test key %s: %s", key, e)

    if len(written) != config.num_test_keys:
        raise RuntimeError(
            f"wrote {len(written)} of {config.num_test_keys} data-consistency test keys to the {mode.value} "
            "deployment; see the log for each failed write"
        )
    logger.info("Successfully wrote %s test keys to the %s deployment", len(written), mode.value)
    return written


def _check_test_keys(
    test_keys: Mapping[str, str],
    read: Callable[[str], tuple[Optional[str], str]],
    missing_keys: list[str],
    inconsistent_keys: list[DataInconsistency],
    unreachable_keys: list[str],
) -> None:
    """Read back every test key; ``read(key)`` returns its value and where the value was read."""
    for key, expected_value in test_keys.items():
        try:
            actual_value, source = read(key)
        except Exception as e:
            logger.warning("Error validating key %s: %s", key, e)
            unreachable_keys.append(key)
            continue
        if actual_value is None:
            missing_keys.append(key)
            logger.warning("Test key %s is missing from %s", key, source)
        elif actual_value != expected_value:
            inconsistent_keys.append(
                DataInconsistency(
                    key=key,
                    inconsistency_type="value_mismatch",
                    expected_value=expected_value,
                    actual_values={source: actual_value},
                )
            )
            logger.warning(
                "Test key %s has wrong value: expected %s, got %s", key, expected_value[:16], actual_value[:16]
            )


def check_data(
    snapshot: Snapshot, test_keys: Mapping[str, str], config: DataConsistencyValidationConfig
) -> DataConsistencyValidation:
    """Read every test key back from its primary, and with the cross-replica check from every replica.

    A key that could not be read was not checked: with no key missing or
    wrong, the result is inconclusive. So is a check with no keys or no live
    node to read them from; the other checks judge a deployment with no node.
    """
    if not test_keys:
        logger.warning("No test keys to validate - data consistency check cannot run")
        return DataConsistencyValidation(
            success=False,
            inconclusive=True,
            error_message="No test keys available - data seeding failed or was skipped",
        )
    live_instances = snapshot.live_rows
    if not live_instances:
        return DataConsistencyValidation(
            success=False, inconclusive=True, error_message="No live nodes available to validate data"
        )

    missing_keys: list[str] = []
    inconsistent_keys: list[DataInconsistency] = []
    unreachable_keys: list[str] = []
    mode = snapshot.mode
    if mode == DeploymentMode.CLUSTER:
        startup_nodes = [ClusterNode(host=node.host, port=node.port) for node in live_instances[:3]]
        try:
            cluster_client = valkey.ValkeyCluster(
                startup_nodes=startup_nodes,
                decode_responses=True,
                skip_full_coverage_check=True,
                read_from_replicas=False,
                socket_timeout=config.timeout,
            )
            try:
                _check_test_keys(
                    test_keys,
                    lambda key: (cluster_client.get(key), f"slot_{key_slot(key.encode())}"),
                    missing_keys,
                    inconsistent_keys,
                    unreachable_keys,
                )
            finally:
                cluster_client.close()
        except Exception as e:
            logger.error("Cannot read the test keys through a cluster client: %s", e)
            unreachable_keys.extend(key for key in test_keys if key not in unreachable_keys)
    else:
        primary = _find_primary(live_instances)
        if primary is None:
            return DataConsistencyValidation(
                success=False,
                inconclusive=True,
                unreachable_keys=list(test_keys),
                error_message=f"No live primary available in {mode.value} deployment",
            )
        primary_address = primary.address
        try:
            with valkey_client(primary.host, primary.port, config.timeout) as direct_client:
                _check_test_keys(
                    test_keys,
                    lambda key: (direct_client.get(key), primary_address),
                    missing_keys,
                    inconsistent_keys,
                    unreachable_keys,
                )
        except Exception as e:
            logger.error("Failed to connect to primary %s: %s", primary_address, e)
            unreachable_keys.extend(key for key in test_keys if key not in unreachable_keys)

    _check_replicas(snapshot, test_keys, config, inconsistent_keys, unreachable_keys)

    error_messages = []
    if missing_keys:
        error_messages.append(
            f"Data loss detected: {len(missing_keys)} test key(s) missing from cluster. "
            f"Keys: {missing_keys[:10]}{'...' if len(missing_keys) > 10 else ''}"
        )
    if inconsistent_keys:
        error_messages.append(f"Data inconsistency detected: {len(inconsistent_keys)} key(s) have wrong values")
    if unreachable_keys:
        error_messages.append(f"{len(unreachable_keys)} test key(s) could not be read, so were not checked")
    error_message = "; ".join(error_messages) or None
    if error_message:
        logger.error(error_message)
    logger.info(
        "Data consistency validation: %s keys checked, %s missing, %s inconsistent, %s unreachable",
        len(test_keys),
        len(missing_keys),
        len(inconsistent_keys),
        len(unreachable_keys),
    )
    return DataConsistencyValidation(
        success=not error_messages,
        inconclusive=bool(unreachable_keys) and not (missing_keys or inconsistent_keys),
        test_keys_checked=len(test_keys),
        missing_keys=missing_keys,
        inconsistent_keys=inconsistent_keys,
        unreachable_keys=unreachable_keys,
        error_message=error_message,
    )


def _read_keys(client, keys: list[str], batch_size: int = 1000) -> list:
    """GET every key over a non-transactional pipeline; a per-key error comes back as its exception."""
    values: list = []
    for start in range(0, len(keys), batch_size):
        pipeline = client.pipeline(transaction=False)
        for key in keys[start : start + batch_size]:
            pipeline.get(key)
        values.extend(pipeline.execute(raise_on_error=False))
    if len(values) != len(keys):
        raise RuntimeError(f"Pipelined read returned {len(values)} values for {len(keys)} keys")
    return values


def _check_replicas(
    snapshot: Snapshot,
    test_keys: Mapping[str, str],
    config: DataConsistencyValidationConfig,
    inconsistent_keys: list[DataInconsistency],
    unreachable_keys: list[str],
) -> None:
    """Read every key of each live replica's primary from the replica itself.

    Failing to inspect the topology is itself a validation failure, and raises.
    """
    all_nodes = snapshot.live_rows
    replica_nodes = [node for node in all_nodes if node.role == "replica"]
    if not replica_nodes:
        logger.debug("No replicas to check for cross-replica consistency")
        return

    cluster = snapshot.mode == DeploymentMode.CLUSTER
    sorted_keys = sorted(test_keys)
    for replica in replica_nodes:
        replica_address = replica.address
        if cluster:
            primary = next(
                (node for node in all_nodes if node.role == "primary" and node.node_id == replica.primary_id),
                None,
            )
            if primary is None:
                raise RuntimeError(f"No live primary found for cluster replica {replica_address}")
            owned = set(primary.slots or ())
            replica_keys = [key for key in sorted_keys if key_slot(key.encode()) in owned]
        else:
            # Non-cluster replication has one keyspace: every key belongs on every replica.
            replica_keys = sorted_keys
        if not replica_keys:
            raise RuntimeError(
                f"No seeded validation key is owned by cluster replica {replica_address}'s "
                f"primary; cross-replica coverage is incomplete"
            )

        try:
            with valkey_client(replica.host, replica.port, config.timeout) as client:
                if cluster:
                    # Cluster replicas refuse direct reads without READONLY, even for their own shard's keys.
                    client.execute_command("READONLY")
                values = _read_keys(client, replica_keys)
        except Exception as e:
            values = [e] * len(replica_keys)
            logger.warning("Error reading test keys from replica %s: %s", replica_address, e)

        for key, actual_value in zip(replica_keys, values):
            expected_value = test_keys[key]
            if isinstance(actual_value, Exception):
                if key not in unreachable_keys:
                    unreachable_keys.append(key)
                logger.warning("Key %s could not be read from replica %s: %s", key, replica_address, actual_value)
                continue
            if actual_value is None:
                kind, shown = "missing_on_replica", "None"
            elif actual_value != expected_value:
                kind, shown = "replica_divergence", actual_value
            else:
                continue
            inconsistent_keys.append(DataInconsistency(key, kind, expected_value, {replica_address: shown}))
            logger.warning("Key %s on replica %s: %s", key, replica_address, kind)
