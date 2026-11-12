"""One node's view of the cluster, from CLUSTER INFO and CLUSTER NODES: its epochs, role, slots, and the primaries
it knows."""

from collections.abc import Mapping, Sequence
from typing import Any, Optional

import valkey

from valkey_fuzzer.deployment.local.cluster_nodes import parse_cluster_nodes, parse_slot_ranges
from valkey_fuzzer.deployment.local.queries import parse_cluster_info


def parse_slots(tokens: Sequence[str]) -> list[list[int]]:
    """``["0-5460", "5462"]`` -> ``[[0, 5460], [5462, 5462]]``; migration markers are skipped."""

    return [list(item) for item in parse_slot_ranges(list(tokens))]


def ranges_overlap(left: Sequence[Sequence[int]], right: Sequence[Sequence[int]]) -> bool:
    return any(a <= d and c <= b for a, b in left for c, d in right)


def _epoch(node: Mapping[str, Any]) -> Optional[int]:
    try:
        return int(node["config_epoch"])
    except ValueError:
        return None


def cluster_view(client: Any) -> dict[str, Any]:
    """One node's view: its own epochs and role plus every primary it knows (slots as ranges), and under
    ``nodes`` its CLUSTER NODES as parsed, for :func:`~valkey_fuzzer.deployment.local.cluster_nodes.split_slots`."""

    info = parse_cluster_info(client.execute_command("CLUSTER", "INFO"))
    nodes = parse_cluster_nodes(client.execute_command("CLUSTER", "NODES"))
    myself = next((node for node in nodes if node["is_myself"]), None)
    if myself is None:
        raise RuntimeError("CLUSTER NODES has no myself entry")
    return {
        "node_id": myself["node_id"],
        "current_epoch": int(info.get("cluster_current_epoch", 0)),
        "config_epoch": _epoch(myself),
        "role": "primary" if myself["is_primary"] else "replica",
        "primary_id": myself["primary_id"],
        "slots": parse_slots(myself["slots"]),
        "state": info.get("cluster_state"),
        "primaries": [
            {
                "node_id": node["node_id"],
                "config_epoch": _epoch(node),
                "slots": parse_slots(node["slots"]),
                "flags": node["flags"].split(","),
            }
            for node in nodes
            if node["is_primary"]
        ],
        "nodes": nodes,
    }


def read_view(handle: Any, instance_id: str, timeout: float = 2.0) -> Optional[dict[str, Any]]:
    """``instance_id``'s :func:`cluster_view`; None when it does not answer (a client error). A reply it
    cannot parse raises."""

    with handle.new_client(instance_id, timeout=timeout) as client:
        try:
            return cluster_view(client)
        except (valkey.exceptions.ValkeyError, OSError):
            return None


def view_summary(view: Optional[Mapping[str, Any]]) -> Optional[dict[str, Any]]:
    if view is None:
        return None
    return {key: view[key] for key in ("node_id", "current_epoch", "config_epoch", "role", "slots")}
