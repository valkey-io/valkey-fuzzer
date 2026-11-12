"""CLUSTER NODES parsing and hash-slot helpers."""

import logging
import string
from collections.abc import Mapping, Sequence
from functools import cache
from itertools import product
from typing import Any, Optional

from valkey.crc import key_slot

logger = logging.getLogger(__name__)

# A cluster's hash slots.
HASH_SLOTS = 16384

# Slot hash tags are tried shortest first, in itertools.product order over these characters.
_SLOT_TAG_ALPHABET = string.digits + string.ascii_lowercase + string.ascii_uppercase


@cache
def slot_hash_tags() -> tuple[str, ...]:
    """For each hash slot, the first tag of one to three characters that hashes to it."""
    tags: dict[int, str] = {}
    for length in (1, 2, 3):
        for chars in product(_SLOT_TAG_ALPHABET, repeat=length):
            tag = "".join(chars)
            tags.setdefault(key_slot(tag.encode()), tag)
    return tuple(tags[slot] for slot in range(HASH_SLOTS))


def parse_cluster_nodes_line(line: str) -> Optional[dict[str, Any]]:
    if not line.strip():
        return None

    parts = line.split()
    if len(parts) < 8:
        return None

    # Parse address (format: host:port@bus_port or host:port)
    address = parts[1].split("@")[0]  # Remove bus port if present
    host, port_str = address.rsplit(":", 1)

    try:
        port = int(port_str)
    except ValueError:
        logger.warning("Invalid port in CLUSTER NODES line: %s", line)
        return None

    # Parse flags - can be comma-separated like "master,fail"
    flags = parts[2]
    flag_list = flags.split(",")

    return {
        "node_id": parts[0],
        "address": address,
        "host": host,
        "port": port,
        "flags": flags,
        "primary_id": parts[3] if parts[3] != "-" else None,
        "config_epoch": parts[6],
        "link_state": parts[7],
        "slots": parts[8:] if len(parts) > 8 else [],
        "is_myself": "myself" in flag_list,
        "is_primary": "master" in flag_list,
        "is_replica": "slave" in flag_list,
        "is_fail": "fail" in flag_list or "fail?" in flag_list,
    }


def parse_cluster_nodes(cluster_nodes_raw: str) -> list[dict[str, Any]]:
    nodes = []
    for line in cluster_nodes_raw.split("\n"):
        node = parse_cluster_nodes_line(line)
        if node:
            nodes.append(node)
    return nodes


def parse_slot_ranges(tokens: list[str]) -> list[tuple[int, int]]:
    """The hash-slot ranges in a node's CLUSTER NODES slot tokens: ``["0-5460", "5462"]`` is
    ``[(0, 5460), (5462, 5462)]``.

    Importing and migrating markers such as "[5461->-<node-id>]" are skipped.
    """
    ranges = []
    for token in tokens:
        if token.startswith("["):
            continue
        start, _, end = token.partition("-")
        try:
            ranges.append((int(start), int(end or start)))
        except ValueError:
            continue
    return ranges


def parse_slot_tokens(tokens: list[str]) -> list[int]:
    """The hash slots in a node's CLUSTER NODES slot tokens (see :func:`parse_slot_ranges`), one by one."""
    return [slot for start, end in parse_slot_ranges(tokens) for slot in range(start, end + 1)]


def split_slots(views: Mapping[Any, Sequence[Mapping[str, Any]]]) -> dict[int, set[str]]:
    """The slots two primaries serve at once, and those primaries' node IDs: a split brain.

    ``views`` holds each node's CLUSTER NODES as :func:`parse_cluster_nodes`
    returns it, keyed by anything that names the node. Two primaries split a
    slot when one node's view gives it to both and neither is flagged
    ``fail`` there, or when each claims it in its own view. A primary only
    suspected there (``fail?``) still serves its slots, so it counts.
    """
    split: dict[int, set[str]] = {}
    own_claims: dict[int, set[str]] = {}
    for view in views.values():
        owners: dict[int, set[str]] = {}
        for node in view:
            if node["is_primary"] and "fail" not in node["flags"].split(","):
                for slot in parse_slot_tokens(node["slots"]):
                    owners.setdefault(slot, set()).add(node["node_id"])
            if node["is_primary"] and node["is_myself"]:
                for slot in parse_slot_tokens(node["slots"]):
                    own_claims.setdefault(slot, set()).add(node["node_id"])
        for slot, nodes in owners.items():
            if len(nodes) > 1:
                split.setdefault(slot, set()).update(nodes)
    for slot, nodes in own_claims.items():
        if len(nodes) > 1:
            split.setdefault(slot, set()).update(nodes)
    return split


def group_slots_into_ranges(slots: list[int]) -> str:
    if not slots:
        return ""

    sorted_slots = sorted(slots)
    ranges = []
    start = sorted_slots[0]
    end = sorted_slots[0]

    for slot in sorted_slots[1:]:
        if slot == end + 1:
            # Consecutive slot, extend range
            end = slot
        else:
            # Gap found, save current range and start new one
            if start == end:
                ranges.append(str(start))
            else:
                ranges.append(f"{start}-{end}")
            start = slot
            end = slot

    # Add the last range
    if start == end:
        ranges.append(str(start))
    else:
        ranges.append(f"{start}-{end}")

    # Limit output length for very long lists
    if len(ranges) > 10:
        return f"{', '.join(ranges[:10])}... ({len(ranges)} ranges total)"
    else:
        return ", ".join(ranges)
