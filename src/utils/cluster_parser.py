"""
Centralized CLUSTER NODES parsing utilities.

This module provides parsing functions for Redis/Valkey CLUSTER NODES output.
It has NO dependencies on models.py to avoid circular imports.
"""

import logging
from typing import Any, Optional

import valkey

logger = logging.getLogger(__name__)


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
        logger.warning(f"Invalid port in CLUSTER NODES line: {line}")
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
        "master_id": parts[3] if parts[3] != "-" else None,
        "ping_sent": parts[4],
        "pong_recv": parts[5],
        "config_epoch": parts[6],
        "link_state": parts[7],
        "slots": parts[8:] if len(parts) > 8 else [],
        "is_myself": "myself" in flag_list,
        "is_master": "master" in flag_list,
        "is_slave": "slave" in flag_list,
        "is_fail": "fail" in flag_list or "fail?" in flag_list,
    }


def parse_cluster_nodes_raw(cluster_nodes_raw: str) -> list[dict[str, Any]]:
    nodes = []
    for line in cluster_nodes_raw.split("\n"):
        node = parse_cluster_nodes_line(line)
        if node:
            nodes.append(node)
    return nodes


def format_node_address(node: dict) -> str:
    return f"{node['host']}:{node['port']}"


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


def compute_cluster_slot(key: str) -> int:
    # Extract hash tag if present
    start = key.find("{")
    if start != -1:
        end = key.find("}", start + 1)
        if end != -1 and end > start + 1:
            key = key[start + 1 : end]

    # CRC16 XMODEM
    crc = 0xFFFF
    for byte in key.encode("utf-8"):
        crc ^= byte << 8
        for _ in range(8):
            crc = crc << 1 ^ 4129 if crc & 32768 else crc << 1
            crc &= 0xFFFF

    return crc % 16384


def fetch_cluster_slot_assignments(live_nodes: list[dict], timeout: float = 2.0) -> dict[int, str]:
    slot_map = {}

    for node in live_nodes:
        try:
            with valkey.Valkey(
                host=node["host"], port=node["port"], socket_timeout=timeout, decode_responses=True
            ) as client:
                # CLUSTER SLOTS returns: [[start, end, [host, port, node_id], ...], ...]
                slots_info = client.execute_command("CLUSTER", "SLOTS")

                for slot_range in slots_info:
                    if len(slot_range) < 3:
                        continue

                    start_slot = slot_range[0]
                    end_slot = slot_range[1]

                    # First node in the list is the primary
                    primary_info = slot_range[2]
                    if len(primary_info) >= 2:
                        primary_host = primary_info[0]
                        primary_port = primary_info[1]
                        primary_addr = f"{primary_host}:{primary_port}"

                        # Map all slots in this range to the primary
                        for slot in range(start_slot, end_slot + 1):
                            slot_map[slot] = primary_addr

                logger.debug(f"Fetched {len(slot_map)} slot assignments from {format_node_address(node)}")
                return slot_map

        except Exception as e:
            logger.debug(f"Failed to fetch slots from {format_node_address(node)}: {e}")
            continue

    logger.warning("Failed to fetch slot assignments from any node")
    return {}
