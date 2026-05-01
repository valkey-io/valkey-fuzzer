"""
Valkey client utilities for safe connection management and common query patterns
"""

import logging
from contextlib import contextmanager
from typing import Callable, Optional, TypeVar

import valkey

logger = logging.getLogger(__name__)

T = TypeVar("T")


@contextmanager
def valkey_client(host: str, port: int, timeout: float, decode_responses: bool = True):
    client = None
    try:
        client = valkey.Valkey(
            host=host,
            port=port,
            socket_timeout=timeout,
            socket_connect_timeout=timeout,
            decode_responses=decode_responses,
        )
        yield client
    finally:
        if client is not None:
            try:
                client.close()
            except Exception:
                pass  # Ignore errors during cleanup


def safe_query_node(
    node: dict, timeout: float, query_func: Callable[[valkey.Valkey], T], error_message: str = "Error querying node"
) -> Optional[T]:
    try:
        with valkey_client(node["host"], node["port"], timeout) as client:
            return query_func(client)
    except Exception as e:
        logger.debug(f"{error_message} {node['host']}:{node['port']}: {e}")
        return None


def query_cluster_nodes(node: dict, timeout: float = 2.0) -> Optional[list[dict]]:
    from .cluster_parser import parse_cluster_nodes_raw

    def query_func(client: valkey.Valkey) -> list[dict]:
        cluster_nodes_raw = client.execute_command("CLUSTER", "NODES")
        # Handle both bytes and string responses
        if isinstance(cluster_nodes_raw, bytes):
            cluster_nodes_raw = cluster_nodes_raw.decode()
        return parse_cluster_nodes_raw(cluster_nodes_raw)

    return safe_query_node(node, timeout, query_func, error_message="Error querying CLUSTER NODES from")


def is_node_alive(host: str, port: int, timeout: float = 2.0) -> bool:
    try:
        with valkey_client(host, port, timeout, decode_responses=True) as client:
            client.ping()
            return True
    except Exception:
        return False
