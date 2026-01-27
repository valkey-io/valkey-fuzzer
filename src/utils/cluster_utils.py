import logging
import os
import glob
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)


def find_node_by_identifier(
    nodes: List[Dict],
    identifier: str,
    match_strategies: Optional[List[str]] = None
) -> Optional[Dict]:
    if not nodes:
        return None
    
    if match_strategies is None:
        match_strategies = ['node_id', 'port', 'shard_pattern']
    
    # Strategy 1: Try exact node_id match
    if 'node_id' in match_strategies:
        for node in nodes:
            if node.get('node_id') == identifier:
                logger.debug(f"Matched node by node_id: {identifier}")
                return node
    
    # Strategy 2: Try exact port match
    if 'port' in match_strategies:
        try:
            target_port = int(identifier)
            for node in nodes:
                if node.get('port') == target_port:
                    logger.debug(f"Matched node by port: {target_port}")
                    return node
        except ValueError:
            pass  # identifier is not a port number
    
    # Strategy 3: Try shard-based matching (e.g., "shard-0-primary", "shard-1-replica")
    if 'shard_pattern' in match_strategies:
        if 'shard-' in identifier:
            try:
                # Parse shard pattern: "shard-X-role"
                parts = identifier.split('-')
                if len(parts) >= 3 and parts[0] == 'shard':
                    shard_num = int(parts[1])
                    role_part = parts[2]  # "primary" or "replica"
                    
                    # Map role names
                    role_map = {
                        'primary': 'primary',
                        'master': 'primary',
                        'replica': 'replica',
                        'slave': 'replica'
                    }
                    
                    target_role = role_map.get(role_part)
                    
                    if target_role:
                        # Find node with matching shard_id and role
                        for node in nodes:
                            if node.get('role') == target_role and node.get('shard_id') == shard_num:
                                logger.debug(
                                    f"Matched node by shard pattern: shard {shard_num} "
                                    f"{target_role} at port {node.get('port')}"
                                )
                                return node
            except (ValueError, IndexError) as e:
                logger.debug(f"Failed to parse shard pattern '{identifier}': {e}")
    
    logger.debug(f"No node found matching identifier: {identifier}")
    return None


def find_primary_node_by_identifier(nodes: List[Dict], identifier: str, match_strategies: Optional[List[str]] = None) -> Optional[Dict]:
    node = find_node_by_identifier(nodes, identifier, match_strategies)
    
    if node and node.get('role') == 'primary':
        return node
    
    if node:
        logger.warning(
            f"Found node matching '{identifier}' but it's not a primary "
            f"(role: {node.get('role')})"
        )
    
    return None


def find_replica_nodes_by_primary(
    nodes: List[Dict],
    primary_node_id: str,
    primary_shard_id: Optional[int] = None
) -> List[Dict]:
    replicas = []
    
    # Strategy 1: Match by shard_id if available
    if primary_shard_id is not None:
        for node in nodes:
            if node.get('role') == 'replica' and node.get('shard_id') == primary_shard_id:
                replicas.append(node)
                logger.debug(
                    f"Found replica by shard_id: {node.get('node_id')} at port {node.get('port')}"
                )
    
    # Strategy 2: Match by master_id (if nodes have this field)
    if not replicas:
        for node in nodes:
            if node.get('role') == 'replica' and node.get('master_id') == primary_node_id:
                replicas.append(node)
                logger.debug(
                    f"Found replica by master_id: {node.get('node_id')} at port {node.get('port')}"
                )
    
    return replicas


def group_nodes_by_shard(nodes: List[Dict]) -> Dict[int, Dict[str, List[Dict]]]:
    shards = {}
    
    for node in nodes:
        shard_id = node.get('shard_id')
        if shard_id is None:
            continue
        
        if shard_id not in shards:
            shards[shard_id] = {'primary': None, 'replicas': []}
        
        if node.get('role') == 'primary':
            shards[shard_id]['primary'] = node
        elif node.get('role') == 'replica':
            shards[shard_id]['replicas'].append(node)
    
    return shards

def cleanup_logs(seed: int, base_data_dir: str = "/tmp/valkey-fuzzer", force: bool = False) -> int:
    """Clean up log files for a specific seed with optional confirmation"""
    log_dir = os.path.join(base_data_dir, "logs")
    
    if not os.path.exists(log_dir):
        logger.info(f"No log files found for seed {seed}")
        return 0
    
    pattern = os.path.join(log_dir, f"{seed}-*.log")
    log_files = glob.glob(pattern)
    
    if not log_files:
        logger.info(f"No log files found for seed {seed}")
        return 0
    
    for log_file in log_files:
        logger.info(f"  {os.path.basename(log_file)}")
    
    if not force:
        response = input(f"\nDelete these {len(log_files)} log file(s)? [y/N]: ")
        if response.lower() != 'y':
            logger.info("Cancelled")
            return 0
    
    deleted = 0
    for log_file in log_files:
        try:
            os.remove(log_file)
            deleted += 1
        except Exception:
            pass
    
    logger.info(f"Deleted {deleted} log file(s) for seed {seed}")
    return 0
