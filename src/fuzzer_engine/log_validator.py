"""
Validates Valkey node logs for affected shards
"""
import os
import re
import logging
from typing import List, Set
from dataclasses import dataclass
from ..models import NodeInfo, Operation

logger = logging.getLogger(__name__)

@dataclass
class LogFinding:
    node_id: str
    shard_id: int
    severity: str
    message: str
    log_line: str

@dataclass
class LogValidationResult:
    success: bool
    findings: List[LogFinding]
    shards_checked: int
    
    def __str__(self):
        if self.success:
            return f"Log validation passed ({self.shards_checked} shards checked)"
        else:
            error_count = len([f for f in self.findings if f.severity == 'error'])
            warning_count = len([f for f in self.findings if f.severity == 'warning'])
            return f"Log validation failed: {error_count} errors, {warning_count} warnings"


class ShardLogValidator:
    """Validates logs for shards affected by operations and chaos"""
    
    def __init__(self):
        self.failover_promotion_patterns = [
            r"Failover election won",
            r"configEpoch set to \d+ after successful failover",
            r"Failover auth granted",
        ]
                
        self.failover_error_patterns = [
            r"Failover attempt expired",
            r"Manual failover timed out",
            r"Failover election in progress for epoch 0"
        ]
        
        self.replication_issue_patterns = [
            r"I'm a sub-replica! Reconfiguring myself"
        ]
    
    def validate_affected_shards(self, cluster_nodes: List[NodeInfo], operations: List[Operation], nodes_killed_by_chaos: Set[str], shards_with_primary_killed: Set[int]) -> LogValidationResult:
        """Validate logs for shards affected by operations and chaos"""
        
        logger.info("Starting log validation for affected shards")
        
        findings = []
        affected_shards = self._get_affected_shards(operations, nodes_killed_by_chaos, cluster_nodes)
        
        logger.info(f"Validating logs for {len(affected_shards)} affected shards: {affected_shards}")
        
        findings.extend(self._validate_failure_detection(cluster_nodes, nodes_killed_by_chaos))
        
        for shard_id in affected_shards:
            nodes_in_shard = [n for n in cluster_nodes if n.shard_id == shard_id]
            
            # Check if shard had explicit failover or chaos-triggered failover
            had_explicit_failover = self._shard_had_failover(shard_id, operations, cluster_nodes)
            had_primary_killed = shard_id in shards_with_primary_killed
            
            if had_explicit_failover or had_primary_killed:
                findings.extend(self._validate_failover(nodes_in_shard))
        
        for finding in findings:
            if finding.severity == 'error':
                logger.error(f"Log validation error in shard {finding.shard_id}: {finding.message}")
            elif finding.severity == 'warning':
                logger.warning(f"Log validation warning in shard {finding.shard_id}: {finding.message}")
        
        success = len([f for f in findings if f.severity == 'error']) == 0
        
        return LogValidationResult(success=success, findings=findings, shards_checked=len(affected_shards))
    
    def _get_affected_shards(self, operations: List[Operation], nodes_killed_by_chaos: Set[str], cluster_nodes: List[NodeInfo]) -> Set[int]:
        """Get all the shard IDs affected by operations or chaos"""
        affected_shards = set()
        
        node_id_to_shard = {node.node_id: node.shard_id for node in cluster_nodes}
        port_to_shard = {str(node.port): node.shard_id for node in cluster_nodes}
        cluster_id_to_shard = {node.cluster_node_id: node.shard_id for node in cluster_nodes if node.cluster_node_id}
        
        for op in operations:
            # Parse shard-pattern targets like "shard-4-primary"
            if 'shard-' in op.target_node:
                try:
                    parts = op.target_node.split('-')
                    if len(parts) >= 3 and parts[0] == 'shard':
                        shard_id = int(parts[1])
                        affected_shards.add(shard_id)
                        continue
                except (ValueError, IndexError):
                    pass          
            else:
                # In the Operation Orchestrator we also store the targets with their port and cluster_ID
                shard_id = (node_id_to_shard.get(op.target_node) or port_to_shard.get(op.target_node) or cluster_id_to_shard.get(op.target_node))
                if shard_id is not None:
                    affected_shards.add(shard_id)
        
        # Find shards with nodes killed by chaos
        for node in cluster_nodes:
            node_addr = f"{node.host}:{node.port}"
            if node_addr in nodes_killed_by_chaos:
                affected_shards.add(node.shard_id)
        
        return affected_shards
    
    def _shard_had_failover(self, shard_id: int, operations: List[Operation], cluster_nodes: List[NodeInfo]) -> bool:
        """Check if shard had a failover operation"""
        node_id_to_shard = {node.node_id: node.shard_id for node in cluster_nodes}
        port_to_shard = {str(node.port): node.shard_id for node in cluster_nodes}
        cluster_id_to_shard = {node.cluster_node_id: node.shard_id for node in cluster_nodes if node.cluster_node_id}
        
        for op in operations:
            if op.type.value != 'failover':
                continue
            
            if 'shard-' in op.target_node:
                try:
                    parts = op.target_node.split('-')
                    if len(parts) >= 3 and parts[0] == 'shard':
                        op_shard_id = int(parts[1])
                        if op_shard_id == shard_id:
                            return True
                        continue
                except (ValueError, IndexError):
                    pass
            else:
                op_shard_id = (node_id_to_shard.get(op.target_node) or port_to_shard.get(op.target_node) or cluster_id_to_shard.get(op.target_node))
                if op_shard_id == shard_id:
                    return True
                
        return False
    
    def _validate_failure_detection(self, cluster_nodes: List[NodeInfo], nodes_killed_by_chaos: Set[str]) -> List[LogFinding]:
        """Check for possible failure logs in nodes"""
        # Node X () possibly failing is the initial suspicion (pfail)
        # FAIL message received from Node Y about Node X and gossip will spread
        # Node X will then we marked as failing once quorum is reached
        findings = []
        
        if not nodes_killed_by_chaos:
            return findings
        
        killed_node_ids = {}
        for node in cluster_nodes:
            node_addr = f"{node.host}:{node.port}"
            if node_addr in nodes_killed_by_chaos and node.cluster_node_id:
                killed_node_ids[node_addr] = node.cluster_node_id
        
        # Check other shards' logs for failure detection messages
        for killed_addr, killed_cluster_id in killed_node_ids.items():
            detected = False
            
            for node in cluster_nodes:
                node_addr = f"{node.host}:{node.port}"
                if node_addr in nodes_killed_by_chaos:
                    continue
                
                if not os.path.exists(node.log_file):
                    continue
                
                log_lines = self._read_log_tail(node.log_file, lines=200)
                pattern = f"Marking node {killed_cluster_id}.*as failing.*quorum reached"
                if self._has_pattern(log_lines, pattern):
                    detected = True
                    break
            
            if not detected:
                findings.append(LogFinding(
                    node_id=killed_addr,
                    shard_id=-1,
                    severity='error',
                    message=f'Cluster did not detect failure of killed node {killed_addr} (no "quorum reached" message found)',
                    log_line=''
                ))
        
        return findings
    
    def _validate_failover(self, affected_nodes: List[NodeInfo]) -> List[LogFinding]:
        """Check that the failover completed successfully by parsing the logs"""
        findings = []
        
        valkey_shard_to_our_shard = {}
        for node in affected_nodes:
            log_lines = self._read_log_tail(node.log_file, lines=500) if os.path.exists(node.log_file) else []
            for line in log_lines:
                # Extract Valkey shard ID from formation messages
                match = re.search(r'is now part of shard ([a-f0-9]{40})', line)
                if match:
                    valkey_shard_id = match.group(1)
                    valkey_shard_to_our_shard[valkey_shard_id] = node.shard_id
                    break
        
        for node in affected_nodes:
            if not os.path.exists(node.log_file):
                logger.warning(f"Log file not found for node {node.node_id}: {node.log_file}")
                continue
            # Validate that failover was successful
            try:
                log_lines = self._read_log_tail(node.log_file, lines=200)
                
                if node.role == 'primary':
                    has_promotion = any(self._has_pattern(log_lines, pattern) for pattern in self.failover_promotion_patterns)
                    
                    if not has_promotion:
                        findings.append(LogFinding(
                            node_id=node.node_id,
                            shard_id=node.shard_id,
                            severity='warning',
                            message='Primary has no failover promotion messages in recent logs',
                            log_line=''
                        ))
                    else:
                        # Check that promoted node is in correct shard
                        for line in log_lines:
                            match = re.search(r'Setting myself to primary in shard ([a-f0-9]{40})', line)
                            if match:
                                promoted_valkey_shard = match.group(1)
                                expected_shard_id = valkey_shard_to_our_shard.get(promoted_valkey_shard)
                                
                                if expected_shard_id is not None and expected_shard_id != node.shard_id:
                                    findings.append(LogFinding(
                                        node_id=node.node_id,
                                        shard_id=node.shard_id,
                                        severity='error',
                                        message=f'Node promoted to wrong shard: expected {node.shard_id}, got {expected_shard_id}',
                                        log_line=line.strip()
                                    ))
                                break
                
                # Check for failover errors only if failover didn't succeed
                has_promotion_any = any(self._has_pattern(log_lines, pattern) for pattern in self.failover_promotion_patterns)
                if not has_promotion_any:
                    for pattern in self.failover_error_patterns:
                        matches = self._find_all_patterns(log_lines, pattern)
                        if matches:
                            findings.append(LogFinding(
                                node_id=node.node_id,
                                shard_id=node.shard_id,
                                severity='error',
                                message=f'Failover error detected',
                                log_line=matches[0]
                            ))
                
                # Check for replication issues
                for pattern in self.replication_issue_patterns:
                    matches = self._find_all_patterns(log_lines, pattern)
                    if matches:
                        findings.append(LogFinding(
                            node_id=node.node_id,
                            shard_id=node.shard_id,
                            severity='warning',
                            message=f'Replication topology issue detected',
                            log_line=matches[0]
                        ))
                            
            except Exception as e:
                logger.error(f"Error reading log for node {node.node_id}: {e}")
        
        return findings
    
    def _read_log_tail(self, log_file: str, lines: int = 100) -> List[str]:
        """Read last N lines from log file"""
        try:
            with open(log_file, 'r') as f:
                all_lines = f.readlines()
                return all_lines[-lines:] if len(all_lines) > lines else all_lines
        except Exception as e:
            logger.error(f"Error reading log file {log_file}: {e}")
            return []
    
    def _has_pattern(self, log_lines: List[str], pattern: str) -> bool:
        """Check if pattern exists in log lines"""
        regex = re.compile(pattern, re.IGNORECASE)
        return any(regex.search(line) for line in log_lines)
    
    def _find_all_patterns(self, log_lines: List[str], pattern: str) -> List[str]:
        """Find all lines matching pattern"""
        regex = re.compile(pattern, re.IGNORECASE)
        matches = []
        for line in log_lines:
            if regex.search(line):
                matches.append(line.strip())
        return matches
