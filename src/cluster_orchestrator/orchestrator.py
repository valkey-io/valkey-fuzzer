import os
import time
import valkey
import subprocess
import shutil
import uuid
import logging
import traceback
from typing import List, Dict, Optional, Tuple
from ..models import ClusterConfig, NodePlan, NodeInfo, ClusterConnection
from ..utils.valkey_utils import valkey_client

logger = logging.getLogger()

class PortManager:
    """Manages port allocation for Valkey cluster nodes"""
    
    def __init__(self, base_port: int = 7000, max_ports: int = 1000):
        self.base_port = base_port
        self.available_ports = set(range(base_port, base_port + max_ports))
        self.allocated_ports: Dict[str, Tuple[int, int]] = {}
    
    def allocate_ports(self, node_id: str) -> Tuple[int, int]:
        """Allocate client and bus ports for a node"""
        if not self.available_ports:
            raise Exception("No available ports!")
        
        client_port = min(self.available_ports)
        bus_port = client_port + 10000 
        
        self.available_ports.discard(client_port)
        self.allocated_ports[node_id] = (client_port, bus_port)
        
        logger.debug(f"Allocated ports: {client_port}, {bus_port}")
        return client_port, bus_port
    
    def release_ports(self, node_id: str) -> None:
        """Release allocated ports for a node"""
        if node_id in self.allocated_ports:
            client_port, bus_port = self.allocated_ports[node_id]
            self.available_ports.add(client_port)
            del self.allocated_ports[node_id]
            logger.debug(f"Released ports: {client_port}, {bus_port}")


class ConfigurationManager:
    """Manages Valkey cluster planning, node spawning, and resource cleanup"""

    def __init__(self, clusterConfig: ClusterConfig, port_manager: PortManager, seed: Optional[int] = None):
        self.clusterConfig = clusterConfig
        self.port_manager = port_manager
        self.cluster_id = str(uuid.uuid4())[:8]
        self.seed = seed
    
    def setup_valkey_from_source(self, base_dir: str = "/tmp/valkey-build") -> str:
        """Clone and build Valkey binary, return path to valkey-server binary"""
        result = subprocess.run(['which', 'valkey-server'], capture_output=True, text=True)
        if result.returncode == 0:
            valkey_binary = result.stdout.strip()
            return valkey_binary
                
        valkey_dir = os.path.join(base_dir, "valkey")
        valkey_binary = os.path.join(valkey_dir, "src", "valkey-server")
       
        # Check if binary already exists from previous build
        if os.path.exists(valkey_binary):
            return valkey_binary
            
        os.makedirs(base_dir, exist_ok=True)
        
        subprocess.run(['git', 'clone', 'https://github.com/valkey-io/valkey.git', valkey_dir], check=True)
        subprocess.run(['make'], cwd=valkey_dir, check=True)
        
        if not os.path.exists(valkey_binary):
            raise Exception(f"Build completed but binary not found at {valkey_binary}")
        
        return valkey_binary
    
    def create_node_plan(self, node_counter: int, role: str, shard_id: int, 
                         slot_start: Optional[int] = None, slot_end: Optional[int] = None,
                         master_node_id: Optional[str] = None) -> NodePlan:
        """Create a node plan with allocated ports and configuration"""
        node_id = f"node-{node_counter}"
        client_port, bus_port = self.port_manager.allocate_ports(node_id)
        
        return NodePlan(
            node_id=node_id,
            role=role,
            shard_id=shard_id,
            port=client_port,
            bus_port=bus_port,
            slot_start=slot_start,
            slot_end=slot_end,
            master_node_id=master_node_id
        )
    
    def plan_topology(self) -> List[NodePlan]:
        """Plan cluster topology with given number of primary and replica nodes"""
        nodes = []
        node_counter = 0
        
        total_slots = 16384
        slots_per_shard = total_slots // self.clusterConfig.num_shards
        
        logger.info(f"Planning topology for {self.clusterConfig.num_shards} shards with {self.clusterConfig.replicas_per_shard} replica(s) each")
        
        for shard_num in range(self.clusterConfig.num_shards):
            slot_start = shard_num * slots_per_shard
            
            if shard_num == self.clusterConfig.num_shards - 1:
                slot_end = total_slots - 1
            else:
                slot_end = slot_start + slots_per_shard - 1
            
            primary_node_plan = self.create_node_plan(node_counter, 'primary', shard_num, slot_start, slot_end)
            nodes.append(primary_node_plan)
            logger.debug(f"{primary_node_plan.node_id}: primary, shard {shard_num}, "
                        f"port {primary_node_plan.port}, slots {slot_start}-{slot_end}")
            node_counter += 1
            
            for _ in range(self.clusterConfig.replicas_per_shard):
                replica_plan = self.create_node_plan(node_counter, 'replica', shard_num, master_node_id=primary_node_plan.node_id)
                nodes.append(replica_plan)
                logger.debug(f"{replica_plan.node_id}: replica, shard {shard_num}, "
                      f"port {replica_plan.port}, master={primary_node_plan.node_id}")
                node_counter += 1
        
        return nodes
    
    def create_node_directories(self, node_id: str) -> Tuple[str, str]:
        """Create directories for a node and return path"""
        node_data_dir = os.path.join(self.clusterConfig.base_data_dir, f"cluster-{self.cluster_id}", node_id, "data")
        log_dir = os.path.join(self.clusterConfig.base_data_dir, "logs")
        
        os.makedirs(node_data_dir, exist_ok=True)
        os.makedirs(log_dir, exist_ok=True)
        
        log_file = os.path.join(log_dir, f"{self.seed}-{node_id}.log")
        return node_data_dir, log_file
    
    def build_node_command(self, port: int, data_dir: str, log_file: str) -> List[str]:
        """Build the Valkey server command with standard cluster configuration."""
        return [
            self.clusterConfig.valkey_binary,
            '--port', str(port),
            '--bind', '127.0.0.1',
            '--protected-mode', 'no',
            '--cluster-enabled', 'yes',
            '--cluster-config-file', os.path.join(data_dir, 'nodes.conf'),
            '--cluster-node-timeout', '1000',
            '--cluster-require-full-coverage', 'no',
            '--dir', data_dir,
            '--logfile', log_file,
            '--loglevel', 'notice',
            '--appendonly', 'yes',
            '--appendfilename', 'appendonly.aof',
            '--save', '',
            '--maxmemory', '500mb',
            '--maxmemory-policy', 'allkeys-lru'
        ]
    
    def spawn_all_nodes(self, node_plans: List[NodePlan]) -> List[NodeInfo]:
        """Spawn all Valkey processes and wait for them to be ready"""
        logger.info("")
        logger.info(f"Spawning {len(node_plans)} nodes")
        node_info_list = []
        
        for plan in node_plans:
            node_data_dir, log_file = self.create_node_directories(plan.node_id)
            
            # Build command using centralized configuration
            cmd = self.build_node_command(plan.port, node_data_dir, log_file)
            
            logger.debug(f"Spawning {plan.node_id} on port {plan.port}")
            try:
                process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                
                # Note: host defaults to 127.0.0.1 in NodeInfo
                node_info = NodeInfo(
                    node_id=plan.node_id,
                    role=plan.role,
                    shard_id=plan.shard_id,
                    port=plan.port,
                    bus_port=plan.bus_port,
                    pid=process.pid,
                    process=process,
                    data_dir=node_data_dir,
                    log_file=log_file,
                    slot_start=plan.slot_start,
                    slot_end=plan.slot_end,
                    master_node_id=plan.master_node_id
                )
                
                node_info_list.append(node_info)
                logger.debug(f"Spawned {plan.node_id} with PID {process.pid}")
                
            except Exception as e:
                logger.error(f"Failed to spawn {plan.node_id}: {e}")
                for node in node_info_list:
                    self.terminate_node(node)
                raise
        
        logger.info(f"Waiting for all nodes to be ready")
        for node in node_info_list:
            deadline = time.time() + 30
            ready = False
            
            while time.time() < deadline:
                if node.process.poll() is not None:
                    logger.info(f"Node {node.node_id} process died")
                    break
                
                try:
                    with valkey_client('127.0.0.1', node.port, timeout=3.0, decode_responses=True) as client:
                        if client.ping():
                            logger.debug(f"Node {node.node_id} is active")
                            ready = True
                            break
                except (valkey.ConnectionError, valkey.TimeoutError):
                    pass
                
                time.sleep(0.5)
            
            if not ready:
                for failed_node in node_info_list:
                    self.terminate_node(failed_node)
                raise Exception(f"Node {node.node_id} failed to start")
        
        logger.info(f"All {len(node_info_list)} nodes are active")
        return node_info_list
    
    def terminate_node(self, node: NodeInfo) -> None:
        """Terminate a Valkey node process"""
        if node.process.poll() is not None:
            return
        
        logger.debug(f"Terminating {node.node_id} (PID {node.pid})")
        
        node.process.terminate()
        try:
            node.process.wait(timeout=5)
            logger.debug(f"{node.node_id} terminated")
        except subprocess.TimeoutExpired:
            node.process.kill()
            node.process.wait()
            logger.debug(f"{node.node_id} terminated")
    
    def restart_node(self, node: NodeInfo, wait_ready: bool = True, ready_timeout: float = 30.0) -> NodeInfo:
        """Restart a Valkey node process."""
        logger.info(f"Restarting {node.node_id} on port {node.port}")
        
        # Build command using centralized configuration
        cmd = self.build_node_command(node.port, node.data_dir, node.log_file)
        
        try:
            # Start new process
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            
            # Update node info with new process
            node.process = process
            node.pid = process.pid
            
            logger.info(f"Restarted {node.node_id} with new PID {process.pid}")
            
            # Wait for node to be ready if requested
            if wait_ready:
                deadline = time.time() + ready_timeout
                ready = False
                
                while time.time() < deadline:
                    if node.process.poll() is not None:
                        raise Exception(f"Node {node.node_id} process died during restart")
                    
                    try:
                        with valkey_client('127.0.0.1', node.port, timeout=3.0, decode_responses=True) as client:
                            if client.ping():
                                logger.info(f"Node {node.node_id} is ready after restart")
                                ready = True
                                break
                    except (valkey.ConnectionError, valkey.TimeoutError):
                        pass
                    
                    time.sleep(0.5)
                
                if not ready:
                    self.terminate_node(node)
                    raise Exception(f"Node {node.node_id} failed to become ready within {ready_timeout:.2f}s")
            
            return node
            
        except Exception as e:
            logger.error(f"Failed to restart {node.node_id}: {e}")
            raise
    
    def cleanup_cluster(self, nodes_in_cluster: List[NodeInfo]) -> None:
        """Clean up cluster by terminating nodes and releasing resources"""
        logger.info(f"Cleaning up cluster {self.cluster_id}")
        
        for node in nodes_in_cluster:
            self.terminate_node(node)
            self.port_manager.release_ports(node.node_id)
        
        if self.clusterConfig.enable_cleanup:
            cluster_dir = os.path.join(
                self.clusterConfig.base_data_dir,
                f"cluster-{self.cluster_id}"
            )
            if os.path.exists(cluster_dir):
                shutil.rmtree(cluster_dir)
                logger.info(f"Deleted data directory")
        
        logger.debug(f"Cluster {self.cluster_id} cleaned up")


class ClusterManager:
    """Manages Valkey cluster formation and validates cluster health. Works with nodes that have been spawned by ConfigurationManager"""
    
    def __init__(self):
        self.connections: Dict[str, valkey.Valkey] = {}
    
    def get_client(self, node: NodeInfo) -> valkey.Valkey:
        """Get or create Valkey client connection for a node"""
        if node.node_id not in self.connections:
            self.connections[node.node_id] = valkey.Valkey(
                host='127.0.0.1',
                port=node.port,
                socket_timeout=5,
                decode_responses=True
            )
        return self.connections[node.node_id]
    
    def get_cluster_info(self, node: NodeInfo) -> Dict[str, str]:
        """Get cluster info from a node and parse into dictionary"""
        client = self.get_client(node)
        info = client.execute_command('CLUSTER', 'INFO')
        
        info_dict = {}
        for line in info.split('\r\n'):
            if ':' in line:
                key, value = line.split(':', 1)
                info_dict[key] = value
        return info_dict
    
    def cluster_meet(self, nodes_in_cluster: List[NodeInfo], timeout: int = 30) -> None:
        """Connect cluster nodes and wait for convergence"""
        if len(nodes_in_cluster) < 2:
            return
        
        first_node = nodes_in_cluster[0]
        first_node_client = self.get_client(first_node)

        logger.info("")
        logger.info(f"Connecting {len(nodes_in_cluster)} nodes with CLUSTER MEET using {first_node.node_id} as starting node")
                
        for node in nodes_in_cluster[1:]:
            logger.debug(f"Meeting {node.node_id} (port {node.port})")
            first_node_client.execute_command('CLUSTER', 'MEET', '127.0.0.1', node.port)
            time.sleep(0.1)
        
        logger.info("CLUSTER MEET complete")
        
        # Wait for convergence
        expected_count = len(nodes_in_cluster)
        deadline = time.time() + timeout
        
        logger.info(f"Waiting for cluster convergence for all {expected_count} nodes (timeout: {timeout:.2f}s)")
        
        while time.time() < deadline:
            all_converged = True
            
            for node in nodes_in_cluster:
                try:
                    info_dict = self.get_cluster_info(node)
                    known_nodes = int(info_dict.get('cluster_known_nodes', 0))
                    
                    if known_nodes < expected_count:
                        all_converged = False
                    
                except Exception as e:
                    logger.error(f"Error checking {node.node_id}: {e}")
                    all_converged = False
            
            if all_converged:
                logger.info(f"All nodes see each other and are apart of the cluster")
                return
            
            time.sleep(1)
        
        raise Exception(f"Cluster failed to converge within {timeout:.2f}s")
    
    def reset_cluster_state(self, nodes_in_cluster: List[NodeInfo]) -> None:
        """Reset cluster state on all nodes"""        
        logger.debug("Resetting cluster state")
        for node in nodes_in_cluster:
            client = self.get_client(node)
            client.execute_command('CLUSTER', 'RESET', 'HARD')
        time.sleep(1)
    
    def assign_and_verify_slots(self, nodes_in_cluster: List[NodeInfo]) -> Dict[str, str]:
        """Assign hash slots to primary nodes and verify assignment"""
        primary_nodes = [node for node in nodes_in_cluster if node.role == 'primary']

        if not primary_nodes:
            logger.info("No primary nodes to assign slots to")
            return {}

        
        logger.info("")
        logger.info(f"Assigning slots to {len(primary_nodes)} primaries")
        
        node_ids = {}
        
        for primary in primary_nodes:
            client = self.get_client(primary)
            
            slots = list(range(primary.slot_start, primary.slot_end + 1))
            logger.debug(f"Assigning slots {primary.slot_start}-{primary.slot_end} to {primary.node_id}")
            
            client.execute_command('CLUSTER', 'ADDSLOTS', *slots)
            
            cluster_node_id = client.execute_command('CLUSTER', 'MYID')
            node_ids[primary.node_id] = cluster_node_id
            primary.cluster_node_id = cluster_node_id
            
            logger.debug(f"{primary.node_id} cluster ID: {cluster_node_id[:8]}")
        
        logger.info("Slot assignment complete")
        
        # Verify slots were assigned correctly with retry logic
        max_retries = 10
        retry_delay = 2.0
        
        for attempt in range(max_retries):
            time.sleep(retry_delay)
            
            info_dict = self.get_cluster_info(nodes_in_cluster[0])
            slots_assigned = int(info_dict.get('cluster_slots_assigned', 0))
            slots_fail = int(info_dict.get('cluster_slots_fail', 0))
            
            if attempt == 0 or attempt == max_retries - 1:
                logger.debug(f"Slot verification (attempt {attempt + 1}/{max_retries}):")
                logger.debug(f"Slots assigned: {slots_assigned}/16384")
                logger.debug(f"Slots failed: {slots_fail}")
            
            if slots_assigned == 16384 and slots_fail == 0:
                logger.info(f"Slot assignment verified successfully after {attempt + 1} attempts")
                return node_ids
            
            if attempt < max_retries - 1:
                logger.debug(f"Waiting for slot propagation... ({slots_assigned}/16384)")
        
        # Final check failed
        raise Exception(f"Slot assignment failed after {max_retries} attempts: {slots_assigned}/16384 assigned, {slots_fail} failed")
    
    def setup_and_sync_replication(self, nodes_in_cluster: List[NodeInfo], primary_ids: Dict[str, str], timeout: int = 60) -> None:
        """Configure replication and wait for replicas to sync"""
        replicas = [node for node in nodes_in_cluster if node.role == 'replica']
        
        if not replicas:
            logger.info("No replicas to configure")
            return
        
        logger.info("")
        logger.info(f"Configuring {len(replicas)} replicas")
        
        for replica in replicas:
            master_cluster_id = primary_ids.get(replica.master_node_id)
            
            if not master_cluster_id:
                logger.info(f"Could not find master for {replica.node_id}")
                continue
            
            logger.debug(f"Configuring {replica.node_id} to replicate {replica.master_node_id}")
            
            client = self.get_client(replica)
            client.execute_command('CLUSTER', 'REPLICATE', master_cluster_id)
            replica.cluster_node_id = client.execute_command('CLUSTER', 'MYID')
            
            logger.debug(f"Replication configured")
                
        logger.info(f"Waiting for {len(replicas)} replicas to sync (timeout: {timeout:.2f}s)")
        deadline = time.time() + timeout
        
        while time.time() < deadline:
            all_synced = True
            
            for replica in replicas:
                try:
                    client = self.get_client(replica)
                    info = client.info('replication')
                    
                    master_link = info.get('master_link_status')
                    
                    if master_link != 'up':
                        all_synced = False
                
                except Exception as e:
                    logger.info(f"Error checking {replica.node_id}: {e}")
                    all_synced = False
            
            if all_synced:
                logger.info("All replicas are in sync with their master")
                return
            
            time.sleep(1)
        
        raise Exception(f"Replicas failed to sync within {timeout:.2f}s")
    
    def check_replication_links(self, nodes_in_cluster: List[NodeInfo]) -> bool:
        """Check if all replica master_link_status is 'up'"""
        for node in nodes_in_cluster:
            try:
                client = self.get_client(node)
                info = client.info('replication')
                
                if info.get('role') == 'slave':
                    if info.get('master_link_status') != 'up':
                        return False
            except:
                return False
        
        return True
    
    def validate_cluster(self, nodes_in_cluster: List[NodeInfo], timeout: float = 30.0, interval: float = 1.0) -> bool:
        if not nodes_in_cluster:
            return False
        
        logger.info("")
        logger.info("CLUSTER VALIDATION")
        deadline = time.time() + timeout
        last_states: List[Dict[str, object]] = []
        last_unreachable: List[str] = []

        while time.time() < deadline:
            node_states = []
            unreachable_nodes = []

            for node in nodes_in_cluster:
                try:
                    info_dict = self.get_cluster_info(node)

                    cluster_state = info_dict.get('cluster_state')
                    slots_assigned = int(info_dict.get('cluster_slots_assigned', 0))
                    slots_fail = int(info_dict.get('cluster_slots_fail', 0))

                    node_state = {
                        'node_id': node.node_id,
                        'state': cluster_state,
                        'slots_assigned': slots_assigned,
                        'slots_fail': slots_fail
                    }
                    node_states.append(node_state)

                    logger.debug(f"{node.node_id}: state={cluster_state}, slots={slots_assigned}/16384, fail={slots_fail}")

                except Exception as e:
                    unreachable_nodes.append(node.node_id)
                    logger.debug(f"Expected node {node.node_id} is unreachable: {e}")

            if unreachable_nodes:
                last_unreachable = unreachable_nodes
                last_states = node_states
                time.sleep(interval)
                continue

            if not node_states:
                time.sleep(interval)
                continue

            first_state = node_states[0]
            consensus = all(
                state['state'] == first_state['state'] and
                state['slots_assigned'] == first_state['slots_assigned'] and
                state['slots_fail'] == first_state['slots_fail']
                for state in node_states
            )

            if consensus:
                is_healthy = (
                    first_state['state'] == 'ok' and
                    first_state['slots_assigned'] == 16384 and
                    first_state['slots_fail'] == 0
                )

                # Check replica sync status
                replicas_synced = self.check_replication_links(nodes_in_cluster)

                if is_healthy and replicas_synced:
                    logger.info(f"Cluster is Healthy (all {len(node_states)} nodes reachable and consistent)")
                    return True

                # Consensus but cluster not yet healthy – wait a bit more before failing
                last_states = node_states
            else:
                last_states = node_states

            time.sleep(interval)

        # If we reach here, validation failed after timeout
        if last_unreachable:
            logger.warning(f"Validation failed: {len(last_unreachable)} expected node(s) unreachable: {', '.join(last_unreachable)}")        
            return False
        
        if last_states:
            first_state = last_states[0]
            consensus = all(
                state['state'] == first_state['state'] and
                state['slots_assigned'] == first_state['slots_assigned'] and
                state['slots_fail'] == first_state['slots_fail']
                for state in last_states
            )

            if not consensus:
                logger.warning("Validation failed: Cluster nodes have inconsistent views:")
                for state in last_states:
                    logger.warning(f"  {state['node_id']}: {state['state']}, {state['slots_assigned']}/16384, fail={state['slots_fail']}")
                return False

            logger.info(
                f"Cluster is Not Healthy: state={first_state['state']}, "
                f"slots={first_state['slots_assigned']}/16384"
            )

        else:
            logger.warning("Could not reach any nodes for validation")

        return False
    
    def form_cluster(self, nodes_in_cluster: List[NodeInfo], cluster_id: str) -> ClusterConnection:
        """Form a complete cluster from spawned nodes"""
        try:
            self.reset_cluster_state(nodes_in_cluster)
            self.cluster_meet(nodes_in_cluster)
            
            primary_ids = self.assign_and_verify_slots(nodes_in_cluster)
                       
            self.setup_and_sync_replication(nodes_in_cluster, primary_ids)
            
            if not self.validate_cluster(nodes_in_cluster):
                raise Exception("Cluster validation failed")
            
            logger.info("Cluster initial state: Healthy")
            
            return ClusterConnection(nodes_in_cluster, cluster_id)
        
        except Exception as e:
            logger.info(f"Cluster formation failed: {e}")
            traceback.print_exc()
            return None
    
    def close_connections(self) -> None:
        """Close all Valkey client connections"""
        for client in self.connections.values():
            try:
                client.close()
            except:
                pass
        self.connections.clear()
