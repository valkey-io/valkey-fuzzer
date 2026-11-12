"""``valkey.observer.cluster-trace``: the run's events together with every node's epochs, in memory and in
nodes.conf, as JSONL that ``valkey-fuzzer spec conform`` reads."""

import os
import threading
import time
from typing import Any, Optional

import valkey

from valkey_fuzzer.declarations import Declarations
from valkey_fuzzer.plan import ComponentSpec, ExecutionPlan
from valkey_fuzzer.protocols import component_artifact_dir
from valkey_fuzzer.results import ComponentStatus, make_evidence, make_result
from valkey_fuzzer.schema import ConfigError, ConfigField, plain_config
from valkey_fuzzer.spec.conform import TRACE_FILE

from .cluster import cluster_view, read_view
from .files import nodes_conf_path, read_cluster_vars, write_jsonl
from .instances import close_quietly, deployment, select_instances
from .polling import StoppableThread
from .registration import SPEC_SHA256_FIELD, component_registration, pinned_spec_mapping
from .schemas import INTEGER, TEXT, declared, mapping_of

TRACE_EVIDENCE = "valkey.cluster.trace.recorded"


_TRACE_FIELDS = {
    "nodes": ConfigField((str, list), default="all"),
    "interval_s": ConfigField(float, default=0.25, minimum=0, maximum=3600),
    "sample_on_events": ConfigField(bool, default=True),
    # Event-driven rounds start at least this long after the event, so a
    # burst of election events costs one round and the sampler does not
    # compete with a fault that fires inside the event's dispatch.
    "event_delay_s": ConfigField(float, default=0.05, minimum=0, maximum=60),
    "max_rounds": ConfigField(int, default=20_000, minimum=1, maximum=1_000_000),
    "sample_timeout_s": ConfigField(float, default=0.5, minimum=0.05, maximum=30),
    # Server configs recorded per node in the header (CONFIG GET at start).
    "config_keys": ConfigField(
        list, default=lambda: ["cluster-config-save-behavior", "cluster-node-timeout"], items=TEXT
    ),
    # The spec bundle whose mapping reads this trace (`valkey-fuzzer spec conform`);
    # the header records the mapping's digest so a later change to it shows.
    "spec": ConfigField(str, allow_none=True, default=None),
    "spec_sha256": SPEC_SHA256_FIELD,
}


def _validate_trace(config: dict, spec: ComponentSpec, plan: ExecutionPlan) -> None:
    if not all(config["config_keys"]):
        raise ConfigError("config_keys must hold config names; remove the empty one")
    if config["spec"]:
        pinned_spec_mapping(config)


class ClusterTraceObserver:
    """Record every lifecycle event and per-node epoch observations as JSONL.

    Records are ``header`` (node map), ``event`` (bus events; ``t`` is the
    log timestamp for log-derived events, else the receive time), and
    ``observation`` (one node's currentEpoch, configEpoch, role, slots, the
    primaries it knows, and the ``vars`` line of its nodes.conf).  Sampling
    rounds run periodically and after events, on a background thread, so the
    observer never delays a fault that fires inside an event's dispatch.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._records: list[dict[str, Any]] = []
        self._wake = threading.Event()
        self._thread: Optional[StoppableThread] = None
        self._subscription = None
        self._clients: dict[str, Any] = {}
        self._rounds = 0
        self._header: dict[str, Any] = {}

    def start(self, context: Any, spec: ComponentSpec):
        self._config = spec.config
        self._handle = deployment(context)
        self._nodes = select_instances(self._handle, self._config["nodes"])
        nodes = {}
        for node in self._handle.instances:
            view = read_view(self._handle, node.instance_id)
            nodes[node.instance_id] = {
                "node_id": view["node_id"] if view else None,
                "shard": node.shard_id,
                "port": node.port,
                "planned_role": node.role,
            }
            nodes[node.instance_id]["config"], error = self._server_config(node.instance_id)
            if error is not None:
                # spec conform then names the config the trace lacks, and this says why.
                nodes[node.instance_id]["config_error"] = error
        plan = context.plan
        self._header = {
            "kind": "header",
            "t": time.time(),
            "scenario_id": plan.scenario_id,
            "seed": plan.seed,
            "experiment_id": plan.experiment_id,
            "inputs": plain_config(plan.inputs),
            "nodes": nodes,
        }
        if self._config["spec"]:
            mapping = pinned_spec_mapping(self._config)
            self._header["spec"] = {"bundle": self._config["spec"], "mapping_sha256": mapping.sha256}
        self._subscription = context.event_bus.subscribe("*", self._on_event)
        self._sample_round("start")

        def loop(stop_event) -> None:
            interval = self._config["interval_s"] or None
            while not stop_event.is_set():
                woke = self._wake.wait(interval)
                if stop_event.is_set():
                    return
                self._wake.clear()
                if woke and self._config["event_delay_s"]:
                    if stop_event.wait(self._config["event_delay_s"]):
                        return
                    self._wake.clear()
                self._sample_round("event" if woke else "periodic")

        self._thread = StoppableThread(f"cluster-trace-{spec.id}", loop)
        self._thread.start()
        return make_result(spec, ComponentStatus.PASS, f"tracing {len(self._nodes)} node(s)")

    def _server_config(self, instance_id: str) -> tuple[dict[str, Any], Optional[str]]:
        """The ``config_keys`` the instance reported, and why it reported no more (None when it did)."""

        values: dict[str, Any] = {}
        with self._handle.new_client(instance_id, timeout=2.0) as client:
            try:
                for key in self._config["config_keys"]:
                    values.update(client.config_get(key))
            except (valkey.exceptions.ValkeyError, OSError) as exc:
                return values, f"{type(exc).__name__}: {exc}"
        return values, None

    def _on_event(self, event: Any) -> None:
        payload = dict(event.payload)
        logged_at = payload.get("logged_at")
        with self._lock:
            self._records.append(
                {
                    "kind": "event",
                    "t": logged_at if isinstance(logged_at, (int, float)) else time.time(),
                    "received_at": time.time(),
                    "sequence": event.sequence,
                    "name": event.name,
                    "source": event.source,
                    "payload": payload,
                }
            )
        if self._config["sample_on_events"]:
            self._wake.set()

    def _client(self, instance_id: str) -> Any:
        client = self._clients.get(instance_id)
        if client is None:
            client = self._handle.new_client(instance_id, timeout=self._config["sample_timeout_s"])
            self._clients[instance_id] = client
        return client

    def _sample_round(self, reason: str) -> None:
        if self._rounds >= self._config["max_rounds"]:
            return
        self._rounds += 1
        round_index = self._rounds
        for instance_id in self._nodes:
            record: dict[str, Any] = {
                "kind": "observation",
                "round": round_index,
                "reason": reason,
                "node": instance_id,
            }
            try:
                view = cluster_view(self._client(instance_id))
                # The trace records the view; the raw CLUSTER NODES records serve only the split check.
                view.pop("nodes")
                record.update(view)
            except Exception as exc:
                record["error"] = f"{type(exc).__name__}: {exc}"
                close_quietly(self._clients.pop(instance_id, None))
            record["t"] = time.time()
            try:
                record["disk"] = read_cluster_vars(nodes_conf_path(self._handle, instance_id))
            except Exception:
                record["disk"] = None
            with self._lock:
                self._records.append(record)

    def stop(self, context: Any, spec: ComponentSpec):
        if self._subscription is not None:
            context.event_bus.unsubscribe(self._subscription)
        if self._thread is not None:
            # With interval_s 0 the sampler waits only for an event: wake it to see the stop.
            self._thread.stop_event.set()
            self._wake.set()
        problem = self._thread.stop() if self._thread is not None else None
        if problem is not None:
            # The trace is partial, and a sampler still running owns its clients.
            return make_result(spec, ComponentStatus.ERROR, f"cluster tracing {problem}")
        self._sample_round("final")
        close_quietly(*self._clients.values())
        with self._lock:
            records = sorted(self._records, key=lambda item: (item["t"], item.get("sequence", 0)))
        path = os.path.join(component_artifact_dir(context, spec), TRACE_FILE)
        write_jsonl(path, [self._header, *records])
        counts = {
            "events": sum(record["kind"] == "event" for record in records),
            "observations": sum(record["kind"] == "observation" for record in records),
            "rounds": self._rounds,
        }
        return make_result(
            spec,
            ComponentStatus.PASS,
            f"trace: {counts['events']} events, {counts['observations']} observations",
            evidence=(
                make_evidence(spec, TRACE_EVIDENCE, "observer.stopped", {"path": path, **counts}, artifacts=(path,)),
            ),
        )


_TRACE_DECLARES = Declarations(
    evidence={
        TRACE_EVIDENCE: declared(
            mapping_of({"path": TEXT, "events": INTEGER, "observations": INTEGER, "rounds": INTEGER})
        )
    }
)

REGISTRATIONS = (
    component_registration(
        "valkey.observer.cluster-trace",
        ClusterTraceObserver,
        _TRACE_FIELDS,
        check=_validate_trace,
        selectors={"nodes": ()},
        pins_spec=True,
        deployment_modes=("cluster",),
        declares=_TRACE_DECLARES,
    ),
)
