"""The oracles every experiment inherits that judge a server by its own INFO: error counters that rose with
nothing in the scenario to explain them, and a quiet end. No Valkey processes: fake deployments answer."""

from types import SimpleNamespace

import pytest

from tests.support import VirtualClock
from tests.valkey_experiments_support import evidence_values, fake_context, resolved, unit_plan
from valkey_fuzzer.components import polling, server_stats
from valkey_fuzzer.components.server_stats import ErrorCountersOracle, QuietEndOracle, risen
from valkey_fuzzer.plan import ComponentSpec
from valkey_fuzzer.protocols import ComponentRole
from valkey_fuzzer.results import Evidence


class _Client:
    """One connection to a fake instance: INFO answers with the instance's next reply (an exception is raised)."""

    def __init__(self, handle, instance_id):
        self.handle, self.instance_id = handle, instance_id

    def info(self, *sections):
        replies = self.handle.infos[self.instance_id]
        reply = replies.pop(0) if len(replies) > 1 else replies[0]
        if isinstance(reply, Exception):
            raise reply
        return reply

    def execute_command(self, *command):
        assert command == ("CLUSTER", "NODES")
        return self.handle.cluster_nodes[self.instance_id]

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        pass


class _Handle:
    """A primary valkey-0 and its replicas, each answering INFO with the replies ``infos`` lists for it."""

    def __init__(self, infos, mode="standalone", down=()):
        self.infos = {instance_id: list(replies) for instance_id, replies in infos.items()}
        self.instances = tuple(
            SimpleNamespace(
                instance_id=instance_id,
                role="replica" if index else "primary",
                shard_id=0,
                host="127.0.0.1",
                port=7000 + index,
            )
            for index, instance_id in enumerate(infos)
        )
        self.mode = SimpleNamespace(value=mode)
        self.down = set(down)
        self.cluster_nodes = {}

    def is_alive(self, instance_id):
        return instance_id not in self.down

    def new_client(self, instance_id, timeout=5.0):
        return _Client(self, instance_id)


def _spec(component_id, **config):
    name = component_id.rsplit(".", 1)[1]
    return resolved(ComponentSpec(role=ComponentRole.ORACLE, use=component_id, id=name, config=config))


def _component(component_id, name, **config):
    return ComponentSpec(role=ComponentRole(component_id.split(".")[1]), use=component_id, id=name, config=config)


def _evidence(evidence_id, value, source="component"):
    return Evidence(evidence_id, source, "action.completed", 1.0, value)


@pytest.fixture
def clock(monkeypatch):
    """Time that passes only as the oracles wait."""

    return VirtualClock().install(monkeypatch, server_stats, polling)


# valkey.oracle.error-counters


def test_the_watched_counters_are_those_whose_names_say_something_went_wrong():
    info = {
        "unexpected_error_replies": 2,
        "sync_partial_err": 1,
        "rejected_connections": 3,
        "acl_access_denied_auth": 1,
        "client_output_buffer_limit_disconnections": 4,
        "search_rdb_load_failure_cnt": 5,
        "errorstat_ERR": {"count": 7},
        "rdb_last_bgsave_status": "err",
        # Not errors, at zero, ok, the sum of the errorstat entries, or one command's own statistics.
        "expired_keys": 9,
        "evicted_keys": 9,
        "keyspace_misses": 9,
        "aof_rewrites_consecutive_failures": 0,
        "errorstat_MOVED": {"count": 0},
        "aof_last_write_status": "ok",
        "master_link_status": "down",
        "total_error_replies": 7,
        "cmdstat_get": {"calls": 9, "failed_calls": 2, "rejected_calls": 1},
    }
    assert risen(info) == {
        "unexpected_error_replies": 2,
        "sync_partial_err": 1,
        "rejected_connections": 3,
        "acl_access_denied_auth": 1,
        "client_output_buffer_limit_disconnections": 4,
        "search_rdb_load_failure_cnt": 5,
        "errorstat_ERR": 7,
        "rdb_last_bgsave_status": "err",
    }


_KILL = {"obligation": "process.stopped"}
_DISK = {"obligation": "disk.filled"}
_TABLE = {"prefix": "p", "commands": 1, "skipped": [], "sent": 9, "distinct_sent": 1, "protocol_errors": []}
_OUTCOMES = {"replied": 5, "error": 4, "unanswered": 0, "protocol_error": 0}
_RANDOM_COMMANDS = {"evidence": [("valkey.workload.command-table", {**_TABLE, "outcomes": _OUTCOMES})]}
_NO_COMMAND_ERRORS = {
    "evidence": [("valkey.workload.command-table", {**_TABLE, "outcomes": {**_OUTCOMES, "error": 0}})]
}
_REFUSED_COMMAND = {"evidence": [("valkey.command.replies", [{"instance_id": "valkey-0", "error": "ERR no save"}])]}
_UNANSWERED_COMMAND = {
    "evidence": [("valkey.command.replies", [{"instance_id": "valkey-0", "error": "Timeout", "unanswered": True}])]
}
_REFUSED_FAULT = {"evidence": [("valkey.fault.injected", {"replies": {"valkey-0": {"error": "ERR no save"}}})]}
_KILLED_FOR_GOOD = {"evidence": [("valkey.instances.expected-down", {"instances": ["valkey-9"]})]}
_NONE_KILLED = {"evidence": [("valkey.instances.expected-down", {"instances": []})]}
_FAILED_OVER = {"evidence": [("valkey.failover.result", {"force": True})]}
_DROPS_LINKS = {"commands": [["CLIENT", "KILL", "TYPE", "replica"]]}
_REPOINTS = {"commands": [["REPLICAOF", "127.0.0.1", 7001]]}
_SAVES = {"commands": [["BGSAVE"]]}
_OWN_MAXMEMORY = {"deployment": {"server_config": {"maxmemory": "1mb"}}}
_SETS_MAXMEMORY = {"commands": [["CONFIG", "SET", "maxmemory", "1mb"]]}


@pytest.mark.parametrize(
    ("counter", "value", "scenario", "explained_by"),
    [
        # An error reply on a replication link is never the scenario's doing.
        ("unexpected_error_replies", 1, {}, None),
        ("unexpected_error_replies", 1, {**_KILL, **_RANDOM_COMMANDS}, None),
        ("client_query_buffer_limit_disconnections", 1, _KILL, None),
        ("rejected_connections", 1, _RANDOM_COMMANDS, None),
        # A failed save needs a disk that refused the write; a kill leaves the status ok.
        ("rdb_last_bgsave_status", "err", {}, None),
        ("rdb_last_bgsave_status", "err", _KILL, None),
        ("rdb_last_bgsave_status", "err", _DISK, "storage"),
        ("aof_last_write_status", "err", {"obligation": "directory.read-only"}, "storage"),
        ("aof_rewrites_consecutive_failures", 2, _DISK, "storage"),
        ("aof_rewrites_consecutive_failures", 2, {}, None),
        ("aof_rewrites_consecutive_failures", 2, _KILL, None),
        # The two replicas were each refused once when the deployment formed; a third refusal needs a broken link.
        ("sync_partial_err", 2, {}, "formation"),
        ("sync_partial_err", 3, {}, None),
        ("sync_partial_err", 3, _KILL, "links"),
        ("sync_partial_err", 3, {"obligation": "process.paused"}, "links"),
        ("sync_partial_err", 3, {"obligation": "network.partition"}, "links"),
        ("sync_partial_err", 3, _DISK, None),
        ("sync_partial_err", 3, _KILLED_FOR_GOOD, "links"),
        ("sync_partial_err", 3, _NONE_KILLED, None),
        ("sync_partial_err", 3, _FAILED_OVER, "links"),
        ("sync_partial_err", 3, {"evidence": [("valkey.chaos.schedule", {"kills": []})]}, "links"),
        ("sync_partial_err", 3, {"evidence": [("valkey.replication.replicaof", {})]}, "links"),
        ("sync_partial_err", 3, _DROPS_LINKS, "links"),
        ("sync_partial_err", 3, _REPOINTS, "links"),
        ("sync_partial_err", 3, {"commands": [["DEBUG", "CHANGE-REPL-ID"]]}, "links"),
        ("sync_partial_err", 3, _SAVES, None),
        ("client_output_buffer_limit_disconnections", 1, {}, None),
        ("client_output_buffer_limit_disconnections", 1, {"obligation": "process.paused"}, "links"),
        ("cluster_stats_messages_fail_sent", 4, {}, None),
        ("cluster_stats_messages_fail_sent", 4, _KILL, "links"),
        # An error reply needs a disturbed deployment or a command of the scenario's that Valkey refused.
        ("errorstat_ERR", {"count": 4}, {}, None),
        ("errorstat_ERR", {"count": 4}, _RANDOM_COMMANDS, "commands"),
        ("errorstat_ERR", {"count": 4}, _NO_COMMAND_ERRORS, None),
        ("errorstat_ERR", {"count": 4}, _REFUSED_COMMAND, "commands"),
        ("errorstat_ERR", {"count": 4}, _UNANSWERED_COMMAND, None),
        ("errorstat_ERR", {"count": 4}, _REFUSED_FAULT, "commands"),
        ("errorstat_READONLY", {"count": 1}, _FAILED_OVER, "links"),
        ("errorstat_MISCONF", {"count": 1}, _DISK, "storage"),
        # The provider's default maxmemory is one no dataset reaches; a scenario that sets one may reach it.
        ("errorstat_OOM", {"count": 1}, {}, None),
        ("errorstat_OOM", {"count": 1}, _OWN_MAXMEMORY, "memory"),
        ("errorstat_OOM", {"count": 1}, _SETS_MAXMEMORY, "memory"),
        ("errorstat_ERR", {"count": 1}, _OWN_MAXMEMORY, None),
        # A counter no rule names (a module's) is judged as an error reply is.
        ("search_rdb_load_failure_cnt", 1, {}, None),
        ("search_rdb_load_failure_cnt", 1, _KILL, "links"),
        ("search_query_failed_requests_count", 1, _RANDOM_COMMANDS, "commands"),
        # Only a command can be denied.
        ("acl_access_denied_auth", 1, {}, None),
        ("acl_access_denied_auth", 1, _KILL, None),
        ("acl_access_denied_auth", 1, _RANDOM_COMMANDS, "commands"),
    ],
)
def test_a_risen_error_counter_needs_a_cause_in_the_scenario(counter, value, scenario, explained_by):
    handle = _Handle({"valkey-0": [{counter: value}], "valkey-1": [{}], "valkey-2": [{}]})
    commands = [
        _component("valkey.action.command", f"command-{index}", command=command)
        for index, command in enumerate(scenario.get("commands", ()))
    ]
    context = fake_context(handle, *commands, deployment=scenario.get("deployment"))
    if "obligation" in scenario:
        fault = _component("valkey.fault.process-signal", "fault")
        context.obligations.open(fault, scenario["obligation"], "valkey-1", lambda: None).discharge()
    evidence = [_evidence(evidence_id, item) for evidence_id, item in scenario.get("evidence", ())]
    result = ErrorCountersOracle().evaluate(context, _spec("valkey.oracle.error-counters"), evidence)
    shown = value["count"] if isinstance(value, dict) else value
    if explained_by is None:
        assert result.status.value == "FAIL", result.summary
        what = "is err" if value == "err" else f"rose to {shown}"
        assert result.summary == f"valkey-0: {counter} {what}; nothing the scenario did explains it"
    else:
        assert result.status.value == "PASS", result.summary
        [record] = evidence_values(result.evidence, "valkey.error-counters.verified")
        assert record["instances"]["valkey-0"] == {counter: {"value": shown, "explained_by": explained_by}}
        assert record["scenario"][explained_by] if explained_by != "formation" else not any(record["scenario"].values())


def test_error_counters_are_read_only_from_running_instances_and_an_unread_one_is_no_finding(clock):
    timeout = TimeoutError("Timeout reading from socket")
    infos = {"valkey-0": [{}], "valkey-1": [timeout], "valkey-2": [{"unexpected_error_replies": 1}]}
    spec = _spec("valkey.oracle.error-counters")
    # A read that failed is retried: a restarted instance may still be loading.
    slow = _Handle({**infos, "valkey-1": [timeout, timeout, {}], "valkey-2": [{}]})
    assert ErrorCountersOracle().evaluate(clock.drive(fake_context(slow)), spec, ()).status.value == "PASS"
    # One that never answers leaves the run unjudged, unless another instance's counter is a finding.
    unread = ErrorCountersOracle().evaluate(clock.drive(fake_context(_Handle({**infos, "valkey-2": [{}]}))), spec, ())
    assert (unread.status.value, unread.summary) == ("ERROR", f"cannot read INFO of valkey-1: TimeoutError: {timeout}")
    found = ErrorCountersOracle().evaluate(clock.drive(fake_context(_Handle(infos))), spec, ())
    assert found.status.value == "FAIL" and found.summary.startswith("valkey-2: unexpected_error_replies rose to 1")
    # An instance that is not running has nothing to read: liveness judges it.
    down = _Handle(infos, down=("valkey-1", "valkey-2"))
    result = ErrorCountersOracle().evaluate(clock.drive(fake_context(down)), spec, ())
    assert (result.status.value, result.summary) == ("PASS", "no unexplained error counter on 1 instance(s)")


# valkey.oracle.quiet-end

_IDLE = {"role": "master", "blocked_clients": 0, "rdb_bgsave_in_progress": 0, "aof_rewrite_in_progress": 0}


def _cpu(seconds, main_thread=None, **fields):
    info = {**_IDLE, "used_cpu_user": seconds / 2, "used_cpu_sys": seconds / 2, **fields}
    if main_thread is not None:
        info.update(used_cpu_user_main_thread=main_thread, used_cpu_sys_main_thread=0.0)
    return info


def _quiet(clock, handle, evidence=(), plan=None, **config):
    context = clock.drive(fake_context(handle, plan=plan))
    return QuietEndOracle().evaluate(context, _spec("valkey.oracle.quiet-end", **config), evidence)


def test_a_deployment_at_rest_passes_after_one_cpu_window(clock):
    result = _quiet(clock, _Handle({"valkey-0": [_cpu(10.0), _cpu(10.01)], "valkey-1": [_cpu(4.0), _cpu(4.002)]}))
    assert result.status.value == "PASS", result.summary
    assert result.summary.startswith("2 instance(s) at rest") and "at most 0.01 of a core" in result.summary
    assert clock.elapsed == pytest.approx(1.0)
    [record] = evidence_values(result.evidence, "valkey.quiet-end.verified")
    assert {name: item["cpu"] for name, item in record["instances"].items()} == {"valkey-0": 0.01, "valkey-1": 0.002}


@pytest.mark.parametrize(
    ("busy", "reason"),
    [
        ({"blocked_clients": 2}, "2 blocked client(s)"),
        ({"rdb_bgsave_in_progress": 1}, "rdb_bgsave_in_progress"),
        ({"aof_rewrite_in_progress": 1}, "aof_rewrite_in_progress"),
        ({"module_fork_in_progress": 1}, "module_fork_in_progress"),
        ({"loading": 1}, "loading"),
    ],
)
def test_a_server_that_settles_within_the_bound_passes_and_one_that_never_does_fails(clock, busy, reason):
    settles = [_cpu(1.0, **busy), _cpu(1.0, **busy), _cpu(1.0), _cpu(1.0)]
    result = _quiet(clock, _Handle({"valkey-0": settles}))
    assert result.status.value == "PASS", result.summary
    assert clock.elapsed == pytest.approx(2.0)
    stuck = _quiet(clock, _Handle({"valkey-0": [_cpu(1.0, **busy)]}), timeout_s=5)
    assert stuck.status.value == "FAIL"
    assert stuck.summary == f"valkey-0: {reason}; not at rest within 5s"


def test_a_server_that_keeps_a_core_busy_is_spinning(clock):
    spinning = [_cpu(float(second)) for second in range(40)]
    result = _quiet(clock, _Handle({"valkey-0": spinning, "valkey-1": [_cpu(1.0)]}))
    assert result.status.value == "FAIL"
    assert result.summary == "valkey-0: used 1.00 of a core over 1.0s; not at rest within 30s"
    # Work that ends within the bound (a load, a rewrite's last writes) is no spin.
    finishing = [_cpu(0.0), _cpu(1.0), _cpu(2.0), _cpu(2.01)]
    assert _quiet(clock, _Handle({"valkey-0": finishing})).status.value == "PASS"
    # The limit is the experiment's to set.
    half = [_cpu(second * 0.4) for second in range(40)]
    assert _quiet(clock, _Handle({"valkey-0": half})).status.value == "PASS"
    assert _quiet(clock, _Handle({"valkey-0": half}), max_cpu=0.25).status.value == "FAIL"


def test_cpu_use_is_the_main_threads_where_info_reports_it_else_shared_among_always_active_io_threads(clock):
    # Linux reports the main thread's own CPU time: IO threads that spin by design do not count.
    threads_busy = [_cpu(second * 3.0, main_thread=second * 0.02) for second in range(40)]
    assert _quiet(clock, _Handle({"valkey-0": threads_busy})).status.value == "PASS"
    main_busy = [_cpu(second * 1.0, main_thread=second * 0.9) for second in range(40)]
    assert _quiet(clock, _Handle({"valkey-0": main_busy})).summary.startswith("valkey-0: used 0.90 of a core")
    # Elsewhere the process's CPU time is all there is: four always-active IO threads run whenever the main
    # thread does, so 1.2 cores among them is 0.3 each.
    process = [_cpu(second * 1.2) for second in range(40)]
    always_active = {"io-threads": "4", "io-threads-always-active": "yes"}
    plans = {
        name: unit_plan(document={"mode": "standalone", "server_config": config})
        for name, config in {"always": always_active, "on-demand": {"io-threads": "4"}}.items()
    }
    assert _quiet(clock, _Handle({"valkey-0": list(process)}), plan=plans["always"]).status.value == "PASS"
    assert _quiet(clock, _Handle({"valkey-0": list(process)}), plan=plans["on-demand"]).status.value == "FAIL"


def test_a_restarted_process_starts_a_new_cpu_window(clock):
    # Its CPU time went down: the first read after the restart measures nothing.
    result = _quiet(clock, _Handle({"valkey-0": [_cpu(50.0), _cpu(0.1), _cpu(0.11)]}))
    assert result.status.value == "PASS", result.summary
    assert clock.elapsed == pytest.approx(2.0)


def test_a_replication_link_must_be_up_unless_its_primary_is_down(clock):
    down = {"role": "slave", "master_host": "127.0.0.1", "master_port": 7000, "master_link_status": "down"}
    up = {**down, "master_link_status": "up"}
    infos = {"valkey-0": [_cpu(1.0)], "valkey-1": [_cpu(1.0, **down)]}
    result = _quiet(clock, _Handle(infos), timeout_s=3)
    assert (result.status.value, result.summary) == (
        "FAIL",
        "valkey-1: replication link to valkey-0 is down; not at rest within 3s",
    )
    reconnects = {**infos, "valkey-1": [_cpu(1.0, **down), _cpu(1.0, **down), _cpu(1.0, **up)]}
    assert _quiet(clock, _Handle(reconnects)).status.value == "PASS"
    # A replica cannot follow a primary that is not running, and liveness judges that primary.
    assert _quiet(clock, _Handle(infos, down=("valkey-0",))).status.value == "PASS"


def _nodes(*lines):
    return "\n".join(
        f"{index:040x} 127.0.0.1:{7000 + index}@17000 {flags} - 0 0 1 {link}"
        for index, (flags, link) in enumerate(lines)
    )


def test_a_cluster_node_must_see_every_running_node_connected_and_none_failed(clock):
    infos = {name: [_cpu(1.0)] for name in ("valkey-0", "valkey-1", "valkey-2")}

    def cluster(view, down=()):
        handle = _Handle(infos, mode="cluster", down=down)
        healthy = _nodes(("master", "connected"), ("master", "connected"), ("master", "connected"))
        handle.cluster_nodes = {"valkey-0": view, "valkey-1": healthy, "valkey-2": healthy}
        return handle

    healthy = _nodes(("myself,master", "connected"), ("master", "connected"), ("master", "connected"))
    assert _quiet(clock, cluster(healthy)).status.value == "PASS"
    broken = _nodes(("myself,master", "connected"), ("master", "disconnected"), ("master,fail?", "connected"))
    result = _quiet(clock, cluster(broken), timeout_s=3)
    assert result.status.value == "FAIL"
    assert result.summary == (
        "valkey-0: cluster link to valkey-1 is disconnected, sees valkey-2 as failed; not at rest within 3s"
    )
    # Nodes that are not running are down for everyone: nothing to be connected to.
    assert _quiet(clock, cluster(broken, down=("valkey-1", "valkey-2"))).status.value == "PASS"


def test_an_instance_stopped_on_purpose_or_unread_is_not_judged(clock):
    timeout = TimeoutError("Timeout reading from socket")
    infos = {"valkey-0": [_cpu(1.0)], "valkey-1": [timeout]}
    result = _quiet(clock, _Handle(infos), timeout_s=3)
    assert (result.status.value, result.summary) == ("ERROR", f"cannot judge valkey-1: TimeoutError: {timeout}")
    # Another instance's finding stands.
    blocked = _quiet(clock, _Handle({**infos, "valkey-0": [_cpu(1.0, blocked_clients=1)]}), timeout_s=3)
    assert blocked.status.value == "FAIL" and blocked.summary.startswith("valkey-0: 1 blocked client(s)")
    # An instance that answers within the bound is judged from then on.
    late = _Handle({**infos, "valkey-1": [timeout, _cpu(1.0), _cpu(1.0)]})
    assert _quiet(clock, late).status.value == "PASS"
    # One a component killed for good is not running, and is not read.
    stopped = [_evidence("valkey.instances.expected-down", {"instances": ["valkey-1"]})]
    assert _quiet(clock, _Handle(infos, down=("valkey-1",)), evidence=stopped).status.value == "PASS"


def test_every_shipped_experiment_inherits_the_generic_oracles():
    from valkey_fuzzer.components import experiments

    for experiment in experiments():
        uses = {component.id: component.use for component in experiment.components}
        assert uses.get("error-counters") == "valkey.oracle.error-counters", experiment.experiment_id
        assert uses.get("quiet-end") == "valkey.oracle.quiet-end", experiment.experiment_id
