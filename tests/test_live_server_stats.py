"""The oracles every experiment inherits, against one real server: both pass while it is left alone, and each
fails when the server is not at rest, or an error counter rose with nothing in the scenario to explain it."""

import subprocess
import threading
import time
from types import SimpleNamespace

import pytest
import valkey

from tests.live_support import BINARY, free_port_block
from tests.valkey_experiments_support import fake_context, resolved
from valkey_fuzzer.components.server_stats import ErrorCountersOracle, QuietEndOracle
from valkey_fuzzer.plan import ComponentSpec
from valkey_fuzzer.protocols import ComponentRole
from valkey_fuzzer.results import Evidence

pytestmark = [pytest.mark.live, pytest.mark.slow]

if not BINARY:
    pytest.skip("valkey-server is not available", allow_module_level=True)


class _Handle:
    """What the oracles ask a deployment for, answered by one standalone server."""

    mode = SimpleNamespace(value="standalone")

    def __init__(self, port):
        self.port = port
        self.instances = (
            SimpleNamespace(instance_id="valkey-0", role="primary", shard_id=0, host="127.0.0.1", port=port),
        )

    def is_alive(self, instance_id):
        return True

    def new_client(self, instance_id="valkey-0", timeout=5.0):
        return valkey.Valkey(port=self.port, decode_responses=True, socket_timeout=timeout)


@pytest.fixture
def server(tmp_path):
    handle = _Handle(free_port_block(1))
    command = [BINARY, "--port", str(handle.port), "--bind", "127.0.0.1", "--dir", str(tmp_path), "--save", ""]
    process = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        deadline = time.monotonic() + 30
        while True:
            try:
                handle.new_client().ping()
                break
            except valkey.ValkeyError:
                assert time.monotonic() < deadline, "the server did not start"
                time.sleep(0.1)
        yield handle
    finally:
        process.kill()
        process.wait()


def _oracle(component_id, **config):
    name = component_id.rsplit(".", 1)[1]
    return resolved(ComponentSpec(role=ComponentRole.ORACLE, use=component_id, id=name, config=config))


def _quiet(handle, **config):
    return QuietEndOracle().evaluate(fake_context(handle), _oracle("valkey.oracle.quiet-end", **config), ())


def _counters(handle, evidence=()):
    return ErrorCountersOracle().evaluate(fake_context(handle), _oracle("valkey.oracle.error-counters"), evidence)


def test_a_client_left_blocked_is_no_quiet_end(server):
    assert _quiet(server).status.value == "PASS"
    blocked = threading.Thread(target=lambda: server.new_client(timeout=30).blpop("nothing", 6), daemon=True)
    blocked.start()
    result = _quiet(server, timeout_s=2)
    assert result.status.value == "FAIL" and "valkey-0: 1 blocked client(s)" in result.summary, result.summary
    blocked.join()
    assert _quiet(server).status.value == "PASS"


def test_a_server_that_keeps_its_main_thread_busy_is_spinning(server):
    stop = threading.Event()

    def burn():
        client = server.new_client(timeout=30)
        while not stop.is_set():
            client.eval("local n = 0 for i = 1, 3000000 do n = n + i end return n", 0)

    burners = [threading.Thread(target=burn, daemon=True) for _ in range(2)]
    for thread in burners:
        thread.start()
    try:
        result = _quiet(server, timeout_s=3)
    finally:
        stop.set()
        for thread in burners:
            thread.join()
    assert result.status.value == "FAIL" and "of a core over" in result.summary, result.summary
    # The load is over: the same server comes to rest.
    result = _quiet(server)
    assert result.status.value == "PASS", result.summary


def test_an_error_reply_needs_a_cause_in_the_scenario(server):
    assert _counters(server).status.value == "PASS"
    with pytest.raises(valkey.ValkeyError):
        server.new_client().execute_command("AUTH", "nobody", "wrong")
    result = _counters(server)
    assert result.status.value == "FAIL", result.summary
    assert "acl_access_denied_auth rose to 1" in result.summary and "errorstat_WRONGPASS rose to 1" in result.summary
    # The same counters, in a scenario whose own command is on record as refused.
    refused = [{"instance_id": "valkey-0", "error": "WRONGPASS invalid username-password pair"}]
    evidence = [Evidence("valkey.command.replies", "auth", "action.completed", time.time(), refused)]
    result = _counters(server, evidence)
    assert result.status.value == "PASS", result.summary
