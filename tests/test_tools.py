"""``valkey.action.tools`` without Valkey builds: two fake builds whose tools are one script that runs the
steps its arguments name, and fake private servers behind the real private-server side, and the verdicts."""

import os
import sys

import pytest

from tests.test_command_sequence import BUILDS, SERVERS, Build, FakeServer, fake_open_raw_connection
from tests.valkey_experiments_support import registration_for, resolved, unit_plan
from valkey_fuzzer._data import to_plain_data
from valkey_fuzzer.components import actions, tools
from valkey_fuzzer.plan import ComponentSpec
from valkey_fuzzer.protocols import ComponentRole
from valkey_fuzzer.schema import ConfigError, load_yaml
from valkey_fuzzer.testing import FakeContext

# Each argument is a step: out:<text>, err:<text>, label (the build's), cwd, stdin, env:<name>,
# cat:<file>, exit:<status>, and <step>@<label> runs the step on that build alone; crash is SIGSEGV, hang
# sleeps, grow:<dir> leaves a file named by its pid in <dir> (which the tests read as memory running away) and
# sleeps, write:<file> writes the arguments into a file; node:<socket> and short:<socket> print the node ID of
# the fake server with that socket (see FakeServer), node: in full and abbreviated, short: its first 8 digits.
TOOL = """#!{python}
import hashlib, os, signal, sys, time
LABEL = {label!r}
for step in sys.argv[1:]:
    step, _, only = step.partition("@")
    if only and only != LABEL:
        continue
    name, _, value = step.partition(":")
    if name == "out": print(value)
    elif name == "err": print(value, file=sys.stderr)
    elif name == "label": print(LABEL)
    elif name == "cwd": print(os.getcwd())
    elif name == "stdin": sys.stdout.buffer.write(sys.stdin.buffer.read())
    elif name == "env": print(os.environ.get(value, "-"))
    elif name == "cat": sys.stdout.buffer.write(open(value, "rb").read())
    elif name == "write": open(value, "w").write(" ".join(sys.argv[1:]))
    elif name == "exit": sys.exit(int(value))
    elif name == "crash": sys.stdout.flush(); os.kill(os.getpid(), signal.SIGSEGV)
    elif name == "hang": time.sleep(30)
    elif name == "grow": open(os.path.join(value, str(os.getpid())), "w").close(); time.sleep(30)
    elif name in ("node", "short"):
        node_id = hashlib.sha1(os.path.dirname(value).encode()).hexdigest()
        print(f"{{node_id}} ({{node_id[:8]}}...)" if name == "node" else node_id[:8])
"""


@pytest.fixture
def builds(monkeypatch, tmp_path):
    """``{label: valkey-server path}`` of the ``candidate`` and ``parent`` fake builds, each with its tools."""

    BUILDS.clear()
    SERVERS.clear()
    monkeypatch.setattr(actions, "PrivateServer", FakeServer)
    monkeypatch.setattr(actions, "open_raw_connection", fake_open_raw_connection)
    paths = {}
    for label in ("candidate", "parent"):
        src = tmp_path / label / "src"
        src.mkdir(parents=True)
        for name in ("valkey-server", *tools.TOOLS):
            path = src / name
            path.write_text(TOOL.format(python=sys.executable, label=label))
            path.chmod(0o755)
        paths[label] = str(src / "valkey-server")
        BUILDS[paths[label]] = Build()
    return paths


def _spec(**config):
    return resolved(ComponentSpec(role=ComponentRole.ACTION, use="valkey.action.tools", id="tools", config=config))


def run(builds, runs, **config):
    """The action on both builds with ``runs``; the result, its evidence value, and the component's folder."""

    spec = _spec(compare_with=builds["parent"], runs=runs, **config)
    context = FakeContext(spec, deployment={"valkey_binary": builds["candidate"]})
    result = tools.ToolsAction().run(context, spec)
    folder = os.path.join(context.artifact_dir, "components", "tools")
    return result, (to_plain_data(result.evidence[0].value) if result.evidence else None), folder


def cli(*steps, **run):
    return {"tool": "valkey-cli", "args": list(steps), **run}


# ---------------------------------------------------------------------------
# What a run is given and what is recorded
# ---------------------------------------------------------------------------


def test_each_side_runs_its_own_tool_in_its_working_directory(builds):
    result, value, folder = run(builds, [cli("cwd", "out:café", "err:warn", "exit:3")])

    assert result.status.value == "PASS", result.summary
    record = value["candidate"]["runs"][0]
    assert record == {"index": 0, "tool": "valkey-cli", "exit": 3, "stdout": "$dir\ncafé\n", "stderr": "warn\n"}
    assert value["parent"]["runs"] == value["candidate"]["runs"]
    assert value["candidate"]["digest"] == value["parent"]["digest"]
    assert (value["reached"], value["unexplained"], value["server"]) == ([], [], False)
    assert sorted(os.listdir(folder)) == ["candidate", "parent"]


def test_files_stdin_and_env_reach_the_tool_and_the_callers_cli_variables_do_not(builds, monkeypatch):
    monkeypatch.setenv("VALKEYCLI_AUTH", "leaked")
    monkeypatch.setenv("REDISCLI_AUTH", "leaked")
    runs = [
        cli("cat:dump.rdb", "cat:notes.txt", "stdin", stdin={"$base64": "AAE="}),
        cli("env:VALKEYCLI_AUTH", "env:REDISCLI_AUTH", "env:HOME", env={"VALKEYCLI_AUTH": "secret"}),
        {"tool": "valkey-check-rdb", "args": ["out:checked"], "expect": {"stdout": {"equals": "checked\n"}}},
    ]
    result, value, folder = run(builds, runs, files={"dump.rdb": {"$base64": "/wD/"}, "notes.txt": "text\n"})

    assert result.status.value == "PASS", result.summary
    outputs = [record["stdout"] for record in value["candidate"]["runs"]]
    assert outputs[0] == {"$base64": "/wD/dGV4dAoAAQ=="}
    assert outputs[1] == "secret\n-\n$dir\n"
    with open(os.path.join(folder, "parent", "dump.rdb"), "rb") as handle:
        assert handle.read() == b"\xff\x00\xff"


def test_a_server_side_names_its_own_port_and_socket_and_the_output_names_them_back(builds):
    runs = [cli("-p", "$port", "-s", "$socket", "out:[::1]:$port", "out:$socket", "write:argv.txt")]
    result, value, folder = run(builds, runs, server=True)

    assert result.status.value == "PASS", result.summary
    assert value["server"] is True
    assert value["candidate"]["runs"][0]["stdout"] == "[::1]:$port\n$socket\n"
    servers = {server.binary: server for server in SERVERS.values()}
    for label in ("candidate", "parent"):
        arguments = servers[builds[label]].arguments
        port = arguments[arguments.index("--port") + 1]
        assert arguments[arguments.index("--dir") + 1] == os.path.realpath(os.path.join(folder, label))
        assert arguments[arguments.index("--bind") + 1] == "127.0.0.1 -::1"
        with open(os.path.join(folder, label, "argv.txt")) as handle:
            assert f"-p {port} -s {servers[builds[label]].socket_path}" in handle.read()
    assert all(server.stopped for server in SERVERS.values())
    assert {os.path.basename(path) for path in result.evidence[0].artifacts} == {
        "candidate.server.log", "candidate.output.log", "parent.server.log", "parent.output.log"
    }  # fmt: skip


def test_a_cluster_sides_node_ids_are_named_as_compared_sequences_name_them(builds):
    other = "0123456789abcdef" * 2 + "01234567"
    runs = [cli("node:$socket", f"out:{other}"), cli("short:$socket", compare="skip")]
    result, value, _ = run(builds, runs, server=True, cluster=True)

    # Only the side's own node ID, in full or abbreviated as valkey-cli prints it, never other hex.
    assert result.status.value == "PASS", result.summary
    for label in ("candidate", "parent"):
        assert value[label]["runs"][0]["stdout"] == f"<node.0.id> (<node.0.id>)\n{other}\n"
    server = next(server for server in SERVERS.values() if server.node and server.binary == builds["candidate"])
    assert value["candidate"]["runs"][1]["stdout"] == f"{server.id[:8]}\n"


# ---------------------------------------------------------------------------
# Comparing the two builds
# ---------------------------------------------------------------------------


def test_a_marked_run_that_differs_reached_the_change_and_its_twin_did_not(builds):
    runs = [
        cli("label", expect_difference=True, expect={"stdout": {"equals": "candidate\n"}}),
        cli("out:same", expect_difference=True, expect={"exit": {"equals": 0}}),
    ]
    result, value, _ = run(builds, runs)

    assert result.status.value == "ERROR" and result.diagnostics["reason"] == "unreached-change"
    assert value["reached"] == [0]
    assert value["candidate"]["digest"] != value["parent"]["digest"]


def test_an_unmarked_run_that_differs_is_an_unexplained_difference_with_both_sides(builds):
    result, value, _ = run(builds, [cli("err:warning@parent", "out:OK")])

    assert result.status.value == "ERROR" and result.diagnostics["reason"] == "unexplained-difference"
    difference = to_plain_data(result.diagnostics["runs"])[0]
    assert difference["run"] == "0 (valkey-cli err:warning@parent out:OK)"
    assert (difference["candidate"]["stderr"], difference["parent"]["stderr"]) == ("", "warning\n")
    assert value["unexplained"] == [0]


@pytest.mark.parametrize(
    "steps, config, status",
    [
        (["out:b@candidate", "out:a", "out:b@parent"], {"compare": "unordered"}, "PASS"),
        (["out:b@candidate", "out:a", "out:b@parent"], {}, "ERROR"),
        (["label"], {"compare": "skip"}, "PASS"),
    ],
)
def test_compare_modes(builds, steps, config, status):
    result, _, _ = run(builds, [cli(*steps, **config)])

    assert result.status.value == status, result.summary


def test_ignore_removes_what_differs_between_processes_from_the_comparison_not_the_record(builds):
    runs = [cli("out:1234:M 03 Nov 2026 * Ready@candidate", "out:99:M 04 Nov 2026 * Ready@parent", "out:done")]
    result, value, _ = run(builds, runs, ignore=[r"^\d+:[A-Z] .*\n"])

    assert result.status.value == "PASS", result.summary
    assert value["candidate"]["runs"][0]["stdout"] == "1234:M 03 Nov 2026 * Ready\ndone\n"


# ---------------------------------------------------------------------------
# Verdicts on the candidate
# ---------------------------------------------------------------------------


def test_an_expect_the_candidate_violates_and_the_parent_meets_is_fail(builds):
    result, value, _ = run(builds, [cli("exit:1@candidate", expect={"exit": {"equals": 0}})])

    assert result.status.value == "FAIL" and not result.crash
    assert result.summary.startswith("run 0 (valkey-cli exit:1@candidate) gave {")
    assert value["violating"] == [0]


def test_an_expect_both_builds_violate_alike_is_disputed(builds):
    result, value, _ = run(builds, [cli("exit:1", expect={"exit": {"equals": 0}})])

    assert result.status.value == "ERROR" and result.diagnostics["reason"] == "expect-disputed"
    assert to_plain_data(result.diagnostics["runs"])[0]["expect"] == {"exit": {"equals": 0}}
    assert value["violating"] == [0]


@pytest.mark.parametrize(
    "steps, cause",
    [
        (["out:before", "crash@candidate"], "killed by signal 11"),
        (["err:==1==ERROR: AddressSanitizer: heap-use-after-free on address", "exit:1"], "AddressSanitizer: heap-use-after-free"),
    ],
)  # fmt: skip
def test_a_candidate_tool_that_crashes_is_a_fail(builds, steps, cause):
    result, value, _ = run(builds, [cli("out:first"), cli(*steps)])

    assert result.status.value == "FAIL" and result.crash, result.summary
    assert dict(result.diagnostics["crashes"]) == {"run 1": cause}
    assert len(value["candidate"]["runs"]) == 2


def test_a_marked_run_the_candidate_crashes_against_its_expect_is_recorded_as_violating(builds):
    # valkey#2177's merge: the marked cli runs differed only because the candidate aborted where its expect
    # wanted exit 0, which is the merge breaking the run, not the change showing (fuzzer#113).
    marked = cli("crash@candidate", "out:OK", expect_difference=True, expect={"exit": {"equals": 0}})
    result, value, _ = run(builds, [marked])

    assert result.status.value == "FAIL" and result.crash, result.summary
    assert value["reached"] == [0] and value["violating"] == [0]


def test_a_candidate_server_that_exits_is_a_fail(builds, monkeypatch):
    ran = tools._Side.run

    def run_then_die(side, index, item, timeout_s):
        ran(side, index, item, timeout_s)
        if side.label == "candidate":
            side.server.nodes[0].server.die()

    monkeypatch.setattr(tools._Side, "run", run_then_die)
    result, value, _ = run(builds, [cli("-p", "$port", "out:OK")], server=True)

    assert result.status.value == "FAIL" and result.crash
    assert dict(result.diagnostics["crashes"]) == {"server": "crashed by signal: 11"}
    assert value["candidate"]["server_exited"] == -11


def test_a_parent_tool_that_crashes_on_a_marked_run_reached_the_change(builds):
    marked = cli("crash@parent", "out:usage", expect_difference=True, expect={"stdout": {"contains": "usage"}})
    result, value, _ = run(builds, [marked])

    assert result.status.value == "PASS", result.summary
    assert value["parent"]["runs"][0]["signal"] == 11 and value["parent"]["runs"][0]["exit"] is None
    assert value["reached"] == [0]


def test_a_run_that_never_exits_is_killed_and_recorded(builds):
    result, value, _ = run(builds, [cli("out:started", "hang@candidate")], timeout_s=1)

    assert result.status.value == "ERROR"
    assert result.summary == "run 0 (valkey-cli out:started hang@candidate) did not exit within 1s"
    assert value["candidate"]["runs"][0]["timed_out"] is True

    result, value, _ = run(
        builds, [cli("hang@parent", expect_difference=True, expect={"exit": {"equals": 0}})], timeout_s=1
    )
    assert result.status.value == "PASS" and value["reached"] == [0]


def test_a_build_without_the_tool_is_untested(builds):
    os.remove(os.path.join(os.path.dirname(builds["parent"]), "valkey-benchmark"))
    result, value, _ = run(builds, [{"tool": "valkey-benchmark"}])

    assert result.status.value == "ERROR" and value is None
    assert result.summary.startswith("the parent build has no valkey-benchmark beside its valkey-server")


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "config, error",
    [
        ({"runs": []}, "at least one run"),
        ({"runs": [{"tool": "sh", "args": ["-c", "id"]}]}, "must be one of"),
        ({"runs": [{"tool": "valkey-cli", "args": ["/etc/passwd"]}]}, "outside the working directory"),
        ({"runs": [{"tool": "valkey-cli", "args": ["sub/../../x"]}]}, "outside the working directory"),
        ({"runs": [{"tool": "valkey-cli", "args": ["-p", "$port"]}]}, "needs server"),
        ({"runs": [{"tool": "valkey-cli", "args": [True]}]}, "strings or numbers"),
        ({"runs": [{"tool": "valkey-cli", "env": {"A B": "x"}}]}, "not an environment variable name"),
        ({"runs": [{"tool": "valkey-cli", "expect_difference": True}]}, "needs an expect"),
        ({"runs": [{"tool": "valkey-cli", "expect": {}}]}, "needs exit, stdout, or stderr"),
        ({"runs": [{"tool": "valkey-cli", "expect": {"stdout": {}}}]}, "needs one predicate"),
        ({"runs": [{"tool": "valkey-cli", "expect": {"status": {"equals": 0}}}]}, "status"),
        ({"files": {"../x": "a"}, "runs": [{"tool": "valkey-cli"}]}, "plain file name"),
        ({"files": {"x": 1}, "runs": [{"tool": "valkey-cli"}]}, "text or a !!binary"),
        ({"ignore": ["("], "runs": [{"tool": "valkey-cli"}]}, "not a regular expression"),
        ({"cluster": True, "runs": [{"tool": "valkey-cli"}]}, "cluster needs server"),
    ],
)
def test_a_bad_config_fails_to_compile(config, error):
    spec = ComponentSpec(
        role=ComponentRole.ACTION, use="valkey.action.tools", id="tools", config={"compare_with": "/p", **config}
    )
    with pytest.raises(ConfigError, match=error):
        registration_for("valkey.action.tools").resolve_config(spec, unit_plan((spec,)))


def test_a_yaml_binary_file_and_a_placeholder_argument_load_as_written():
    config = load_yaml(
        "compare_with: /p\nserver: true\nfiles: {e.rdb: !!binary UkVESVMwMDA5/gL/AAAAAAAAAAA=}\n"
        "runs: [{tool: valkey-cli, args: [--cluster, check, '[::1]:$port']}]\n",
        "inline",
    )
    spec = ComponentSpec(role=ComponentRole.ACTION, use="valkey.action.tools", id="tools", config=config)
    resolved_config = registration_for("valkey.action.tools").resolve_config(spec, unit_plan((spec,)))

    assert resolved_config["files"]["e.rdb"] == {"$base64": "UkVESVMwMDA5/gL/AAAAAAAAAAA="}
    assert resolved_config["runs"][0]["args"][2] == "[::1]:$port"


def test_a_run_whose_memory_runs_away_is_stopped_before_it_takes_the_host(builds, monkeypatch, tmp_path):
    # As in the walk's test of valkey-io/valkey#5173: before it, valkey-cli --hotkeys-count -1 asks for 64 GB of
    # counters, which took the CI runner down twice.
    grown = tmp_path / "grown"
    grown.mkdir()
    monkeypatch.setattr(tools, "_resident_bytes", lambda pid: 8 << 30 if (grown / str(pid)).exists() else 1 << 20)
    marked = cli(
        f"grow:{grown}@parent",
        "err:Invalid hotkeys count@candidate",
        "exit:1@candidate",
        expect_difference=True,
        expect={"exit": {"equals": 1}},
    )
    result, value, _ = run(builds, [marked], timeout_s=20)

    assert result.status.value == "PASS", result.summary
    assert value["parent"]["runs"][0]["memory_exceeded"] is True and value["parent"]["runs"][0]["exit"] is None
    assert value["reached"] == [0]

    result, value, _ = run(builds, [cli(f"grow:{grown}@candidate")], timeout_s=20)
    assert result.status.value == "FAIL"
    assert "used more than 4 GiB of memory" in result.summary, result.summary
