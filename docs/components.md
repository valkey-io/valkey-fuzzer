# Valkey component reference

The Valkey components in `valkey_fuzzer.components` are the 35 components,
9 generator hooks, and 3 providers on this page. Experiments configure them; how is in
[Scenarios and experiments](scenarios-and-experiments.md#components).
`valkey-fuzzer list components` and `valkey-fuzzer list generators` print what
is installed. The generator hooks are listed under
[Generator hooks](scenarios-and-experiments.md#generator-hooks); they are not
components.

Component IDs have the form `<namespace>.<role>.<name>`. Every config is
checked when the scenario compiles, and the execution plan records it with
every default filled in, so a report shows exactly what ran. The one
exception is the `validation` settings of `valkey.action.cluster-chaos`,
`valkey.action.replication-chaos`, and `valkey.oracle.deployment-state`, whose
left-out keys the deployment fills in when the run starts. In the tables,
a field without a default is required, ranges are inclusive, and "or null"
means `null` is accepted.

- [Common config forms](#common-config-forms)
- [Summary](#summary)
- [Events, evidence, and state changes](#events-evidence-and-state-changes)
- [Capabilities](#capabilities)
- [Workloads](#workloads)
- [Actions](#actions)
- [Faults](#faults)
- [Observers](#observers)
- [Oracles](#oracles)
- [Providers](#providers)

## Common config forms

### Instance selectors

Fields that pick instances take a selector:

| Selector | Selects |
| --- | --- |
| `all` | every instance |
| `primaries` / `primary` | every primary / the first primary |
| `replicas` / `replica` | every replica / the first replica |
| `valkey-2` | that instance |
| `[valkey-0, valkey-3]` | those instances, in that order |

The fields `instances`, `isolate`, `peers`, and the trace observer's `nodes`
take any of these forms. The single-instance fields (`instance` of the
process-signal fault, the command fault, and the restart action, and
`target` of the failover action) take a word or an instance ID, not a list,
and use the first instance it selects. The process-signal and command faults
also accept `event`, the instance named by the `instance_id` field of the
triggering event's payload. Startup overlays also accept a mapping
`{roles, instance_ids, first}` (see
[startup_overlays](scenarios-and-experiments.md#startup_overlays)). Every
component checks its selector fields when the scenario compiles, so a
malformed selector, or an instance ID not of the form `valkey-N`, is a
compile error.

Roles are the deployment's live roles at the time the selector is resolved,
so a promoted replica counts as a primary; instances come in plan order. An
action resolves its selector when it runs and an observer when it starts. A
fault resolves its selector when it installs its trigger at the start of the
run, not when the trigger fires, so `instance: primary` names the instance
that was primary then.

### Predicates

A predicate is one of `{exists: true|false}`, `{equals: V}`,
`{not_equals: V}`, `{in: [V, ...]}`, `{contains: S}`, or a numeric
`{min: N}`, `{max: N}`, or both. Values compare as strings, except `min` and
`max`, which compare as numbers.

### Trigger fields

Every fault takes these fields, and injects once, synchronously inside the
dispatch of the matching event, after `delay_s`:

| Field | Default | Meaning |
| --- | --- | --- |
| `when_event` | | The event that triggers the fault, for example `run-before-crash.completed` or `bgsave.save-started`. |
| `occurrence` | 1 | Fire on the n-th matching event (1 to 1,000,000). |
| `when_payload` | `{}` | Payload fields the event must match: a scalar (compared as a string), a list (any of), or a predicate. |
| `delay_s` | 0 | Seconds (0 to 600) to wait inside the trigger's dispatch before firing. A fault whose call is cancelled during the wait does not fire and is not applied. |

A fault publishes `<id>.injected` when it fires (with `applied`, `trigger`,
`applied_at`, and its target) and records `valkey.fault.injected` evidence
when it applied. Its `recover` returns `ERROR` if it never fired, which
becomes the fault's result, and then a failure of anything that depends on
it is not trusted: the run is `ERROR`. A fault that refuses to
fire because firing would be unsafe, such as filling a path that is not the
run's tmpfs, also makes the run `ERROR`. Each change a fault applies is owned
by an undo obligation (its kind is in the [declarations table](#events-evidence-and-state-changes));
an early undo (a heal, a release, the end of a pause, an immediate restart)
discharges it, `recover` discharges the rest, and an undo that failed before
a retry succeeded makes the fault, and the run, `ERROR`.

## Summary

| Component | Role | Modes | Does |
| --- | --- | --- | --- |
| [`valkey.capability.build-probe`](#valkeycapabilitybuild-probe) | capability | any | checks each binary supports the experiment |
| [`valkey.capability.host-probe`](#valkeycapabilityhost-probe) | capability | any | checks the host (Linux, root, tmpfs, iptables) |
| [`valkey.workload.key-value`](#valkeyworkloadkey-value) | workload | any | writes keys and records every client-visible result |
| [`valkey.action.command`](#valkeyactioncommand) | action | any | sends a command and checks the reply |
| [`valkey.action.commands`](#valkeyactioncommands) | action | any | sends a command sequence on one connection, and compares the replies of two builds |
| [`valkey.action.tools`](#valkeyactiontools) | action | any | runs a Valkey tool of two builds and compares what they do |
| [`valkey.action.bgsave`](#valkeyactionbgsave) | action | any | runs BGSAVE to completion |
| [`valkey.action.failover`](#valkeyactionfailover) | action | cluster | fails over one shard |
| [`valkey.action.restart-instance`](#valkeyactionrestart-instance) | action | any | restarts one instance |
| [`valkey.action.sleep`](#valkeyactionsleep) | action | any | waits |
| [`valkey.action.poll`](#valkeyactionpoll) | action | any | waits for an INFO field or reply to match |
| [`valkey.action.wait-event`](#valkeyactionwait-event) | action | any | waits for an event |
| [`valkey.action.rolling-upgrade`](#valkeyactionrolling-upgrade) | action | standalone, cluster | moves every instance to a new build |
| [`valkey.action.cluster-chaos`](#valkeyactioncluster-chaos) | action | cluster | failover waves with process kills |
| [`valkey.action.cluster-election`](#valkeyactioncluster-election) | action | cluster | forces one failover election |
| [`valkey.action.replication-chaos`](#valkeyactionreplication-chaos) | action | standalone | waves of kills, promotions, repoints, stalls, and partitions, healed and validated |
| [`valkey.action.replicaof`](#valkeyactionreplicaof) | action | standalone | points an instance at a primary, or makes it one |
| [`valkey.fault.process-signal`](#valkeyfaultprocess-signal) | fault | any | kills, stops, or pauses an instance |
| [`valkey.fault.command`](#valkeyfaultcommand) | fault | any | runs a command at an event |
| [`valkey.fault.network-partition`](#valkeyfaultnetwork-partition) | fault | any (needs per-instance addresses) | drops traffic between instances |
| [`valkey.fault.disk-fill`](#valkeyfaultdisk-fill) | fault | any | fills the run's tmpfs |
| [`valkey.fault.deny-writes`](#valkeyfaultdeny-writes) | fault | any | makes data directories read-only |
| [`valkey.observer.info`](#valkeyobserverinfo) | observer | any | samples INFO and checks expectations |
| [`valkey.observer.log`](#valkeyobserverlog) | observer | any | scans instance logs |
| [`valkey.observer.log-trigger`](#valkeyobserverlog-trigger) | observer | any | turns log lines into events |
| [`valkey.observer.cluster-trace`](#valkeyobservercluster-trace) | observer | cluster | records events and epochs as JSONL |
| [`valkey.oracle.liveness`](#valkeyoracleliveness) | oracle | any | instances alive, no crash reports or forbidden log lines |
| [`valkey.oracle.ledger`](#valkeyoracleledger) | oracle | any | data matches the workload ledger |
| [`valkey.oracle.rdb-snapshot`](#valkeyoraclerdb-snapshot) | oracle | any | captured RDBs reload with plausible data, in the expected codec |
| [`valkey.oracle.repl-compression`](#valkeyoraclerepl-compression) | oracle | any | every replication link uses the predicted codec |
| [`valkey.oracle.deployment-state`](#valkeyoracledeployment-state) | oracle | any | the deployment's state validators pass, with a chaos action's kills and keys |
| [`valkey.oracle.cluster-election-safety`](#valkeyoraclecluster-election-safety) | oracle | cluster | the election model's invariants hold |
| [`valkey.oracle.binary-check`](#valkeyoraclebinary-check) | oracle | any | each instance runs its assigned binary |
| [`valkey.oracle.dataset-digest`](#valkeyoracledataset-digest) | oracle | any | records a digest of the dataset |
| [`valkey.oracle.evidence`](#valkeyoracleevidence) | oracle | any | requires or forbids evidence |

## Events, evidence, and state changes

What each component declares it publishes (besides the runner's
`<id>.started`, `.completed`, `.failed`, and for roles with a teardown
`.stopping`, `.stopped`, `.stop-failed`), the evidence it emits (one item
per run unless marked many), and the kinds of state it changes, each owned
by an undo obligation. `{config.<path>}` names come from the component's
config. The runner rejects anything else as `ERROR` (see
[Evidence contract](contracts.md#evidence-contract));
`valkey-fuzzer list components --format json` shows the payload and value
schemas.

<!-- BEGIN declarations: generated by tests/test_docs_declarations.py -->
| Component | Publishes | Emits | Changes |
| --- | --- | --- | --- |
| `valkey.action.bgsave` | `<id>.save-completed`<br>`<id>.save-started` | `valkey.bgsave.result` (many) | none |
| `valkey.action.cluster-chaos` | `<id>.instance-killed`<br>`<id>.wave-validated` | `valkey.chaos.schedule`<br>`valkey.chaos.test-keys`<br>`valkey.instances.expected-down` | none |
| `valkey.action.cluster-election` | `<id>.election-completed`<br>`<id>.election-started`<br>`<id>.injected` | `valkey.cluster.election.result`<br>`valkey.fault.injected` | `process.paused`<br>`process.stopped` |
| `valkey.action.command` | none | `valkey.command.replies` | none |
| `valkey.action.commands` | none | `valkey.command.sequence` (many) | `process.private-server` |
| `valkey.action.failover` | none | `valkey.failover.result` | none |
| `valkey.action.poll` | none | `valkey.poll.satisfied` | none |
| `valkey.action.replicaof` | none | `valkey.replication.replicaof` | none |
| `valkey.action.replication-chaos` | `<id>.instance-killed`<br>`<id>.promoted`<br>`<id>.wave-validated` | `valkey.chaos.schedule`<br>`valkey.chaos.test-keys`<br>`valkey.data.loss-window` (many)<br>`valkey.instances.expected-down` | `network.partition`<br>`process.paused`<br>`process.stopped`<br>`server.client-pause`<br>`server.config` |
| `valkey.action.restart-instance` | none | `valkey.instance.restarted` | `process.stopped` |
| `valkey.action.rolling-upgrade` | `<id>.failed-over`<br>`<id>.instance-upgraded` | `valkey.upgrade.completed` | `process.stopped` |
| `valkey.action.sleep` | none | none | none |
| `valkey.action.tools` | none | `valkey.tool.runs` | `process.private-server` |
| `valkey.action.wait-event` | none | `valkey.wait.event-observed` | none |
| `valkey.capability.build-probe` | none | `valkey.capability.supported` | `process.private-server` |
| `valkey.capability.host-probe` | none | `valkey.host.supported` | none |
| `valkey.fault.command` | `<id>.injected` | `valkey.fault.injected` | `server.command` |
| `valkey.fault.deny-writes` | `<id>.injected` | `valkey.fault.injected` | `directory.read-only` |
| `valkey.fault.disk-fill` | `<id>.injected`<br>`<id>.released` | `valkey.fault.injected` | `disk.filled` |
| `valkey.fault.network-partition` | `<id>.healed`<br>`<id>.injected`<br>`<id>.partitioned` | `valkey.fault.injected` | `network.partition` |
| `valkey.fault.process-signal` | `<id>.injected`<br>`<id>.recovered` | `valkey.fault.injected`<br>`valkey.instances.expected-down` | `process.paused`<br>`process.stopped` |
| `valkey.observer.cluster-trace` | none | `valkey.cluster.trace.recorded` | none |
| `valkey.observer.info` | none | `{config.expectations[].evidence_id}` | none |
| `valkey.observer.log` | none | `valkey.log.forbidden-match`<br>`{config.expect[].evidence_id}` | none |
| `valkey.observer.log-trigger` | `{config.rules[].publish}` | `{config.rules[].evidence_id}` | none |
| `valkey.oracle.binary-check` | none | `valkey.binary.verified` | none |
| `valkey.oracle.cluster-election-safety` | none | `valkey.cluster.election-safety.verified`<br>`valkey.cluster.vote.unsaved-at-crash` | none |
| `valkey.oracle.dataset-digest` | none | `valkey.dataset.digest` | `process.private-server` |
| `valkey.oracle.deployment-state` | none | `valkey.deployment.validated` | none |
| `valkey.oracle.evidence` | none | none | none |
| `valkey.oracle.ledger` | none | `valkey.data.ledger-verified` | none |
| `valkey.oracle.liveness` | none | `valkey.liveness.verified` | none |
| `valkey.oracle.rdb-snapshot` | none | `valkey.rdb.reload-verified` | `process.private-server` |
| `valkey.oracle.repl-compression` | none | `valkey.repl.compression-verified` | none |
| `valkey.workload.key-value` | `<id>.drained`<br>`<id>.seeded` | `valkey.workload.ledger` | none |
<!-- END declarations -->

## Capabilities

Capabilities run before the deployment starts. A required capability that
returns `SKIP` makes the run `SKIP_UNSUPPORTED`, which a probe does only for a
feature the experiment names (in the probe's config); any other startup or
configuration error is `ERROR`, so a typo never skips a run.

### `valkey.capability.build-probe`

Starts each distinct binary the run will use (the deployment's default
binary, every `instance_binaries` entry, and `binaries`) as a private server
on a Unix socket with `startup_config`, then checks its version, config
values, INFO fields, and commands. It returns `SKIP` when a binary lacks
one of them, and `ERROR` when a binary cannot be found, the probe server
fails, a binary knows a config it must lack (`lacks_configs`: it is not the
build the experiment needs), or the binaries cover fewer than
`min_distinct_builds` builds (a second build is an input the operator gives,
not a feature the host lacks). It never builds Valkey from source. With
`deployment_config` it also starts each deployment binary with the
configuration the deployment gives the instances that run it.
`deployment_config` names the settings the build may lack: when the build
rejects the configuration but starts without those settings (and the
probe's own `startup_config`), such as `rdbcompression zstd` on a build made
without `BUILD_ZSTD=yes`, the run is `SKIP_UNSUPPORTED`; any other rejected
setting makes it `ERROR`. Those starts ask whether the build accepts the
configuration and, once started, implements `commands`: a module the
deployment loads (`deployment.server_config.loadmodule`) brings its commands
only there, so with `deployment_config` the commands are checked on those
starts instead of the probe's own, and a deployment that does not load the
module is `SKIP_UNSUPPORTED` (a module that fails to load is `ERROR`). The
`config_get` values and INFO fields are checked on the probe's own
`startup_config`, never on the deployment's. A scenario
that configures a feature away (such as `bgsave-default-method fork` for a
forkless experiment) therefore runs and lacks the experiment's required path
evidence, which makes it `ERROR`; it is never hidden as unsupported.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `startup_config` | mapping | `{}` | Server config the probe server starts with; a directive the build rejects means `SKIP`. |
| `deployment_config` | list | `[]` | Settings the build may lack, such as `[rdbcompression, repl-compression]`. Non-empty: also start each binary with each distinct config the deployment gives its instances (`server_config`, `instance_configs`, and startup overlays), on top of `startup_config`; `SKIP` when the build refuses it but starts without these settings, `ERROR` when it refuses something else. |
| `scope` | `all` or `listed` | `all` | `listed` probes only `binaries` (which it then requires), not the deployment's. |
| `config_get` | mapping | `{}` | Config name to `null` (must exist) or a list of accepted values. |
| `info_fields` | mapping | `{}` | INFO section to the field names it must report. |
| `lacks_configs` | list | `[]` | Configs the build must not know (`CONFIG GET` returns nothing), such as `repl-compression` for an older build; a build that knows one is `ERROR`. |
| `config_set` | mapping | `{}` | Config name to the values `CONFIG SET` must accept at runtime. |
| `commands` | list | `[]` | Command names the build must implement; with `deployment_config` (`[loadmodule]` for a module's commands, such as `JSON.GET`), checked on the deployment config's starts. |
| `min_version` | string or null | `null` | Lowest accepted version, such as `"8.1.0"`. |
| `min_distinct_builds` | integer, 1 to 64 | 1 | Distinct (version, git SHA) builds the binaries must cover; fewer is `ERROR`. |
| `binaries` | list of strings or references | `[]` | Further binaries to probe, such as a rolling-upgrade target. |
| `timeout_s` | number, 1 to 120 | 15 | How long the probe server may take to start. |

Evidence: `valkey.capability.supported` (per-binary results), on `PASS`.

### `valkey.capability.host-probe`

Returns `SKIP` when the host lacks a requirement.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `requires` | list | | One or more of `linux`, `root`, `not_root`, `tmpfs` (Linux, root, `mount` and `umount`), `iptables` (Linux, root, a working `iptables`), and `per_instance_loopback` (the host can bind `127.0.0.2`). |

Evidence: `valkey.host.supported`.

## Workloads

### `valkey.workload.key-value`

Preloads `stable_keys`, which never change afterwards, and `mutable_keys`,
then mutates the mutable keys from one writer thread at `rate` operations per
second until teardown. For every operation it records what the client
observed in a ledger: `ok` (acknowledged, with the `instance_id` that
acknowledged it), `error` (an error reply such as `OOM` or `READONLY`, or no
reachable server, so never applied), or `ambiguous` (a timeout or connection error, which may or
may not have applied). Oracles derive the acceptable final values from that
ledger. The preload counts as acknowledged writes too: each preloaded key
records when its batch was acknowledged and by which instance, so a key
preloaded inside a loss window may be lost like any other write. In cluster
mode it writes through a cluster client that makes one
attempt per command and still follows `MOVED`, `ASK`, and `TRYAGAIN`; in
standalone mode it writes to the current primary. Keys are named
`<prefix>:stable:<n>` and `<prefix>:m:<n>`.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `stable_keys` | integer, 0 to 5,000,000 | 0 | Keys written once in the preload. |
| `mutable_keys` | integer, 0 to 1,000,000 | 100 | Keys the writer mutates. At least one of the two counts must be above 0. |
| `preload_mutable` | boolean | `true` | Also write an initial value for every mutable key in the preload. |
| `rate` | number, 0 to 100,000 | 200 | Operations per second; 0 writes the preload only. |
| `value_size` | integer, 1 to 1,048,576 | 64 | Value size in bytes. |
| `compressible` | boolean | `false` | Repetitive values instead of random ones. |
| `mix` | mapping | `{set: 0.7, del: 0.1, get: 0.2}` | Weights of `set`, `del`, `get`, `rename`, `lrange`, and `lindex`; one left out never runs. `rename` is standalone only (a cluster answers `CROSSSLOT`) and needs 2 mutable keys; `lrange` and `lindex` need `list_keys`. |
| `list_keys` | integer, 0 to 1000 | 0 | Lists preloaded beside the keys, named `<prefix>:list:<n>` and never mutated; `lrange` and `lindex` read them. |
| `list_length` | integer, 1 to 10,000,000 | 100000 | Elements per list. |
| `list_element_size` | integer, 1 to 65,536 | 120 | Element size in bytes. Elements repeat a tag, so quicklist nodes compress under `list-compress-depth`. |
| `list_read_batch` | integer, 1 to 1000 | 1 | Reads pipelined per `lrange` or `lindex` operation. |
| `prefix` | string or null | `null` | Key prefix; `vf:<id>` by default. |
| `op_timeout_s` | number, 0.05 to 60 | 2 | Client timeout per operation. |
| `preload_batch` | integer, 1 to 100,000 | 1000 | Keys per preload batch. |
| `max_ops` | integer or null | `null` | Stop after this many operations and publish `<id>.drained`. |
| `drain_on_stop` | boolean | `false` | At teardown, wait until `max_ops` operations completed; needs `max_ops`, `rate` above 0, and `mutable_keys` above 0. |
| `drain_timeout_s` | number, 0.1 to 7200 | 120 | How long `drain_on_stop` waits. |
| `wait_replicas` | integer, 0 to 100 | 0 | Standalone, at most `replicas_per_primary`: follow every acknowledged `SET` or `DEL` with `WAIT wait_replicas wait_timeout_ms`, recorded in its ledger entry as `wait` (or `wait_error`, which leaves the write acknowledged). |
| `wait_timeout_ms` | integer, 1 to 60,000 | 100 | |

The `valkey.writes` fragment sets `stable_keys`, `mutable_keys`, `rate`, and
`value_size` from its inputs (defaults 1000, 200, 200, and 64).

A `rename` operation is `RENAME src dst` between two mutable keys of one
name length whose values the workload knows for certain (their last
operation was an acknowledged `SET`, or the preload), so `dst` exists. The
ledger records it as two entries with one outcome, `id`, `t_end`, and
`reply`, both carrying `rename: [src, dst]`: a `del` of `src` and a `set` of
`dst` with the value `src` held, which is what the oracles' model needs. A
rename whose source or destination is not known for certain (its last write
was ambiguous, or it was deleted) runs as a `SET` instead. Renames count as
two operations toward `max_ops`.

`lrange` reads 30 elements from a random index of a random list, and
`lindex` one element; each operation pipelines `list_read_batch` such reads.
List reads are not ledger entries: the summary counts them (`list_reads`,
`ok` and `failed`), and `lists.jsonl` describes each list (`key`, `length`,
and the `digest` of its elements in order), which `valkey.oracle.rdb-snapshot`
checks against the reloaded RDB.

Evidence: `valkey.workload.ledger`, with the operation counts by outcome,
`ledger_path` and `preload_path` (JSONL artifacts; each preload entry holds
`t_end`, its acknowledgement time, and `instance_id`), `lists_path` (null
without lists), `list_keys`, `list_reads`, `replies_digest` (a digest of every
client-visible reply), and `drained`. Events: `<id>.seeded` after the preload,
`<id>.drained` after `max_ops` operations.

## Actions

Actions run once, in dependency order, and judge what they did: a `FAIL` from
an action is a finding about the system under test.

### `valkey.action.command`

Sends `command` to each selected instance `repeat` times and checks each reply.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `instances` | selector | `primaries` | |
| `command` | list of strings or numbers | | For example `[CONFIG, SET, maxmemory-policy, allkeys-lru]`. |
| `expect` | mapping or null | `null` | A [predicate](#predicates) on the reply, or `{error: true}`, `{error: false}`, or `{error: "<substring>"}`. `null` accepts any reply that is not an error. |
| `repeat` | integer, 1 to 100,000 | 1 | |
| `interval_s` | number, 0 to 3600 | 0 | Pause between repeats. |
| `timeout_s` | number, 0.1 to 3600 | 10 | Client timeout, and how long to keep trying to open a connection. |

Each command is sent once. A connection that could not be opened sent
nothing (under load the host can run out of local ports), so opening it is
retried for up to `timeout_s`; only then is its error recorded, with
`unanswered: true`, as is a connection lost or a reply that timed out.
`FAIL` when any reply does not match; `ERROR` when every reply that came back
matched but some command got none. Evidence: `valkey.command.replies`, one
record per reply.

### `valkey.action.commands`

Sends a command sequence on one connection, so `MULTI`/`EXEC`, `WATCH`,
`SELECT`, and client state span the commands (a command may name another
connection, for a subscriber or a second client), and with `compare_with`
compares every reply with the same sequence on another build; with `modules`
and `modules_parent`, on another build of a module.

```yaml
- use: valkey.action.commands
  id: lset
  config:
    compare_with: {$ref: artifacts.parent.binary}   # the parent commit's build
    server_config: {list-max-listpack-size: 4}      # both servers start with it
    resp: 2
    commands:
      - {command: [RPUSH, l, a, b, c], expect: {equals: 3}}
      - {command: [LSET, l, 9223372036854775807, x], expect_difference: true,
         expect: {error: "index out of range"}}
      - {command: [LRANGE, l, 0, -1], expect_difference: true, expect: {equals: [a, b, c]}}
      - {command: [HGETALL, h], compare: unordered-pairs}
      - {command: [RESTORE, k, 0, !!binary "AA4BAgMEBQ=="], expect: {equals: OK}}
```

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `compare_with` | binary path or null | `null` | A build to compare with, usually `{$ref: artifacts.parent.binary}`. With it the sequence runs on two private servers the action starts, one of the deployment's binary (the candidate) and one of this (the parent), both from an empty dataset, each from a config file (`valkey.conf`) in its own directory, which is also its working directory, so `CONFIG REWRITE` works; the deployment's instances are never touched. |
| `modules` | list of paths or references | `[]` | Modules both private servers load (`--loadmodule` each, a bare path: see [`valkey.artifact.local-module`](#valkeyartifactlocal-module)), usually `[{$ref: artifacts.module.path}]`; recorded in the evidence. Needs `compare_with` or `modules_parent`: the deployment's instances load modules through `deployment.server_config.loadmodule`, which `server_config` here refuses. |
| `modules_parent` | list of paths or references, or null | `null` | The parent server loads these instead of `modules`, usually `[{$ref: artifacts.module_parent.path}]`. Without `compare_with`, both servers run the deployment's binary, so the sequence compares two builds of a module on one Valkey build; with it (a core change to the module API), each Valkey build loads its own commit's modules. Needs `modules`. |
| `instances` | selector | `primaries` | Without `compare_with`: the instances the sequence runs on, one connection each. |
| `cluster` | boolean | `false` | With `compare_with`: every private server runs in cluster mode (`cluster-enabled yes`); node 0 serves every slot (`CLUSTER ADDSLOTSRANGE 0 16383`), every other node meets it (`CLUSTER MEET`) as an empty primary, and each node's `cluster_state:ok`, knowing every node, is awaited within `timeout_s`, so a sequence can test `CLUSTER` subcommands, slot-keyed replies, and, with `nodes`, slot migration and replication it sets up itself. Without `compare_with` it is a config error: the deployment decides the mode. |
| `nodes` | integer, 1 to 6 | 1 | With `compare_with`: each side runs on this many private servers, its nodes, and a command names its node with `node`. Without `cluster` they are standalone primaries; the sequence sends `REPLICAOF` itself. |
| `server_config` | mapping | `{}` | Server options both private servers start with, as the deployment's `server_config`; the options the deployment manages (`dir`, `port`, `cluster-enabled`, ...) are refused. `sanitize-dump-payload yes` and `enable-debug-command yes` unless set. An option the parent rejects is dropped on both sides and recorded as `dropped_config`; test an option the change adds with `CONFIG SET` inside the sequence, marked `expect_difference`. |
| `files` | mapping | `{}` | With `compare_with`: files written into every private server's directory before it starts, by plain relative path (no `..`, not the harness's own `valkey.conf`, `server.log`, `output.log`, `s.sock`): a string as UTF-8, or `!!binary`. `server_config` may name them, as `{aclfile: users.acl}`; the default user must stay usable without a password, since the harness connects as it. |
| `resp` | 2 or 3 | 2 | The protocol of every connection. |
| `timeout_s` | number, 0.1 to 3600 | 10 | Per command: a reply that takes longer is `unanswered`, and the sequence stops there. |
| `commands` | list of mappings | | The sequence, below. |
| `checks` | mapping or null | `null` | With `compare_with` (or `modules_parent`): `{variants: bool, replica: bool, acl: bool, concurrency: 0 to 16, compat: bool}`, each off unless set, below. `compat` needs `compare_with`. |

Each command has:

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `command` | list of strings, numbers, or `!!binary` values | | A `!!binary` argument is sent as bytes (it loads as `{$base64: ...}`). A string argument may hold placeholders, below. |
| `raw` | string or `!!binary` value | | Instead of `command`: bytes sent as they are, a string as UTF-8 (placeholders replaced), for the inline protocol and malformed requests: `raw: "SET k v\0 EX 100\r\n"`. One reply is read; when none begins within a second (the bytes are not a whole request, or the server holds them), the command is recorded as `{no_reply: true}`: an `expect` sees a null reply, and it compares unequal to any reply. A further reply stays on the connection for the next command (or a `read`), and bytes the server left unanswered stay in its buffer, so send raw bytes on a `connection` of their own. |
| `restart` | boolean | `false` | Instead of `command`, with `compare_with`: stop the node gracefully (`SIGTERM`, as `SHUTDOWN` does, which a sequence may not send) and start it again from its directory and config file, so it reads back what the sequence rewrote there: `CONFIG REWRITE`, `ACL SAVE`, a `SAVE`d RDB, `nodes.conf`. Recorded as the reply `OK` once the node answers `PING` again; its connections open anew on their next use. A node that does not stop within 30 s, or does not answer again, is a command that got no reply; one that exits at startup has exited. |
| `read` | boolean | `false` | Instead of `command`: read the replies waiting on the connection, and each that begins within a second of the last, as one list (an error among them as `{$error: ...}`); for Pub/Sub messages and RESP3 pushes such as client-side caching invalidations. An empty list when none came. |
| `node` | integer | 0 | The node the command goes to, by index; less than `nodes`. |
| `connection` | string | `main` | The connection to the node it goes on. Every other name opens a connection of its own on first use, which keeps its state (a `SUBSCRIBE`, a `CLIENT TRACKING`, bytes `raw` left unanswered) apart from the main one. `SUBSCRIBE`, `PSUBSCRIBE`, and `SSUBSCRIBE` go on a named connection, one channel each, and so do their unsubscribing commands (one channel each, on any connection): every channel is confirmed by a reply of its own. |
| `wait_s` | number, 0.1 to 3600, or null | `null` | Send the command again every 0.1 s until its reply meets `expect`, for this long at most: for asynchronous work the sequence started, such as `[GET, k]` on the target node until a slot migration (`CLUSTER MIGRATESLOTS`) finished, or `[INFO, replication]` until `master_link_status:up`. Its last reply is recorded, judged, and compared like any other, so a candidate that never meets `expect` violates it. Needs `expect`. |
| `expect` | mapping or null | `null` | A [predicate](#predicates) on the reply (`equals` also takes a list or a mapping, compared as JSON data, or null, a null reply), or `{error: true}`, `{error: false}`, or `{error: "<substring>"}`. `null` accepts any reply that is not an error. |
| `expect_difference` | boolean | `false` | The change under test means this reply to differ from the parent's: the command the change is in, or a later read it carries into. Needs `expect` and `compare_with`. |
| `compare` | `equal`, `unordered`, `unordered-pairs`, `json`, `skip` | `equal` | How the two builds' replies compare: as sent; a flat reply with its items sorted; a flat field-value reply with its pairs sorted (values swapped between fields still differ; a RESP3 map compares as a map); the JSON value a bulk string (or each item of an array) holds, for valkey-json's `JSON.GET` and `JSON.MGET` (key order, spacing, and `1.0` against `1` do not count; `true` is not `1`; a reply that is not JSON compares as itself; an `expect: {equals: ...}` may give the JSON text or the value it holds, so `{equals: [1]}` matches `JSON.GET`'s `"[1]"`); or recorded and never compared (`TIME`, `TTL`, `INFO`, `CLIENT ID`, `SCAN` cursors, `OBJECT ENCODING`, `MEMORY USAGE`, `DEBUG OBJECT`). Avoid commands that also change state at random (`SPOP`, `SRANDMEMBER`, `HRANDFIELD`, `RANDOMKEY`) in a compared sequence: Valkey seeds its hash function per process. |

With `compare_with`, a string argument may name a node of the side it runs
on: `<node.1.id>` (its cluster node ID, with `cluster`), `<node.1.host>`,
`<node.1.port>`, and `<node.1.cluster-port>` (with `cluster`) are replaced by
each side's own values, for example `[CLUSTER, SETSLOT, 7, NODE, <node.1.id>]`
or `[REPLICAOF, <node.0.host>, <node.0.port>]`. Angle brackets, not braces:
YAML reads `{node.1.id}` as a mapping, and Valkey braces as a hash tag. Every
node listens on its own loopback port (and cluster bus port), so the nodes can
meet and replicate; the sequence itself talks to each over its unix socket.
Before the two sides are compared, each side's node IDs and ports in its replies
and errors (a port also as an integer, as `CLUSTER SLOTS` replies it) are
replaced back by their placeholders, so `CLUSTER NODES` and `MOVED` errors
compare equal when the builds behave the same, and an `expect` names them the
same way (`{contains: "<node.1.id>"}`); the evidence records the replies so
named. Other random values (`CLUSTER SHARDS` shard IDs, replication IDs) still
differ: compare those with `skip`.

Replies are recorded as the server sent them, over a raw connection without
valkey-py's response callbacks: `SET` replies `OK`, not `true`; `HGETALL` a
flat list on RESP2 and a map on RESP3; a bulk string that is valid UTF-8 as a
string, any other as `{$base64: ...}`, so two different binary replies never
compare equal; an error inside an array (a transaction's) as `{$error: ...}`.
No timestamps, so two runs' replies compare equal. A connection a private
server closes without a reply (`QUIT`, `CLIENT KILL`, a slot migration
client's `RESET`) is recorded as `{closed: true}` when its node still runs 5 s
later: an `expect` sees a null reply (`{exists: false}` expects it), it
compares unequal to any reply, and the sequence goes on, the connection opening
anew on its next use. When the node exited, the connection was lost to a
crash; on a deployment instance, a closed connection is a command without a
reply. A reply the client cannot parse (a RESP3 double `,abc`) is recorded as
`{protocol_error, raw}`, the client's message and the first 4 KB of the bytes
the command gets again on a fresh connection (same RESP and `SELECT`),
escaped; it is a `FAIL` with `diagnostics: {reason: protocol-error, commands:
[...]}`, even where the parent gives the same, and without `compare_with`.

The outcome, with `compare_with`:

- `FAIL`: a candidate reply that violates its `expect` where the parent
  replied otherwise, or a node of the candidate exiting (a crash, a sanitizer abort);
  its cause is `crashes` in the diagnostics; or a failing check (below).
  Nothing else: a difference between builds is never, by itself, a Valkey
  failure.
- `ERROR` with `diagnostics: {reason: expect-disputed, commands: [...]}`: an
  `expect` the candidate violates but the parent replied the same to. The
  expectation is about behaviour the change did not touch, nearly always the
  test's own mistake; each command is given with its `expect` and both
  replies.
- `ERROR` with `diagnostics: {reason: unreached-change}`: a marked command
  whose parent reply equals the candidate's, the parent alive. The sequence
  did not reach the change; for a bug fix, the parent is a build with the
  bug, so a test of the fix must see it there.
- `ERROR` with `diagnostics: {reason: unexplained-difference, commands:
  [...]}`: an unmarked command whose replies differ, both replies given.
  Usually a write the change affects carried into a later read that was not
  marked; sometimes a change the pull request did not intend. Not a `FAIL`:
  the fuzzer did not see Valkey do something wrong.
- A parent that dies on a marked command, the candidate replying as
  expected, counts as reached: the bug was a crash. A parent that dies on an
  unmarked command is an unexplained difference.
- `ERROR` for a private server that does not start, or a command with no
  reply on the candidate (a connection lost without a crash, a timeout).

`checks` run the sequence again on fresh candidate servers once the compared
run completed on the candidate. A check that sees Valkey do something wrong
is `FAIL` with `diagnostics.reason` and what reproduces it; a candidate that
exits during one is a crash. `variants`: compact encodings off
(`*-max-listpack-entries 0`, `set-max-intset-entries 0`,
`list-max-listpack-size 0`; not with `modules`), then the other `resp`;
replies compare as multisets of their values shaped as RESP2's, except
`compare: skip`, `raw`, `read`, `DEBUG`, `OBJECT`, `MEMORY`, `CONFIG`,
`CLIENT INFO`/`LIST`, `HELLO`, and `INFO` (`variant-mismatch`).
`replica`: node 0 gets a synced replica, whose offset and `DEBUG DIGEST` must
reach the primary's within 30 s of the sequence (`replica-diverged`). `acl`:
as the default user with `+@all -@<category>` (from `COMMAND INFO`, the one
most commands but not all are in, never `@fast` or `@slow`), then with `+@all
resetkeys ~nomatch-*`, a refused command (in the category; with keys, from
`COMMAND GETKEYS`) must reply NOPERM or an error Valkey replies before it
checks permissions (`acl-bypass`), and no other NOPERM (`acl-overreach`);
scripts are not judged, and a sequence sending `ACL`, `AUTH`, or `HELLO ...
AUTH` is skipped. `concurrency`: N connections run it at once, replies
unjudged; a command unanswered while the server answers no `PING` within
`timeout_s` is `hang` (a crash), and with `replica` the replica must catch
up. `compat`: a fresh parent runs it and `SAVE`s; its RDB (kept as
`parent.compat.rdb`) must load into a fresh candidate with the parent's
digest, compared after a reload as a replica, which keeps keys expired since
the save (`compat-load`), and a fresh candidate replicating the parent must
reach it (`compat-sync`). `acl` and `concurrency` leave restarts out and do
not wait (`wait_s`); `replica` and `compat` skip several nodes and `cluster`,
`compat` also a module loaded on one build only. A check that could not run
is recorded as `error` and leaves the verdict alone.

Without `compare_with`, `expect` alone judges the sequence: `FAIL` on a
violated reply, `ERROR` on a command with no reply.

Refused when the scenario compiles, naming the command: `SHUTDOWN`; `DEBUG
RESTART`, `DEBUG CRASH-AND-RECOVER`, `DEBUG RELOAD`, and `DEBUG SLEEP` longer
than `timeout_s`; `MODULE LOAD`, `LOADEX`, `UNLOAD`; `CONFIG SET` of an option
the deployment manages; `MONITOR`; `SUBSCRIBE`, `PSUBSCRIBE`, and
`SSUBSCRIBE` on the main connection, and those and their unsubscribing
commands with other than one channel.
The private servers are disposable, so `DEBUG SEGFAULT` and friends are
allowed and simply crash the server they are sent to (a candidate crash is a
`FAIL`, which is what such a test means); `REPLICAOF`, `CLUSTER`, `SYNC`,
`PSYNC`, and `REPLCONF` are allowed on them too, and refused on the
deployment's instances.

Evidence: `valkey.command.sequence`, `{candidate: {replies: [{index, reply |
error | no_reply | closed | protocol_error and raw, unanswered?}], digest, exited?, exited_node?}, parent: {...} | null, modules:
{candidate: [paths], parent: [paths]} | null, cluster, nodes, dropped_config,
reached: [indexes], unexplained: [indexes], checks?}` (and
`instance_id` without `compare_with`, one item per instance). `checks` has
each enabled check's outcome by name: `{outcome: pass | skipped | error |
fail}`, with `reason` (skipped, error) or `summary` and `diagnostics` (fail),
and for a check of two parts each part's (`encodings` and `protocol`,
`category` and `keys`, `load` and `sync`). Every check server's logs are kept
beside the others (`candidate.encodings.server.log`). `digest` is the
SHA-256 of the canonical JSON of the compared replies. `exited` is the exit
status of a side's node that exited, and `exited_node` that node; any node of
the candidate that exits is a `FAIL`. Every private server's
`server.log` and `output.log` are copied into the run's component folder
before they are stopped (`candidate.server.log`, or `candidate.node1.server.log`
with several nodes), so a crash or sanitizer report survives, and are the
evidence's artifacts.

### `valkey.action.tools`

Runs Valkey's own tools (`valkey-cli`, `valkey-benchmark`,
`valkey-check-rdb`, `valkey-check-aof`) of two builds with the same
arguments, stdin, and files, and compares their exit status, stdout, and
stderr: a test of a change to a tool. Each build's tools are the ones beside
its `valkey-server`: the deployment's binary (the candidate) and
`compare_with` (the parent). The deployment's instances are never touched.

```yaml
- use: valkey.action.tools
  id: check-rdb
  config:
    compare_with: {$ref: artifacts.parent.binary}   # the parent commit's build
    files:
      empty-db2.rdb: !!binary UkVESVMwMDA5/gL/AAAAAAAAAAA=   # SELECTDB 2, then EOF
    ignore: ['^\d+:[A-Z] .*\n']                        # Valkey log lines: pid and time
    runs:
      - {tool: valkey-check-rdb, args: [empty-db2.rdb, --stats, --format, info],
         expect_difference: true, expect: {exit: {equals: 0}, stdout: {contains: "db.2.type.string.keys.total:0"}}}
- use: valkey.action.tools
  id: cli
  config:
    compare_with: {$ref: artifacts.parent.binary}
    server: true                                      # each side's own private server
    runs:
      - {tool: valkey-cli, args: [-p, $port, SET, k, v], expect: {stdout: {equals: "OK\n"}}}
      - {tool: valkey-cli, args: [-p, $port, PING], env: {VALKEYCLI_AUTH: secret}}
```

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `compare_with` | binary path | | The parent's `valkey-server`, usually `{$ref: artifacts.parent.binary}`; its tools sit beside it. A build without a tool a run names is `ERROR`. |
| `server` | boolean | `false` | Each side starts a private server of its own build, from an empty dataset, on a free loopback TCP port (`127.0.0.1`, and `::1` where the host has it) and a unix socket, with `enable-debug-command yes` and `sanitize-dump-payload yes`, and the side's working directory as its `dir`, so a `SAVE` writes `dump.rdb` where `valkey-check-rdb` reads it, and a `dump.rdb` in `files` is what the server loads. A run's arguments name it as `$port` and `$socket`. |
| `cluster` | boolean | `false` | Both private servers run as a one-node cluster serving every slot, for `valkey-cli --cluster`. Needs `server`. |
| `files` | mapping | `{}` | `{name: text or !!binary}`, written into each side's working directory before the first run. A name is a plain file name. |
| `ignore` | list of regular expressions | `[]` | Removed from stdout and stderr (`^` and `$` match at line ends) before the two sides compare, for what differs between processes: the pid and time of a Valkey log line, a benchmark's throughput. Recorded output keeps them. |
| `timeout_s` | number, 0.1 to 3600 | 30 | Per run: a tool still running then is killed and recorded `timed_out`; also how long a private server may take to start. |
| `runs` | list of mappings | | The runs, below, in order; each side runs them all. |

Each run has:

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `tool` | `valkey-cli`, `valkey-benchmark`, `valkey-check-rdb`, `valkey-check-aof` | | Run with its bare name as `argv[0]`, never through a shell, in the side's working directory. |
| `args` | list of strings or numbers | `[]` | `$port` and `$socket` (also inside an argument, as in `[::1]:$port`) become the side's own; they need `server`. An argument that could name a path outside the working directory (it starts with `/` or `~`, or has a `..` part) is refused. |
| `stdin` | text, `!!binary`, or null | `null` | What the tool reads; null is an empty stdin. |
| `env` | mapping | `{}` | Environment variables for this run. The fuzzer's own environment is passed on without `VALKEYCLI_*` and `REDISCLI_*`, with `HOME` the working directory. |
| `expect` | mapping or null | `null` | `{exit, stdout, stderr}`, each optional, each a [predicate](#predicates): `exit` on the exit status (null when a signal ended the tool), `stdout` and `stderr` on the output as recorded. `null` accepts any run. |
| `expect_difference` | boolean | `false` | The change under test means this run to differ from the parent's. Needs `expect`. |
| `compare` | `equal`, `unordered`, `skip` | `equal` | How the two sides' runs compare: exit status, signal, `timed_out`, stdout, and stderr as recorded (after `ignore`); the same with stdout's and stderr's lines sorted; or recorded and never compared. |

Output is recorded with the side's port, socket path, and working directory
written back as `$port`, `$socket`, and `$dir`, so the two sides compare
equal; output that is not valid UTF-8 is recorded as `{$base64: ...}`. With
`cluster`, its private server's node ID, in full or as the `1a2b3c4d...`
valkey-cli abbreviates it to, is written back as `<node.0.id>`, as
[`valkey.action.commands`](#valkeyactioncommands) names it.

The outcome, as for [`valkey.action.commands`](#valkeyactioncommands):

- `FAIL`: a candidate tool that crashes (a signal ends it, or its output
  has a crash or sanitizer report) or a candidate private server that exits;
  their causes are `crashes` in the diagnostics. Also a candidate run that
  violates its `expect` where the parent ran otherwise.
- `ERROR` with `diagnostics: {reason: expect-disputed, runs: [...]}`: an
  `expect` the candidate violates but the parent ran the same.
- `ERROR` with `diagnostics: {reason: unreached-change}`: a marked run the
  same on both builds.
- `ERROR` with `diagnostics: {reason: unexplained-difference, runs: [...]}`:
  an unmarked run that differs, both sides given.
- A parent tool that crashes or times out where the candidate does not
  differs: reached on a marked run, unexplained on an unmarked one.
- `ERROR` for a candidate run that does not exit within `timeout_s`, a
  missing tool, or a private server that does not start.

Hints and other interactive features need a terminal; `valkey-cli
--test_hint` and `--test_hint_file` print a hint without one.

Evidence: `valkey.tool.runs`, `{candidate: {runs: [{index, tool, exit,
signal?, timed_out?, stdout, stderr}], digest, server_exited?}, parent:
{...}, server, reached: [indexes], unexplained: [indexes]}`. `digest` is the
SHA-256 of the canonical JSON of the compared runs. Each side's working
directory, with its files and what the tools wrote, stays in the run's
component folder as `candidate/` and `parent/`; with `server`, both private
servers' `server.log` and `output.log` are the evidence's artifacts.

### `valkey.action.bgsave`

Issues `BGSAVE` on the selected instances, follows every save until it
finishes or `timeout_s` passes, and compares each result with `expect`. A
save's status is `ok` (the RDB file was replaced), `err` (Valkey reported
`rdb_last_bgsave_status:err`), `not_completed` (it finished without replacing
the file, as a cancelled save does), `timeout`, `rejected` (Valkey replied
to `BGSAVE` with an error), `unanswered` (`BGSAVE` got no reply: a lost
connection or a client timeout), `unavailable` (`INFO persistence` could
not be read until `timeout_s`; a read that fails is tried again on the next
poll), or `busy`. While another child process runs (a replication RDB, an
AOF rewrite), Valkey refuses `BGSAVE` with `Background save already in
progress`; the action sends it again until `timeout_s`, and a save still
refused that way then is `busy`, not a rejection. `FAIL` when a save Valkey
reported does not reach `expect`; `ERROR` when, with an `expect` other than
`any`, a save is `unanswered`, `unavailable`, or `busy`: the harness did not
see what Valkey did, or never got a save started.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `instances` | selector | `primaries` | |
| `args` | list of strings | `[]` | Extra `BGSAVE` arguments. |
| `timeout_s` | number, 0.5 to 7200 | 120 | |
| `poll_interval_s` | number, 0.001 to 10 | 0.02 | |
| `expect` | `ok`, `err`, `not_completed`, or `any` | `ok` | The status every save must reach. |
| `expect_type` | `fork`, `forkless`, or null | `null` | With `expect: ok`, the `rdb_last_bgsave_type` every save must report. |
| `capture_rdb` | boolean | `true` | Copy each `ok` save's RDB into the component's artifact folder, for `valkey.oracle.rdb-snapshot`. |

Evidence: `valkey.bgsave.result` per instance (status, observed save types,
persistence fields before and after, the captured RDB, the instance's `role`
(`primary` or `replica`),
and `sent_at`, `replied_at`, and `finished_at`: when `BGSAVE` was sent, when
its reply arrived, and when the save was seen finished). An `ok` save also
records `rdb_format`, read from the first bytes of the RDB: `envelope` (`VCS`
for a streaming-compressed RDB, `plain`, `unknown`, or `unreadable`), `codec`
(`lz4` or `zstd` for `VCS`; `none` for `plain`, including LZF, which
compresses strings inside a plain RDB), `size`, and `rdb_version` (plain) or
`vcs_version` and `stream_kind` (`VCS`). Events:
`<id>.save-started` once the commands were sent (faults that must hit a save
in progress trigger on it), `<id>.save-completed` once every save finished or
`timeout_s` passed.

### `valkey.action.failover`

Cluster mode. Promotes a replica of one shard's current primary with
`CLUSTER FAILOVER`, or `CLUSTER FAILOVER FORCE`, and waits for the cluster to
settle. The replica is the first, in an order drawn from the component's
seeded random stream, that has synced with the primary (link up, no sync in
progress, offset past 0); the failover waits up to `timeout_s` for one,
then gives Valkey another `timeout_s` from the command to settle the shard.
A primary the cluster flagged failed (`fail`, not one node's `fail?`), or one
that stops answering before a replica synced, is left to Valkey's own
failover, which must settle within `timeout_s`. It is `ERROR` when the
failover could not be observed: no node answered, the shard has no replica
or none synced in time, a command failed, or the shard could not be read at
the end of the wait.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `shard` | integer or null | `null` | The shard to fail over, below `primaries`; shard 0 when neither field is set. |
| `target` | selector or null | `null` | An instance whose planned shard to fail over. Set `shard` or `target`, not both. |
| `force` | boolean | `false` | Use `CLUSTER FAILOVER FORCE`. |
| `timeout_s` | number, 1 to 600 | 60 | |

Evidence: `valkey.failover.result` (target, force, success, roles after,
`primary_before`, `started_at`, and `completed_at`). With `force`, the
failover is a loss window for its old primary (see
[`valkey.oracle.ledger`](#valkeyoracleledger)).

### `valkey.action.restart-instance`

Stops one instance with `SIGTERM` if it is running and starts it again with
the same address, binary, data directory, and config. An instance whose
replica was promoted restarts in its current role. Until the instance exits
it gets `SIGTERM` again every second (Valkey drops one it cannot act on yet);
one still running after 30 seconds is restarted anyway, and the action
fails.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `instance` | selector | | The first selected instance restarts. |
| `wait_ready` | boolean | `true` | Wait until the instance is ready for its role. |

`FAIL` when the instance does not exit; `ERROR` when it does not come back
(`valkey.oracle.liveness` judges a server that crashed) or the selector
matches no live instance. Evidence: `valkey.instance.restarted`
(instance ID and new PID).

### `valkey.action.sleep`

Waits, for example so a workload runs for a while before a fault triggers on
`<id>.completed`. A deadline or cancellation cuts the wait short.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `seconds` | number, 0 to 3600 | | |

### `valkey.action.poll`

Polls an INFO field or a command reply on every selected instance until the
predicate holds on all of them.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `instances` | selector | `all` | |
| `section`, `field` | string or null | `null` | The INFO section and field to read. |
| `command` | list or null | `null` | A command whose reply to read. Set `section` and `field`, or `command`. |
| `predicate` | mapping | | A [predicate](#predicates). |
| `timeout_s` | number, 0.1 to 7200 | 60 | |
| `poll_interval_s` | number, 0.001 to 60 | 0.1 | |

`FAIL` on timeout when an instance was read and does not satisfy the
predicate; `ERROR` when every instance that does not was unreadable at its
last read, since nothing was seen. Evidence: `valkey.poll.satisfied` (the
last values).

### `valkey.action.wait-event`

Waits until an event has been dispatched `min_count` times.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `event` | string | | For example `writes.drained`. |
| `min_count` | integer, 1 to 1,000,000 | 1 | |
| `timeout_s` | number, 0.01 to 7200 | 120 | |
| `poll_interval_s` | number, 0.001 to 10 | 0.02 | |
| `only_after` | string or null | `null` | Wait only if this event was already dispatched; otherwise pass at once. |

`ERROR` on timeout: an event that did not come in time says nothing about the
server. Evidence: `valkey.wait.event-observed` (event and count).

### `valkey.action.rolling-upgrade`

Moves every instance to `to_binary`, one shard at a time: each replica
restarts on the new build and resynchronizes, the shard fails over gracefully
to the first upgraded replica (`CLUSTER FAILOVER` in cluster mode; in
standalone mode `FAILOVER TO` with `step_timeout_s` as its timeout, aborted
with `FAILOVER ABORT` if it does not complete, after which the other replicas
follow the new primary), and the former primary restarts on the new build. Without replicas,
or with `failover: false`, the primary restarts in place, with downtime. In
standalone mode it needs `replicas_per_primary` of at least 1. Instances
already on the target are skipped. `FAIL` when Valkey is seen doing something
wrong (an instance that does not exit or does not resynchronize, or a
failover that does not complete, each while it answers); `ERROR` when the
upgrade cannot run or the harness cannot tell what Valkey did (a shard
without one live primary, an instance that restarts on another version than
the harness put in place, a client error, a `FAILOVER TO` that got no reply
and roles that did not swap, roles that could not be read at the end of a
wait, a cancelled run).

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `to_binary` | string or reference | | Usually `{$ref: inputs.to_binary}` fed by an artifact. |
| `failover` | boolean | `true` | Fail over to an upgraded replica before restarting a primary. |
| `settle_s` | number, 0 to 600 | 1 | Pause after each instance. |
| `step_timeout_s` | number, 1 to 3600 | 60 | Limit for each exit, resync, and failover. Until an instance exits it gets `SIGTERM` again every second: Valkey drops one it cannot act on yet, such as while it writes its initial AOF. |

Evidence: `valkey.upgrade.completed` (target version and every step). Events:
`<id>.instance-upgraded` per instance, `<id>.failed-over` per failover.

### `valkey.action.cluster-chaos`

Cluster mode, at least one replica per primary. Runs `waves` of `failover`
and `kill` operations on the [wave engine](architecture.md#the-wave-engine)
(operations, timing, evidence, and verdicts as for
[`valkey.action.replication-chaos`](#valkeyactionreplication-chaos)); Valkey
heals the cluster, and the action validates it after every wave with the
killed instances counted as stopped on purpose. How targets are chosen and
when a failed failover is tolerated is in
[Kill safety and verdicts](cluster-chaos-and-ci.md#kill-safety-and-verdicts).

| Operation | Fields (defaults) | What it does |
| --- | --- | --- |
| `failover` | `shard` (required, below `primaries`), `force` (`false`), `timeout_s` (30) | `CLUSTER FAILOVER` (or `FORCE`) on a replica of the shard's current primary that has synced with it (as for [`valkey.action.failover`](#valkeyactionfailover)), never one killed on purpose; done once the shard has a new primary that its live replicas follow. At most one per shard per wave. |
| `kill` | `target` (`replica`; `primary`, `any`, an instance ID, or a list of IDs), `signal` (`SIGKILL`) | As in replication chaos. Kills are never restarted. |

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `waves` | list of lists | | The waves. The experiment's default is one failover of shard 0. |
| `validation` | mapping | `{}` | The [state validation](cluster-chaos-and-ci.md#state-validation) settings. A critical failure, or any with `blocking_on_failure`, stops the waves. |
| `keys_per_slot` | integer, 0 to 100 | 5 | Keys written into every hash slot before the first wave. |

### `valkey.action.cluster-election`

Cluster mode. Forces one failover election in a shard and waits for a stable
winner.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `shard` | integer | 0 | The shard, below `primaries`. |
| `trigger` | `force_failover`, `kill_primary`, or `pause_primary` | `force_failover` | `force_failover` sends `CLUSTER FAILOVER FORCE` to every candidate at once, so they compete in one epoch and the old primary votes; `kill_primary` and `pause_primary` first wait, up to `timeout_s`, until every candidate replicates the primary with its link up (Valkey's failure detection elects only a replica whose data is recent, which one restarted moments ago and still in its first sync is not; `ERROR` if they do not link), then send `SIGKILL` or `SIGSTOP` to the primary and let failure detection start the election. |
| `candidates` | `all` or `one` | `all` | Replicas that compete. |
| `attempts` | integer, 1 to 20 | 3 | `force_failover` attempts. |
| `timeout_s` | number, 1 to 600 | 30 | |
| `settle_s` | number, 0 to 60 | 1 | How long the winner must stay the shard's only primary. |
| `pause_s` | number, 0.1 to 600 | 10 | Longest `SIGSTOP` for `pause_primary`; it ends once a winner is stable, which must happen within it. An election that loses a vote retries 4 s later, so `valkey.cluster-election-durability`, whose vote killer takes votes, sets 30. |
| `restart_killed` | boolean | `true` | Restart the primary `kill_primary` killed. |
| `wait_synced` | boolean | `true` | Wait until the shard's other instances replicate from the winner. |

Before the election it reads the shard until exactly one primary owns its
slots, up to `timeout_s`: an old primary restarted from its `nodes.conf`
claims its old slots until gossip corrects it. `FAIL` when no replica wins
after every running candidate answered; `ERROR` when the shard still has
another number of slot owners then (no election started, so the oracles
judge the shard), a node did not answer before the election or at its end,
the shard has no live replica, or the trigger never took effect (no
candidate accepted `CLUSTER FAILOVER FORCE`, or the killed primary did not
exit). Evidence: `valkey.cluster.election.result`, and for
`kill_primary` `valkey.fault.injected` (the kill, with the `nodes.conf` vars
read once the process exited, as the restart loads them), so `valkey.oracle.cluster-election-safety` can list the
action in `crash_faults`. Events: `<id>.election-started`,
`<id>.election-completed` (with the winner and its `configEpoch`), and for
`kill_primary` `<id>.injected`, which `valkey-fuzzer spec conform` reads as a
crash.

### `valkey.action.replication-chaos`

Standalone, at least one replica. Runs `waves` on the
[wave engine](architecture.md#the-wave-engine); each wave is a list of
operations that start together, each after its own `delay_s` (0 to 600,
default 0), or, with `after: <i>`, once operation `i` of the wave (an
earlier one) finished and then its `delay_s`. Targets are `primary`,
`replica`, `any`, or an instance ID, and resolve against the roles at the
start of the wave; each operation draws from its own random stream,
`wave-<n>-op-<i>`.

| Operation | Fields (defaults) | What it does |
| --- | --- | --- |
| `kill` | `target` (`replica`, or a list of instance IDs: one of them), `signal` (`SIGKILL` or `SIGTERM`; `SIGKILL`) | Signals one instance and waits up to 30 seconds for it to exit, sending the signal again every second (Valkey drops a `SIGTERM` it cannot act on yet); one that never exits fails the wave after `SIGTERM`, and is an `ERROR` after `SIGKILL`, which Valkey cannot refuse. |
| `restart` | `target` (`killed`, or an instance ID) | Restarts an instance killed earlier in the same wave as a replica of the current primary. Needs a `kill` of the wave that surely starts before it: one it waits for (`after`, directly or through others), or one without `after` whose `delay_s` is below the restart's earliest start; skipped while the primary is down. |
| `promote` | `target` (`replica`), `mode` (`graceful` or `force`; `graceful`), `split_s` (0 to 60; 0), `timeout_s` (10) | `graceful`: `FAILOVER TO` on the primary, then the other replicas follow the new primary; it succeeded when the target leads, or when the old primary follows the target with its failover state cleared (a target killed right after taking over was still promoted). A `FAILOVER TO` that got no reply, with roles that did not swap, or roles that could not be read at the end, is an `ERROR`: Valkey may never have seen it. `force`: `REPLICAOF NO ONE` on the replica; the old primary keeps serving for `split_s`, then it and every other replica follow the new primary. Refused when the target is killed, stalled, partitioned, or (for `graceful`) not linked, and for `force` when it was restarted in the wave and has not resynced: it holds only what it loaded from disk, so writes no loss window covers (from earlier terms, or WAIT-confirmed) could be lost. |
| `repoint` | `target` (`replica`), `to` (`replica`, `primary`, or an ID; `replica`) | `REPLICAOF` from one replica to another (a chain) or to the primary. `to: replica` needs two replicas. |
| `partition` | `target` (`replica`), `duration_s` (1) | Drops traffic between the instance and every other instance (iptables), then heals. Needs `addressing: per_instance_loopback`; on a host without Linux, root, and iptables the action is `ERROR`. |
| `stall` | `target` (`replica`), `duration_s` (1) | `SIGSTOP`, then `SIGCONT`. |
| `backlog_pressure` | `repl_backlog_size` (16384), `output_limit` (65536 bytes), `duration_s` (1) | On the current primary: shrinks `repl-backlog-size` and the replica class of `client-output-buffer-limit`, then restores both. |
| `compression` | `target` (`primary`, `replica`, `any`, `all`, or an ID; `primary`), `value` (`no`, `yes`, `lz4`, `zstd`; `lz4`), `duration_s` (1) | `CONFIG SET repl-compression`, then restores the previous value and waits, up to 15 seconds, until every live link renegotiated the codec the restored settings predict (Valkey reconnects each link after the `CONFIG SET`, one per second; a reconnect left for the next wave would land in it). Links still on another codec then are recorded as `codecs_unsettled`, for [`valkey.oracle.repl-compression`](#valkeyoraclerepl-compression) to judge. |

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `waves` | list of lists | | The waves, each a non-empty list of operations. |
| `validation` | mapping | `{}` | The [state validation](cluster-chaos-and-ci.md#state-validation) settings, as for `valkey.action.cluster-chaos`. |
| `heal_timeout_s` | number, 1 to 3600 | 60 | How long the heal waits for every replica to link to the primary. |

Kill safety (the standalone policy of the
[wave engine](architecture.md#the-wave-engine)): a kill never takes the last
instance that is not down, so one copy of the latest synced data always
survives; promotions and repoints pick their targets after the wave's kills,
stalls, and partitions, and avoid the replicas those disrupt.

Ownership: every change the action makes and will undo is an
[undo obligation](contracts.md#fault-ownership) from the moment it
applies it: a stall (`process.paused`), a partition (`network.partition`),
each instance a `backlog_pressure` or `compression` changes
(`server.config`), a kill, which the heal or a `restart` undoes
(`process.stopped`), and the validation's `CLIENT PAUSE`
(`server.client-pause`). An undo that fails leaves its obligation open for
the heal, the final cleanup, and then the runner to try again, and the run
is `ERROR` even if a retry succeeds.

Overlapping faults: two held faults of a wave on the same state (a stall and
a config change of one instance, two config changes of one key) run one
after the other, and a kill waits for the held faults on its target to be
undone, so no undo restores a value another fault saved or meets a process
that is going away. A fault whose state is held by one whose undo failed is
skipped.

After every wave the action heals: it undoes stalls, then partitions, then
config changes (an undo that fails stays owned by its fault and is tried
again by the next heal and by the final cleanup); if the primary is down it promotes the live instance with
the highest offset in the dead primary's replication history
(`REPLICAOF NO ONE`) and points the others at it; it restarts every killed
instance as a replica of the primary (`replicate`, then `restart`); and it
points any instance it had told to follow someone else at the primary. An
instance told to follow the primary that does not is left alone, so
validation reports it. Then, with writes paused (`CLIENT PAUSE WRITE`), it
waits for every replica's offset to equal the primary's and validates: one
primary, every replica linked to it, no offset difference, and equal test
keys.

Loss windows: during a wave the action samples the primary's replication
offset every 0.1 seconds (and right before killing it). A forced promotion,
and the promotion that heals a dead primary, publishes
`valkey.data.loss-window` evidence `{start, end, instance_id, reason}`:
`instance_id` is the old primary; `start` is the latest sample whose offset
the new primary had reached, read before it was promoted, in the same
replication history (the old primary's term start if none); `end` is when
the old primary stopped acknowledging writes: its confirmed exit, or the
`REPLICAOF` that demoted it. A graceful promotion publishes no window.

Verdict (both chaos actions): `FAIL` when a wave's validation fails, an
instance exits without being killed, an instance does not exit after
`SIGTERM` (after `SIGKILL` it is an `ERROR`), or
Valkey refuses or does not complete a promotion, repoint, restart, or
failover with no interfering operation (replication chaos: an earlier,
landed operation of the wave that acted on an instance it involves; cluster
chaos: a kill in the failover's shard, or of a primary in the same wave).
`ERROR` when an operation had no target at all, a fault could not be
injected, an operation raised with no interfering operation (the harness
did not see what Valkey did with it: no reply, a target that did not answer,
a topology it could not read), a wave ran no operation, or the harness could
not undo a fault (even one a later heal undid), heal the deployment,
validate it, or clean up at the end; a held fault that raised is undone at
once; a wave
whose heal could not complete stops the run. `SKIP` when faults were planned
and the safety rules refused every one. A `FAIL` summary lists the `ERROR`
problems too. Replication chaos stops after the first failed wave.

Evidence: `valkey.chaos.schedule` (`operations`, each with its `wave`,
`op`, `shard`, `target`, `config`, `success`, `applied_at`, and `refused`,
`skipped`, `missing`, `error`, `undo_error`, `undone_at`/`undone_by`,
`codecs_unsettled`, or `excused_by`; `kills`, the kills whose instance is still down; `all_kills`,
every kill tried; `waves`, each with its `planned`, `applied`, `healed`
(landed, and every change it owned is undone: a held fault, or a kill the
heal restarted), `still_active` (a change it owns is still in place: its
obligation is open), and `skipped` operations, heal problems, validation,
and, for replication chaos, its primary; `still_active` and
`cleanup_problems` after the final cleanup, which also restarts the
instances replication chaos killed; `heal_promotions` and `loss_windows`), `valkey.instances.expected-down`
(still down), `valkey.chaos.test-keys`, and one `valkey.data.loss-window`
per window. Events: `<id>.instance-killed`, `<id>.promoted` (`from`, `to`,
`mode`), `<id>.wave-validated`.

### `valkey.action.replicaof`

Standalone. Points one instance at a primary, or makes it a primary, through
the deployment's `replicate` (which records the role, so a later restart
keeps it), and waits until it took effect.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `instance` | selector | | The instance (the first match). |
| `primary` | selector or null | `null` | The primary to follow (the first match other than `instance`); `null` sends `REPLICAOF NO ONE`. |
| `wait_linked` | boolean | `true` | Wait for `master_link_status:up` to that primary (or for the primary role). |
| `timeout_s` | number, 0.1 to 3600 | 60 | |

`FAIL` when `REPLICAOF` does not take effect in time while the instance
answers; `ERROR` when the command itself fails (a timeout or lost
connection), or the instance did not answer at the last look. Evidence:
`valkey.replication.replicaof` (`instance_id`, `primary_id`, `sent_at`,
`took_effect_at`).

## Faults

Every fault also takes the [trigger fields](#trigger-fields).

### `valkey.fault.process-signal`

Sends a signal to one instance. After `SIGKILL` or `SIGTERM` it waits up to 30
seconds for the process to exit, sending the signal again every second, records when it was confirmed gone (`exited_at`), and, with
`restart`, starts it again. A process that did not exit leaves the fault not
applied (`ERROR`), with nothing to restart. After `SIGSTOP` it sends `SIGCONT` once `pause_s`
has passed, or as soon as the pause is cut short; if that `SIGCONT` fails,
recovery sends it again. A kill without `restart` lasts until the deployment
is removed, so it is not undone; the fault records the instance as
`valkey.instances.expected-down`, which the liveness, ledger, dataset-digest,
and repl-compression oracles honor.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `instance` | selector or `event` | `primary` | The first selected instance. |
| `signal` | `SIGKILL`, `SIGTERM`, or `SIGSTOP` | `SIGKILL` | |
| `restart` | boolean | `true` | Restart a killed instance. |
| `restart_mode` | `recover`, `immediate`, or `background` | `recover` | When to restart: at teardown (`recover`), inside the trigger's dispatch right after `<id>.injected` so later actions see the restarted instance (`immediate`), or on a worker thread right after `<id>.injected` so the publisher is not held up (`background`). |
| `restart_delay_s` | number, 0 to 600 | 0 | Wait before an `immediate` or `background` restart. |
| `pause_s` | number, 0 to 600 | 1 | `SIGSTOP` only. |
| `ready_timeout_s` | number, 1 to 600 | 30 | How long a restart waits for the instance to become ready; the restart fails after that. Teardown waits for a `background` restart up to this plus `restart_delay_s` plus 60 seconds. |
| `capture_cluster_vars` | boolean | `false` | Cluster mode: record the `currentEpoch` and `lastVoteEpoch` the killed node's `nodes.conf` holds once the process exited, as the restart will load them. |

Events: `<id>.injected`, then `<id>.recovered` after an `immediate` or
`background` restart that started the instance again. Evidence: `valkey.fault.injected` (target, signal, PID,
`exited_at`, restart timing, and the `nodes.conf` vars when captured).

### `valkey.fault.command`

Runs a command on the selected instances when the trigger fires, such as
`BGSAVE CANCEL` or `CLIENT KILL TYPE replica`. The fault counts as applied
only if the command behaved as `expect_error` says on every target. Like
[`valkey.action.command`](#valkeyactioncommand), it sends the command once
and keeps trying to open a connection for up to 10 seconds.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `instance` | selector or `event` | `primary` | May select several instances; one that matches no live instance when the fault is installed is an `ERROR`. |
| `command` | list of strings or numbers | | |
| `expect_error` | boolean | `false` | The command must fail instead of succeed: Valkey replies with an error (a connection error or timeout is not that error). |
| `undo` | list or null | `null` | A command to run at teardown on each target the command succeeded on; it must be safe to run twice. A `CONFIG SET` command needs one. |

Events: `<id>.injected`. Evidence: `valkey.fault.injected` (targets and
replies).

### `valkey.fault.network-partition`

Drops every packet between each isolated instance and each peer with
iptables rules in a chain of its own, jumped to from `INPUT`. Client traffic
from the harness (127.0.0.1) still reaches both sides. It needs Linux, root,
iptables, and `deployment.addressing: per_instance_loopback`; pair it with
`valkey.capability.host-probe` and `requires: [linux, root, iptables,
per_instance_loopback]`. The partition heals on `heal_event`, after
`duration_s`, or at teardown. Installing is all-or-nothing: if any iptables
step fails, the chain is removed again and the fault is not applied. Every
iptables call times out after 30 seconds. A heal that fails is not raised
into the event's dispatch; recovery removes the chain and reports the
failure. A later partition removes chains left by a process that no longer
exists.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `isolate` | selector | | |
| `peers` | selector or `others` | `others` | Every instance that is not isolated, or a selection. |
| `heal_event` | string or null | `null` | |
| `duration_s` | number, 0.01 to 3600, or null | `null` | Set `heal_event` or `duration_s`, not both. |

Events: `<id>.injected`, `<id>.partitioned`, `<id>.healed`. Evidence:
`valkey.fault.injected` (with `healed_at`), which `valkey.oracle.ledger` uses
for loss windows.

### `valkey.fault.disk-fill`

Fills the filesystem at `path` with a ballast file until it is full, and
removes the ballast on `release_event` or at teardown. `path` must be
`{$ref: resources.<id>.path}` of a [`valkey.resource.tmpfs`](#valkeyresourcetmpfs)
resource and also a `deployment.instance_data_roots` entry, so instance data
lives on it. Before it writes, the fault checks that `path` is still that
size-limited mount, and refuses (`ERROR`) otherwise; it never fills host
disk.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `path` | reference | | |
| `leave_bytes` | integer, at least 0 | 0 | Free space to leave. |
| `max_bytes` | integer or null | `null` | Most bytes to write; the tmpfs size by default, never more. A fill that stops here before the disk is full is not applied, and neither is one whose call is cancelled. |
| `chunk_bytes` | integer, 4096 to 67,108,864 | 1,048,576 | |
| `release_event` | string or null | `null` | Free the space when this event is published. |

Events: `<id>.injected`, `<id>.released`. Evidence: `valkey.fault.injected`
(bytes written, free bytes after).

### `valkey.fault.deny-writes`

Makes the data directories of the selected instances read-only (mode 0500)
and restores them at teardown. It has no effect as root, where it reports
that it was not applied, which makes the run `ERROR`; pair it with
`requires: [not_root]`.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `instances` | selector | `primaries` | |

Events: `<id>.injected`. Evidence: `valkey.fault.injected`.

## Observers

Observers only record: they collect evidence for the whole run, record an
evidence item when a declared expectation is met, and never judge the
server. An expectation or rule marked `required: true` becomes required
evidence: path evidence, without which the run did not reach what it tests
and is `ERROR`. Mark only path evidence required, such as a forkless save
seen in progress. An expectation about behaviour, such as no save left in
progress or a partial resync counted, is left unmarked and checked by a
[`valkey.oracle.evidence`](#valkeyoracleevidence) oracle that depends on the
observer, which fails the run when it is missing. An observer that could not
judge an unmet expectation, because every INFO sample it needed failed or a
log could not be read, returns `ERROR`: it did not do its part, so a failure
that depends on it is not trusted. Raw samples and excerpts are saved in the
component's artifact folder.

### `valkey.observer.info`

Samples INFO on the selected instances at start (`baseline`), every
`interval_s`, on each event in `on_events`, and at stop (`final`), writes the
samples to `info-samples.jsonl`, and checks its expectations.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `instances` | selector | `primaries` | |
| `sections` | list | `[persistence]` | INFO sections to read. |
| `interval_s` | number, 0 to 3600 | 0.05 | 0 turns periodic sampling off. |
| `on_events` | list | `[]` | Events that trigger a sample. |
| `expectations` | list | `[]` | See below. |

An expectation has `evidence_id` (namespaced, unique in the list), `field`,
and either a [predicate](#predicates) or `delta_min` and `delta_max` (the
change from an instance's `baseline` sample to its `final` one). Counters
restart with the process, so a delta cannot be judged (the observer returns
`ERROR` if it is unmet) when either sample failed, the instance's process
changed between them, or the value went down. `when` picks the samples
a predicate is checked on: `any` (the default), `baseline`, `final`, or
`during` (periodic and event samples). `instances` is `any` (the default: one
instance suffices) or `all`. `required` defaults to `false`.

A `field` can read into the `key=value,...` lists some INFO fields hold with a
dotted path: `slave0.state` in `slave0:ip=…,state=online`. A `*` in the first
segment stands for an index, so `slave*.repl_compression` names that entry of
every replica line; a predicate then holds when the path names at least one
entry and every entry matches, and a delta sums the entries.

```yaml
expectations:
  - {evidence_id: valkey.forkless.in-progress, field: rdb_current_bgsave_type, equals: forkless, when: during, required: true}
  - {evidence_id: valkey.persistence.saves-advanced, field: rdb_saves, delta_min: 1, instances: all}
  - {evidence_id: valkey.repl.links-lz4, field: slave*.repl_compression, equals: lz4, when: final}
```

Evidence: one item per met expectation, under its `evidence_id`.

### `valkey.observer.log`

Notes the size of each selected instance's log at start, and at stop reads
what was written since, saves the excerpts, and checks its rules. A
`forbidden` match, such as `Bad file format reading the append only file`, is
a finding about the server, not an observer failure: the observer passes and
records it, and [`valkey.oracle.liveness`](#valkeyoracleliveness), which
every Valkey experiment includes, fails the run on it. A log that cannot be
read makes the observer `ERROR` when an unmet `expect` rule or a `forbidden`
rule needs it: it cannot show what the server did or did not log.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `instances` | selector | `all` | |
| `expect` | list | `[]` | Rules `{pattern, evidence_id, min_count, instances, required}`: a regular expression, the evidence to record, the matches needed on an instance (default 1), `any` or `all` instances (default `any`), and `required` (default `false`). |
| `forbidden` | list | `[]` | Rules `{pattern}`. |
| `max_excerpt_lines` | integer, 1 to 100,000 | 200 | Lines kept in each excerpt. |

Evidence: one item per met `expect` rule; `valkey.log.forbidden-match` on a
forbidden match, mapping each matched pattern to the instances that logged it
and up to five of their matching lines.

### `valkey.observer.log-trigger`

Tails the selected instances' logs and publishes an event for every line a
rule matches, so a fault can trigger on a log line. The payload carries
`instance_id`, `line`, `logged_at` (the line's own timestamp), `detected_at`,
`rule` (the rule's index), and the pattern's named groups; all-digit groups
become integers. Combined with `occurrence`, `when_payload`, and
`instance: event`, a fault can, for example, kill the instance that granted
the second failover vote the moment it logs it.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `instances` | selector | `all` | |
| `rules` | list | | At least one `{pattern, publish, evidence_id, min_count, max_publishes, required}`: a regular expression, the event name to publish, optional evidence to record once an instance matched `min_count` times (default 1), the most events to publish (default `null`: no limit), and `required` (needs `evidence_id`). Named groups may not be called `instance_id`, `line`, `logged_at`, `detected_at`, or `rule`. A rule without `pattern` takes the `log` pattern `spec`'s mapping gives for its `publish` event. |
| `spec` | string or null | `null` | A [spec bundle](spec-bridge.md) (`formal/<protocol>/`) whose `mapping.yaml` holds the patterns of rules that have none. |
| `spec_sha256` | string or null | `null` | Filled in when the scenario compiles: the SHA-256 of `spec`'s `mapping.yaml`, so the plan pins the mapping it was compiled against. A run whose mapping has another digest stops with `ERROR`; compile the scenario again. |
| `poll_interval_s` | number, 0.0005 to 10 | 0.002 | |
| `switch_interval_s` | number, 0.0001 to 0.1, or null | `null` | Sets Python's thread switch interval for the process while tailing, which shortens the delay before a fault fires; restored at stop. |

Evidence: one item per rule with `evidence_id` that was met. Events: the
`publish` name of each matching rule. When a log could not be read (or a
poll failed), lines may have been missed: if a rule never matched (its
evidence or its event may be in those lines), the observer returns `ERROR`.

### `valkey.observer.cluster-trace`

Cluster mode. Writes the run's events and every node's epochs to
`trace.jsonl`: a `header` record (the node map and `config_keys`), an `event`
record per event, and `observation` records (each node's `currentEpoch`,
`configEpoch`, role, slots, the primaries it knows, and the `vars` line of its
`nodes.conf`). Sampling rounds run periodically and shortly after events, on
a background thread, so the observer never delays a fault.
[`valkey-fuzzer spec conform`](spec-bridge.md#spec-conform) reads the file.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `nodes` | selector | `all` | |
| `interval_s` | number, 0 to 3600 | 0.25 | 0 turns periodic rounds off. |
| `sample_on_events` | boolean | `true` | Also sample after events. |
| `event_delay_s` | number, 0 to 60 | 0.05 | How long after an event its round starts. |
| `max_rounds` | integer, 1 to 1,000,000 | 20,000 | |
| `sample_timeout_s` | number, 0.05 to 30 | 0.5 | Client timeout per sample. |
| `config_keys` | list | `[cluster-config-save-behavior, cluster-node-timeout]` | Configs recorded per node in the header; a node whose configs could not be read gets `config_error` instead. |
| `spec` | string or null | `null` | A [spec bundle](spec-bridge.md); the header records its mapping's SHA-256 (and the plan's inputs), so `spec conform` can flag mapping drift. |
| `spec_sha256` | string or null | `null` | Filled in when the scenario compiles: the SHA-256 of `spec`'s `mapping.yaml`, so the plan pins the mapping it was compiled against. A run whose mapping has another digest stops with `ERROR`; compile the scenario again. |

Evidence: `valkey.cluster.trace.recorded` (the path and record counts).

## Oracles

Oracles run after teardown, with all the evidence recorded, and judge the
system under test.

### `valkey.oracle.liveness`

Every instance must be running, answer `PING`, and not be loading, within
one shared `timeout_s`. Instances listed in `valkey.instances.expected-down`
evidence (recorded by the chaos actions for the instances they killed and
left down) may be down; any other instance that is not running fails the run.
It also scans what each instance wrote during the run to its log and to its
captured stdout and stderr, including the instances that are down, for
`=== VALKEY BUG REPORT START` (or `REDIS`), `ASSERTION FAILED`, the
assertion's `==> file:line 'condition' is not true`, `Guru Meditation`,
`crashed by signal`, `ERROR: AddressSanitizer`, `ERROR: LeakSanitizer`,
`WARNING: ThreadSanitizer`, and `runtime error: `. Each crash is reported by
its cause, in the summary and as `crashes` (instance to cause) in the
diagnostics: the first assertion's `file:line 'condition' is not true`, else
the panic message, the sanitizer finding, the UBSan error, or the signal (an
assertion raised while the bug report is written is not the cause). An
instance that died unexpectedly without a crash report exited on a fatal
error: its cause is the last warning-level (`#`) line it logged since it
last started that names a fatal exit, such as `Unrecoverable error, aborting
now`, `aborting.`, `Exiting.`, `Fatal error`, or `Terminating server` (a
later `resuming operation` line means it carried on), reported in the
summary as `fatal exit (cause)`. The
[failure signature](cli.md#minimize) of a run with crashes is those causes.
A `FAIL` for any of these (an instance not running, not answering, still
loading, a crash report, or a fatal exit) reports a crash
(`ComponentResult.crash`), so it stands even when a teardown before the
oracles failed (see [Base contracts](contracts.md#outcomes-and-verdicts)).
And it fails when a [`valkey.observer.log`](#valkeyobserverlog) in the run
recorded `valkey.log.forbidden-match`, naming each pattern, the instances
that logged it, and the observer; that alone is no crash.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `timeout_s` | number, 0.1 to 600 | 30 | |
| `extra_crash_patterns` | list | `[]` | More regular expressions that count as crash reports. |

Evidence: `valkey.liveness.verified`.

### `valkey.oracle.ledger`

Reads every key the workload touched and checks that its value is one the
ledger allows: the value of the last acknowledged write, or of a later write
whose outcome was ambiguous. Inside a loss window an acknowledged write may
also be lost, including a preloaded key whose batch was acknowledged inside
it. A loss window runs from `loss_window_s` before a fault was
applied until `loss_window_after_s` after it ended: a partition ends when it
heals, a kill when the process is confirmed gone (`exited_at`), and a forced
[failover](#valkeyactionfailover) when it completes. Reads narrow the allowed
values: a value a read proved overwritten may not reappear, so reads are
monotonic. With `check_reads`, every acknowledged read must have returned a
value acceptable at that time. With `check_replicas`, every replica must
converge on its primary's values. Reads are retried within
`converge_timeout_s`; a read that keeps failing (a timeout, `LOADING`,
`CLUSTERDOWN`, an unreachable replica) makes the oracle `ERROR`, not `FAIL`:
the harness could not read, and [`valkey.oracle.liveness`](#valkeyoracleliveness)
judges a server that is down.

Any component can also publish `valkey.data.loss-window` evidence `{start,
end, instance_id?}`, as `valkey.action.replication-chaos` does for a forced
promotion: writes acknowledged inside `(start, end]` may be lost, with
`instance_id` (and `loss_window_scope: fault_targets`) only those that
instance acknowledged. The window fields below widen and filter these
windows like fault windows (`loss_window_faults: []` turns them all off); a
window without numeric `start` and `end` permits no loss. When the workload
ran with `wait_replicas`, a write whose `WAIT` reply counted every planned
replica is never lost, inside a window or not.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `workload` | string | | The workload component's `id`. |
| `acknowledged` | `must_survive` or `may_be_lost` | `must_survive` | `may_be_lost` allows any acknowledged write to be lost. |
| `loss_window_s` | number, 0 to 3600 | 0 | Acknowledged writes that completed from this long before a fault was applied ... |
| `loss_window_after_s` | number, 0 to 3600 | 0 | ... until this long after it ended may be lost. |
| `loss_window_until` | list | none | Components whose evidence marks when a fault's effect ended, such as a poll that saw the isolated primary demoted: each fault window runs at least until their last evidence was recorded. |
| `loss_window_faults` | list or null | `null` | The faults (and forced failovers) whose windows count; all of them by default. |
| `loss_window_scope` | `all`, `fault_target_slots`, or `fault_targets` | `all` | `fault_target_slots` limits the windows to keys in the slots of the shards the faults targeted (cluster mode). `fault_targets` also lets only writes the faults' targets acknowledged be lost. |
| `lost_writes_budget` | integer, at least 0 | 0 | Keys allowed to be missing. |
| `check_reads` | boolean | `true` | |
| `window_reads` | `strict` or `written` | `strict` | `written`: inside a loss window, a read only has to return some value written to the key. |
| `check_replicas` | boolean | `true` | |
| `converge_timeout_s` | number, 0 to 600 | 30 | How long the dataset read is retried, and one budget shared by all replicas. |

With `acknowledged: must_survive`, the scenario does not compile when:

- a forced failover is not listed in `loss_window_faults` (when that list is
  set), because it can lose writes the old primary acknowledged;
- every instance runs `appendonly yes` with `appendfsync always`, the windowed faults are all
  `valkey.fault.process-signal`, no replica can take over (standalone, or no
  replicas), and `loss_window_s` or `loss_window_after_s` is above 0. Such a
  crash cannot lose an acknowledged write;
- the same, but with `appendfsync everysec` and a `SIGKILL`, and
  `loss_window_s` is below 2.5: everysec may lose what it acknowledged up to
  two seconds before the kill.

Both read each instance's effective config, the local deployment's
defaults (`appendonly yes`, `appendfsync everysec`) included.

`FAIL` when a key holds a value outside its acceptable set, more keys are
missing than the budget allows, a read returned an unacceptable value, or a
replica does not converge. Evidence: `valkey.data.ledger-verified`, with
`wait_confirmed_replicas` and `wait_protected_ops` under `wait_replicas`.

### `valkey.oracle.rdb-snapshot`

Loads every RDB a `valkey.action.bgsave` captured into a private server
running that instance's binary and loading its modules (the `loadmodule` of
its server config, unless `load_config` sets one). Every bgsave target must
have produced an RDB.
With `workload`, each RDB must hold only keys the workload wrote, every stable
key must hold its value, the RDB must be a consistent cut of the write
history taken while `BGSAVE` ran, and each of the workload's lists it holds
must have the length and elements the workload wrote (`lists.jsonl`). A
bgsave with `capture_rdb: false` is rejected.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `bgsave` | string | | The `id` of the `valkey.action.bgsave` whose captured RDBs to check; checked when the scenario compiles. |
| `workload` | string or null | `null` | The workload's `id`. |
| `timeout_s` | number, 1 to 3600 | 60 | |
| `expect_codec` | `none`, `lz4`, `zstd`, an `rdbcompression` value, or null | `null` | Every RDB's `rdb_format.codec` must be this (`no`, `yes`, and `lzf` mean `none`), and its load must log `Loading compressed RDB (algo=X)` exactly when the codec is compressed. |
| `instance_reload` | boolean | `false` | With `expect_codec`: the saving instance must also have reloaded during the run (`DB loaded from …`), in that codec from `dump.rdb` and with no compressed load from the AOF (Valkey writes an AOF's base RDB plain). |
| `load_binary` | string or reference, or null | `null` | Load with this binary instead of the saving instance's. |
| `load_config` | mapping | `{}` | Server config of the loading server, such as `{rdb-version-check: relaxed}`. |
| `expect` | `loads` or `refused` | `loads` | `refused`: the loading server must refuse every RDB cleanly (exit status 1, `Fatal error loading the DB`, no crash report); a configuration error is not a refusal. |
| `refused_log` | regex or null | `null` | With `expect: refused`: the refusal must log this. |

`FAIL` when a target captured no RDB, one fails to load (or loads when it
must be refused), is in another codec, or its contents are wrong. `ERROR`
when the loading server did not start or was still loading after
`timeout_s`: whether the RDB loads is unknown; and when a target's save was
`unanswered` or `unavailable`, or its RDB could not be copied.
Evidence: `valkey.rdb.reload-verified` (per-snapshot results and a
`content_digest` of the loaded data).

### `valkey.oracle.repl-compression`

Checks every primary-to-replica link against a model of Valkey's negotiation, built from each instance's
`repl-compression`, `rdbcompression`, `repl-diskless-sync`, and `dual-channel-replication-enabled`, read
when the oracle runs:

- A replica advertises what its own `repl-compression` allows: `zstd` advertises zstd and lz4, `yes` and
  `lz4` advertise lz4, `no` nothing. A build without `repl-compression` advertises nothing.
- A primary uses the strongest codec its `repl-compression` allows that the replica advertised; a build
  without `repl-compression` sends plaintext.
- A full sync sends the RDB in that codec, disk-based only when the primary has `repl-diskless-sync no`,
  the codec equals the whole-stream codec of its `rdbcompression`, and the replica is a build with
  `repl-compression` (an older one reads an older RDB version); otherwise diskless. Dual-channel needs
  it enabled on both sides and the primary's `repl-diskless-sync yes`; a sync that is diskless only
  because of its codec or the replica's RDB version stays a normal sync.
- A replica that loads from the socket loads the wire codec; one that loads from disk first rewrites the
  RDB into its own `rdbcompression`.

It waits up to `converge_timeout_s` for INFO to agree with the model, then requires, per link: INFO
`slaveN:repl_compression` equals the predicted codec (absent for plaintext) and the compressor saw bytes;
the primary logged `Replication compression enabled for replica R (algo=X)` in the link's last session
(since its last `Replica R asks for synchronization`), and never for a plaintext link.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `converge_timeout_s` | number, 0 to 600 | 30 | How long INFO may take to match, for example after a runtime change. |
| `min_ratio` | number or null | `null` | Every compressed link's uncompressed-to-compressed byte ratio must reach this. |
| `full_sync` | boolean | `false` | Each link's last full sync must have run as predicted: the primary's `Starting BGSAVE for SYNC with target: T using: U, compression: X` line names the predicted target, dual-channel or normal sync, and codec, and the replica's load took the predicted codec. |
| `expect_algo` | `none`, `lz4`, `zstd`, or null | `null` | Every link's predicted codec must be this; a scenario that configures something else (or two identical builds for a mixed-version pair) tests another codec, an `ERROR`. |
| `min_links` | integer | 1 | Fewest links the deployment must have. |

`FAIL` names the link, the expected codec and the configuration it came from, and what INFO or the log
showed. `ERROR` when an instance is still unreachable at `converge_timeout_s`: its links were never seen;
when a log the check needs cannot be read; or when the predicted codec is not `expect_algo`. Evidence: `valkey.repl.compression-verified` with `links` (`primary`, `replica`, `algo`,
`observed`, `compressed_bytes`, `uncompressed_bytes`, `ratio`, `enabled_logged`, and with `full_sync`
the expected and logged sync and the replica's load) and `instances` (each one's configuration).

### `valkey.oracle.deployment-state`

Runs the deployment's state validators at the end of the run, with retries:
replication links, cluster state, slot coverage, roles (the planned number
of primaries and replicas), cluster views, data consistency, and node logs.
Outside cluster mode only the replication check (when there are replicas)
and the role check run, and the role check runs even with
`check_roles: false`. After a chaos action (`valkey.cluster-chaos`,
`valkey.replication-chaos`) these are the validators the action runs after
every wave: the action's kills count as stopped on purpose, the data check
reads its test keys, and the log check reads the logs of the shards its
failovers touched. Without the action's test keys the data check is off (a
note says so), and without its schedule the log check is.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `validation` | mapping | `{}` | The [state validation](cluster-chaos-and-ci.md#state-validation) settings, as the chaos actions take them (`stabilization_wait`, `validation_timeout`, the `check_*` flags, ...). `valkey.base` sets `stabilization_wait: 1` and `validation_timeout: 60`. |
| `chaos` | string or null | `null` | The chaos action whose kills, failovers, and test keys to use; any by default. |

`FAIL` when a check finds the deployment wrong; `ERROR` when the validation
stayed [inconclusive](cluster-chaos-and-ci.md#state-validation) (a running
instance did not answer, or the time ran out). A `validation` whose
`stabilization_wait` is not below its `validation_timeout` does not
compile. A wave whose
validation stayed inconclusive is an error of the chaos action, not a
failure. Evidence: `valkey.deployment.validated`, the validation summary
with the oracle's `notes`.

### `valkey.oracle.cluster-election-safety`

Cluster mode. Checks the invariants of the
[TLA+ election model](../formal/cluster-election/README.md) on the live run, from vote and
win events published by a log-trigger observer, the crash faults' records,
the trace, and the final cluster views:

- **VoteOncePerEpoch:** no primary voted for two candidates in one epoch;
- **OneWinnerPerEpoch:** no epoch promoted two replicas;
- **Win:** every winner counted distinct votes from a quorum (the larger of
  its logged quorum and a majority of shards), from the
  `Failover auth ACK` lines Valkey logs since 9.2;
- **votes saved before they leave:** when durable votes are required, no
  vote that a crashed primary had not saved to `nodes.conf` reached a
  candidate before the primary's new process was spawned (an ACK logged
  after that may be the new process's own vote, even before it is ready);
- **no split primary:** no slots claimed by two primaries, and no two
  primaries with overlapping slots and the same `configEpoch`;
- the views converge on one owner per slot.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `trace` | string or null | `null` | The trace observer's `id`. |
| `crash_faults` | list | `[]` | Fault `id`s whose kills to check. |
| `spec` | string | `cluster-election` | The [spec bundle](spec-bridge.md) whose `mapping.yaml` names the vote, ACK, and win events (its `granted`, `acked`, and `won` evidence) and their fields. |
| `spec_sha256` | string or null | `null` | Filled in when the scenario compiles: the SHA-256 of `spec`'s `mapping.yaml`, so the plan pins the mapping it was compiled against. A run whose mapping has another digest stops with `ERROR`; compile the scenario again. |
| `require_durable_votes` | `auto`, `always`, or `never` | `auto` | `auto` requires them when every node runs `cluster-config-save-behavior sync`; a node whose setting cannot be read then leaves a vote that left before it was saved unjudged (ERROR). |
| `converge_timeout_s` | number, 0 to 600 | 60 | |

`ERROR` when elections were won but no ACK line was published as the
mapping's `acked` event, so the quorum check cannot run, when the views
did not converge while a running node's view could not be read, and when
`trace` names an observer whose trace is missing or unreadable.

Evidence: `valkey.cluster.election-safety.verified`; also
`valkey.cluster.vote.unsaved-at-crash` when a crash caught a vote its
`nodes.conf` did not hold.

### `valkey.oracle.binary-check`

Each instance's `INFO server` must report the executable, version, git SHA,
and process ID of the binary and process the deployment assigned it, and at
least `min_distinct_builds` distinct builds must be running. Reads are
retried for 30 seconds; a running instance that never answers makes the
oracle ERROR, and one that is not running is left to `valkey.oracle.liveness`.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `min_distinct_builds` | integer, 1 to 64 | 1 | Distinct (version, git SHA) builds that must be running. |
| `expect_binary` | string, reference, or null | `null` | Every instance must be assigned this binary. |

Evidence: `valkey.binary.verified`.

### `valkey.oracle.dataset-digest`

Records a canonical digest of every primary's dataset (all keys, with values
compared by type), for differential runs, and writes a key listing as
`keyspace.jsonl`. The digest itself never fails a run. The oracle fails only
when no instance is a primary, in cluster mode a key is held by two primaries, or, with
`include_replicas`, a replica does not converge on its primary's digest.
Reads are retried within `converge_timeout_s`; an instance that stays
unreachable or unreadable makes it `ERROR`, since a digest that left it out
would compare as a different (or the same) dataset.

A value of a module's type is digested from its `DUMP` payload, less the
trailing 2-byte RDB version and 8-byte CRC64, so the same value written by two
builds digests the same. This compares the module's serialization, not its
logical data: a module that changes its format between builds changes the
digest without a bug. A module's save may write a hash table in iteration
order, which differs between processes, so before judging any digest the
oracle probes each module type it found: one key's payload is `RESTORE`d on
two private servers of its primary's binary, started with the deployment's
`loadmodule`, and the two `DUMP`s must be the same. A type that serializes
differently is `ERROR` ("this module's serialization is not deterministic
across processes"), never a `FAIL` across replicas.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `match` | string or null | `null` | A `SCAN` pattern. |
| `include_replicas` | boolean | `false` | |
| `converge_timeout_s` | number, 0 to 600 | 30 | |
| `max_keys_listed` | integer, at least 0 | 100,000 | Keys written to the listing. |

Evidence: `valkey.dataset.digest` (`digest`, key count, per-instance digests,
and `module_types`, the module types it digested and probed).

### `valkey.oracle.evidence`

Judges what other components recorded: `FAIL` when required evidence is
missing or forbidden evidence was recorded. Observers only record, so this is
where an experiment checks behaviour an observer saw, such as a partial
resync counted or no save left in progress.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `require` | list | `[]` | Rules `{evidence_id, source, min_count}`: the evidence `source` (a component `id`) must have recorded at least `min_count` times (at least 1, default 1). |
| `forbid` | list | `[]` | Rules `{evidence_id, source}`: evidence `source` must not have recorded. At least one `require` or `forbid` rule. |
| `preconditions` | list | `[]` | Rules `{evidence_id, source, min_count}`: evidence the run must have recorded for the other rules to apply. Without it the run never reached the state they are about, and the oracle returns `ERROR`. |

Every rule's `source` must be a component the oracle depends on (in its
`depends_on`, directly or through others), and must declare the rule's
`evidence_id`; otherwise the scenario does not compile. A source that could
not read the server returns `ERROR`, and a `FAIL` that depends on it is not
trusted. A misspelt ID would otherwise make a `forbid` rule pass vacuously
and a `require` rule fail every run.

```yaml
- use: valkey.oracle.evidence
  id: psync
  depends_on: [info, logs]
  config:
    require:
      - {evidence_id: valkey.replication.partial-resync-counted, source: info}
      - {evidence_id: valkey.replication.psync-accepted-logged, source: logs}
```

## Providers

Scenarios declare artifacts and resources with these providers; see
[Artifacts, resources, and policies](scenarios-and-experiments.md#artifacts-resources-and-policies).

### `valkey.artifact.local-binary`

Resolves a `valkey-server` on this host, records its SHA-256, version, and git
SHA, and optionally verifies them.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `path` | string or reference | | A path or a command name on `PATH`. |
| `expected_version` | string or null | `null` | The version `--version` must report. |
| `expected_sha` | string or null | `null` | A prefix of the git SHA or of the file's SHA-256. |

Output: `binary`, the absolute path.

### `valkey.artifact.local-module`

A module build on this host, such as valkey-json's `libjson.so` or one of
Valkey's `tests/modules`, for a deployment's `server_config.loadmodule` or the
`modules` of `valkey.action.commands`. Its SHA-256 is the provenance a replay
compares, so a replay that loads another build of the module reports drift,
as for a binary.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `path` | string or reference | | The module file; a relative path is taken from the current directory. |

Output: `path`, the absolute path. A bare path only, without module
arguments: Valkey reads a module's arguments from the words after its path on
one `loadmodule` directive, and a server config value is one argument (a list
repeats the directive), so arguments could not reach the module.

### `valkey.resource.tmpdir`

A private directory, removed after the run.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `prefix` | string | `valkey-fuzzer-` | |
| `parent` | string or null | `null` | The directory to create it in; the temporary directory by default. |
| `keep` | boolean | `false` | Keep it after the run. |

Output: `path`.

### `valkey.resource.tmpfs`

A size-limited tmpfs mount, unmounted after the run. Linux and root only;
elsewhere the provider declines and the run is `SKIP_UNSUPPORTED`. It is the
only filesystem `valkey.fault.disk-fill` fills. A busy unmount is retried,
and the mount stays owned until it is confirmed gone, so a later cleanup or
rollback can still release it. `mount` and `umount` time out after 60
seconds.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `size_mb` | integer, 1 to 65,536 | 64 | |
| `prefix` | string | `valkey-fuzzer-tmpfs-` | |

Output: `path`, the mount point.
