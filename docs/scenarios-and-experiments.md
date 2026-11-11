# Scenario and experiment reference

This page is the reference for the two YAML formats: scenarios, which say what
to run where, and experiments, which say what a run does and what it must
observe. It also documents the built-in local deployment and lists the Valkey
experiments. The components that experiments use are in
[Valkey components](components.md); the commands are in the
[command-line reference](cli.md). The model behind both formats is in
[Concepts and glossary](concepts.md).

- [Scenarios](#scenarios)
- [Input references](#input-references)
- [Artifacts, resources, and policies](#artifacts-resources-and-policies)
- [The local deployment](#the-local-deployment)
- [Experiments](#experiments)
- [Fragments](#fragments)
- [The Valkey experiments](#the-valkey-experiments)

## Scenarios

A scenario selects one experiment, supplies its inputs, and describes the
deployment. Unknown fields and duplicate YAML keys are rejected, here and in
every other file the fuzzer reads: reports, plans, batch specs, differential
runs. YAML scalars are read as YAML 1.2 reads them, not as YAML 1.1 does:
only `true` and `false` are booleans (`yes`, `no`, `on`, and `off` are
strings, so write `"yes"` or `yes` alike for a server config value, and
`true` for a boolean field), numbers are plain decimals (`0o` and `0x`
prefixes allowed), `1:30`, `1_000`, and dates are strings, and an integer
with a leading zero such as `017400` is refused, because YAML 1.1 reads it as
octal. A `!!binary` scalar loads as `{$base64: ...}`, since plans and
reports hold JSON data; `valkey.action.commands` sends one as bytes.

```yaml
scenario_id: forkless-save-cluster
seed: 11
experiment:
  id: valkey.forkless-save     # experiment ID
  inputs: {stable_keys: 2000}  # checked against the experiment's inputs schema
deployment:
  mode: cluster                # standalone | cluster
  primaries: 3                 # cluster mode only
  replicas_per_primary: 1
  base_port: 7400
```

| Field | Required | Meaning |
| --- | --- | --- |
| `scenario_id` | yes | Name used in reports and folder names: letters, digits, `.`, `_`, `:`, `-`. |
| `seed` | yes | Integer. Seeds every component's random streams and names the instance log files. |
| `experiment` | yes | `id` and optional `inputs`. |
| `deployment` | yes | `mode`, optional `provider`, and the provider's own fields. |
| `artifacts`, `resources`, `policies` | no | See [Artifacts, resources, and policies](#artifacts-resources-and-policies). |

`experiment.id` names one installed experiment. `experiment.inputs` is
checked against the experiment's [inputs schema](#inputs), and every input
the scenario leaves out gets its default; the execution plan records the
result.

`deployment.mode` must be one of the experiment's supported modes.
`deployment.provider` names the deployment provider; it may be left out when
only one provider is registered for the mode, which is the case for the
built-in modes (`valkey-fuzzer.deployment.local`). The plan records the
provider's identity, not the field. Every other deployment field belongs to
the provider; the local provider's fields are under
[The local deployment](#the-local-deployment).

## Input references

`{$ref: inputs.<name>}` inserts a resolved input value. A path can go deeper
(`inputs.chaos.signal`), and a number selects a list item. References may
appear in:

- an experiment's component `config`, startup overlay `server_config`, and
  `metadata`;
- a scenario's `deployment`, `artifacts`, `resources`, and `policies`.

A reference must be the whole value: `{$ref: ...}` with no sibling keys. A
scenario's `experiment.inputs` may reference only artifact and resource
outputs. To pass a mapping that looks like a reference through unchanged,
wrap it: `{$valkey_fuzzer_literal: {$ref: "#/components/schemas/Request"}}`.

## Artifacts, resources, and policies

Artifacts (such as binaries) and resources (such as filesystems) are acquired
by registered providers before the deployment starts and released in reverse
order afterwards. Each entry is keyed by its ID and has `use` (the provider
ID), optional `with` (the provider's config), and optional `depends_on`
(other `artifacts.<id>` or `resources.<id>` entries).

```yaml
scenario_id: rolling-upgrade
seed: 61
artifacts:
  baseline: {use: valkey.artifact.local-binary, with: {path: /opt/valkey-8.1/src/valkey-server}}
  candidate: {use: valkey.artifact.local-binary, with: {path: /opt/valkey/src/valkey-server}}
experiment:
  id: valkey.rolling-upgrade
  inputs:
    to_binary: {$ref: artifacts.candidate.binary}
deployment:
  mode: standalone
  replicas_per_primary: 1
  base_port: 7470
  valkey_binary: {$ref: artifacts.baseline.binary}
```

`{$ref: artifacts.<id>.<output>}` and `{$ref: resources.<id>.<output>}` name
one output of a declared entry. The compiler checks that every reference
names a declared entry and an output its provider declares, that the
dependency graph has no cycle, and that only config fields declared to accept
references hold them. Each entry's `with` is checked by its provider and
recorded with its defaults. References stay symbolic in the execution plan
(reports show them as `{$valkey_fuzzer_ref: artifacts.<id>.<output>}`); the
runner executes a copy of the plan with each reference replaced by the
acquired value. A provider that declines the host (for example
`valkey.resource.tmpfs` on macOS) makes the run `SKIP_UNSUPPORTED` before
anything starts.

Policies have `use` (a policy type) and optional `with`. The core passes them
to components without interpreting them; no Valkey component reads one
today.

The Valkey providers are listed under [Providers](components.md#providers).

## The local deployment

The built-in deployment provider, `valkey-fuzzer.deployment.local`, runs a
deployment as `valkey-server` processes on this host. It handles both
`standalone` and `cluster` mode. How it starts and forms them is in
[Architecture](architecture.md#the-local-deployment-provider).

| Field | Default | Meaning |
| --- | --- | --- |
| `mode` | required | `standalone` (one primary and its replicas) or `cluster`. |
| `primaries` | 3 | Cluster mode only: 3 to 16 primaries, one per shard. |
| `replicas_per_primary` | standalone: 0; cluster: 1 | 0 to 5 replicas for each primary. |
| `base_port` | 7000 | `valkey-N` listens on `base_port + N`; its cluster bus port is that plus 10000. Every port must be at most 65535, and below the host's ephemeral port range (Linux: from 32768; macOS: from 49152), where an outgoing connection can take a port before Valkey binds it: provisioning refuses a deployment that reaches it and names the highest `base_port` that fits. |
| `base_data_dir` | `/tmp/valkey-fuzzer` | Instance data under `deployment-<id>/valkey-N/data`, logs under `logs/`. |
| `valkey_binary` | see below | The `valkey-server` every instance runs unless `instance_binaries` says otherwise. |
| `enable_cleanup` | `true` | Delete the deployment's data directories after the run. Logs are kept. |
| `server_config` | `{}` | Server config for every instance. |
| `instance_configs` | `{}` | Server config by role (`primary`, `replica`) or instance ID. |
| `instance_binaries` | `{}` | Binary by role or instance ID. |
| `instance_data_roots` | `{}` | Where an instance's RDB, AOF, and `nodes.conf` live, by role or instance ID. Logs stay under `base_data_dir`. |
| `addressing` | `shared_loopback` | `per_instance_loopback` makes `valkey-N` bind `127.0.B.(N+2)` (`B` is `loopback_block`) and connect from it, so network faults can match instance pairs (at most 253 instances). |
| `loopback_block` | 0 | `B` above, 0 to 255. A [batch](cli.md#isolation-deadlines-and-leaks) sets it per worker. A per-instance deployment holds its block host-wide (a lock under `/tmp/valkey-fuzzer-locks`) until it is cleaned up, since a partition's rules match addresses only; a run whose block another run holds is an `ERROR` before anything starts. |

The execution plan records `primaries` and `replicas_per_primary` with their
defaults; the other defaults apply when the deployment is planned. Unknown
fields are rejected, and so is `primaries` in standalone mode.

### Instances

Instances are named `valkey-0`, `valkey-1`, and so on, in plan order:

- **standalone:** `valkey-0` is the primary and the rest are its replicas;
- **cluster:** each shard's primary is followed by its replicas. The 16,384
  hash slots are divided evenly among the primaries, and the last one also
  takes the remainder.

For example, 4 primaries with 2 replicas each is 12 instances: `valkey-0` is
the primary of shard 0 with slots 0 to 4095, `valkey-1` and `valkey-2` are its
replicas, `valkey-3` is the primary of shard 1 with slots 4096 to 8191, and so
on.

In `instance_configs`, `instance_binaries`, and `instance_data_roots`, a key
is `primary`, `replica` (only when there are replicas), or an instance ID. An
instance ID wins over its role, and the role wins over the deployment-wide
value.

### Server configuration

Each instance starts with these settings, which `server_config`,
`instance_configs`, and startup overlays can override:

- `--appendonly yes --appendfilename appendonly.aof --appendfsync everysec
  --save ""` (AOF on, fsync every second, RDB snapshots off);
- `--loglevel notice`;
- `--maxmemory 500mb`, so a run cannot exhaust the host; with Valkey's default
  `noeviction` policy, hitting the cap shows up as write errors;
- `--crash-memcheck-enabled no`: a crashing server does not test its memory
  for hardware faults first, which under a sanitizer is reported in place of
  the crash;
- `--enable-debug-assert yes`: Valkey's assertions that are too costly for
  production run, as in Valkey's own test suite (a build older than 8.0, which
  lacks the config, starts without it);
- `--sanitize-dump-payload yes`, so every RDB payload a server loads (its own
  file at a restart, a full sync's, a `RESTORE`'s) is validated in full, and
  data a server wrote wrongly stops the load instead of corrupting the server
  that loaded it (Valkey 10 always validates, and only accepts the option);
- in cluster mode, `--cluster-node-timeout 1000` and
  `--cluster-require-full-coverage no`.

Config layers apply in order: these defaults, `server_config`,
`instance_configs` for the role, `instance_configs` for the instance ID, then
the experiment's startup overlays. The plan records the result for every
instance (`instances` in `plan.json`), and compile-time checks read it, so
relying on a default is checked like writing it out. Keys are normalized (lowercase, `_` becomes `-`, a leading `--` is
dropped), and two keys that normalize to the same option are rejected. A list
value repeats a directive; `true` and `false` become `yes` and `no`.

The provider owns the options that define an instance's identity and address,
and rejects them in any config: `bind`, `bind-source-addr`, `port`, `dir`,
`logfile`, `pidfile`, `daemonize`, `include`, `proc-title-template` (it carries
the owner tag `valkey-fuzzer sweep` finds leftover servers by), `protected-mode`
(always `no`), `cluster-enabled`,
`cluster-config-file`, `cluster-port`, `cluster-announce-ip`,
`cluster-announce-port`, `cluster-announce-bus-port`,
`cluster-announce-tls-port`, `cluster-announce-hostname`,
`cluster-announce-human-nodename`, `replicaof`, `slaveof`,
`replica-announce-ip`, `replica-announce-port`, `slave-announce-ip`,
`slave-announce-port`, `tls-port`, `unixsocket`, and `unixsocketperm`.

### Binaries

The deployment-wide binary is, in order: `valkey_binary` (a path, which must
exist and be executable, or a command name on `PATH`); `valkey-server` on
`PATH`; a previous build at `/tmp/valkey-build/valkey/src/valkey-server`;
otherwise a fresh clone of release 9.1.3 of
`https://github.com/valkey-io/valkey.git` built with `make`, under a lock so
concurrent runs build it once. `--valkey-binary` on the command line sets
`valkey_binary`. Every `instance_binaries` entry must name an existing
binary. The outcome records the server's git SHA, read from
`valkey-server --version`, as `valkey_sha`, and each instance's binary and its
SHA-256 under `diagnostics.deployment.instances`.

### Modules

A deployment loads a module through `server_config.loadmodule`, usually a
[`valkey.artifact.local-module`](components.md#valkeyartifactlocal-module)
artifact; a list loads several:

```yaml
artifacts:
  module: {use: valkey.artifact.local-module, with: {path: /opt/valkey-json/build/src/libjson.so}}
deployment:
  mode: standalone
  server_config:
    loadmodule: {$ref: artifacts.module.path}
```

A bare path only: Valkey reads a module's arguments from the words after its
path on one directive, and a config value is one argument. What the fuzzer
does with a module:

- **Is it loaded?** A [build probe](components.md#valkeycapabilitybuild-probe)
  with `commands: [JSON.GET]` and `deployment_config: [loadmodule]` checks the
  commands on a server started with the deployment's config, so a
  deployment that does not load the module is `SKIP_UNSUPPORTED`.
- **Its data.** [`valkey.oracle.dataset-digest`](components.md#valkeyoracledataset-digest)
  digests a module value by its `DUMP` payload, less the RDB version and
  CRC64, after checking that the module serializes a value the same way in
  two processes (a module that does not is `ERROR`, never a `FAIL`);
  [`valkey.oracle.rdb-snapshot`](components.md#valkeyoraclerdb-snapshot)
  reloads an RDB with the saving instance's modules. This compares the
  module's serialization, not its logical data, so a module that changes its
  format changes the digest without a bug. A module command that does not
  replicate itself (`datatype.set` of Valkey's `tests/modules/datatype.so`
  never does) leaves replicas without its writes, which `include_replicas`
  reports as replicas that do not hold their primary's data; write such values
  with `RESTORE` of their payload, which replicates.
- **Its commands, across builds.** [`valkey.action.commands`](components.md#valkeyactioncommands)
  starts two private servers that load `modules` and `modules_parent`, on one
  Valkey build (or, with `compare_with`, each on its own), and compares their
  replies; `compare: json` compares valkey-json's `JSON.GET` replies as JSON
  values. [`examples/valkey/experiments/module-commands-compare.yaml`](../examples/valkey/experiments/module-commands-compare.yaml)
  is such a sequence on `tests/modules/datatype.so`, and
  [`examples/valkey/module_commands_compare.yaml`](../examples/valkey/module_commands_compare.yaml)
  runs it with the two module builds declared as `artifacts.module` and
  `artifacts.module_parent`, the names the [PR batch](cluster-chaos-and-ci.md#scheduled-and-repository-workflows)
  declares when it builds a module.

The first module the fuzzer supports is valkey-json, after Valkey's own
`tests/modules`.

### Ports, logs, and cleanup

Before resolving binaries or starting anything, the provider checks that every
planned client port (and, in cluster mode, cluster bus port) is free. If any is
in use, it names every busy port and its instance and starts nothing, so a
server it did not start never receives commands such as `CLUSTER RESET HARD`. Two
scenarios that run at the same time need port ranges that do not overlap.

Logs go to `<base_data_dir>/logs/<seed>-<deployment id>-valkey-N.log`, and
each instance's stdout and stderr (where sanitizer reports land) to a
`.stdio.log` next to it. Every deployment writes its own files, so runs with
the same seed, even concurrent ones, never share a log.
`valkey-fuzzer cleanup-logs SEED` deletes them.

At cleanup each instance gets `SIGTERM`, and `SIGKILL` if it has not exited
after 5 seconds; then, with `enable_cleanup` and once every instance has
stopped, its data directories are deleted.

All instances share the host's CPUs, memory, and disk and are not pinned to
CPUs, so a busy host changes timing.

## Experiments

An experiment is a data-only YAML document that composes registered
components. How to write one is in
[Adding tests](extending.md#write-a-data-only-experiment); this section
lists the format.

```yaml
extends: [valkey.base]                    # fragments, merged first
id: valkey.forkless-save
mode: experiment                          # or smoke
deployment:
  modes: [standalone, cluster]            # plus optional rules for deployment fields
metadata:
  description: shown by `valkey-fuzzer list experiments`
inputs:
  properties:
    key_save_delay_us: {type: integer, minimum: 0, maximum: 100000, default: 500}
startup_overlays:
  - overlay_id: forkless-enabled
    selector: primaries                   # left out: every instance
    server_config:
      bgsave-default-method: forkless
      rdb-key-save-delay: {$ref: inputs.key_save_delay_us}
components:
  - use: valkey.action.bgsave             # registration ID; the role comes from it
    id: bgsave                            # unique within the experiment
    depends_on: [writes]
    timeout_s: 120                        # optional deadline for each call
    config: {instances: primaries, expect: ok}
required_events: [bgsave.save-completed]
required_evidence:
  - {evidence_id: valkey.bgsave.result, source: bgsave}
```

| Field | Required | Meaning |
| --- | --- | --- |
| `id` | yes | `<namespace>.<name>`, for example `valkey.forkless-save`. Unique among installed experiments. |
| `deployment` | yes | `modes` and optional field rules; see [deployment](#deployment). |
| `components` | yes | The configured components; see [components](#components). |
| `extends` | no | Fragments merged before the document; see [Fragments](#fragments). |
| `mode` | no | `experiment` (the default) or `smoke`; see [Modes](#modes). |
| `inputs` | no | The schema of the inputs a scenario passes; see [inputs](#inputs). |
| `startup_overlays` | no | Server config for selected instances; see [startup_overlays](#startup_overlays). |
| `required_events`, `required_evidence` | no | What a run must observe; see [Required events and evidence](#required-events-and-evidence). |
| `metadata` | no | Free-form; `description` is shown by `list experiments`. It cannot reference artifacts or resources. |

### deployment

`modes` lists the deployment modes the experiment supports. Every other key is
a rule for one deployment field, with `type` (one type or a list, as for
[inputs](#inputs)), `enum`, `minimum`, `maximum`, and `description`:

```yaml
deployment:
  modes: [standalone, cluster]
  replicas_per_primary: {minimum: 1}
```

The compiler checks the rules against the deployment document after the
provider filled in its defaults, so `replicas_per_primary: {minimum: 1}`
rejects a standalone scenario that asks for no replicas. A field a rule names
must be present after normalization.

### inputs

`inputs` is a small, strict subset of JSON Schema:

- the root has `type: object`, `properties`, `required`,
  `additionalProperties` (default `false`), and the annotations `title`,
  `description`, and `$schema`;
- each property has `type` (`string`, `integer`, `number`, `boolean`, `array`,
  `object`, `null`, or `environment_reference`), `enum`, `minimum`, `maximum`,
  `default`, `reference_kinds`, and the annotations `title` and `description`.
  `type` may be a list, such as `[string, "null"]`, which takes a value of any
  of them; `environment_reference` cannot be in a list, and `null` cannot be
  alone in one.

A property without `type` accepts any value that holds no artifact or resource
reference. An `environment_reference` property accepts only an exact
`{$ref: artifacts.<id>.<output>}` or `{$ref: resources.<id>.<output>}`;
`reference_kinds: [artifacts]` or `[resources]` limits it to one kind. An
experiment without `inputs` accepts any inputs that hold no artifact or
resource reference. Input names are letters,
digits, underscores, and hyphens. Deeper checks belong in the components that
receive the inputs.

### components

| Field | Meaning |
| --- | --- |
| `use` | The registration ID, for example `valkey.action.bgsave`. Its role (capability, workload, action, fault, observer, oracle) comes from the registration; an unknown `use` is a compile error. |
| `id` | Unique within the experiment. Events, evidence sources, and `depends_on` use it. |
| `role` | Optional. When given, it must be the registration's role. |
| `config` | Checked by the registration's config schema at compile time; the plan records it with every default. |
| `depends_on` | IDs that must pass first. A component whose dependency did not pass is skipped. |
| `required` | Default `true`. See [Outcomes and verdicts](contracts.md#outcomes-and-verdicts). |
| `timeout_s` | Deadline for each call; see [Deadlines and cancellation](contracts.md#deadlines-and-cancellation). |

The runner starts observers, workloads, faults, and actions in dependency
order, and in document order where dependencies allow. Fragments' components
come after the experiment's own unless the experiment lists them; see
[Fragments](#fragments).

### startup_overlays

An overlay adds `server_config` to the instances its `selector` picks, before
they start. `selector` takes the [instance selector](components.md#instance-selectors)
forms (`all`, `primaries`, `primary`, `replicas`, `replica`, an instance ID, a
list of IDs, or `{roles, instance_ids, first}`); left out, it selects every
instance. Roles here are the planned roles. A selector that names an unknown
role or instance, or matches nothing, is an error. `conflict_policy` is
`override` (the default: a later value replaces an earlier one) or `error`
(reject a different value already set by the deployment or an earlier
overlay).

### Required events and evidence

- `required_events` lists event names, and `required_evidence`
  `{evidence_id, source, min_count}`: at least `min_count` (default 1)
  matching items, from `source` (a component `id`; optional). Both are path
  evidence, showing the run reached what it claims to test: a run missing one
  is `ERROR`, unless an action or oracle found a trusted `FAIL` anyway. A
  check on behaviour an observer records belongs in a `valkey.oracle.evidence`
  oracle instead, which fails the run (see
  [Outcomes and verdicts](contracts.md#outcomes-and-verdicts)).

A registration can also derive requirements from a component's config: the
`valkey.observer.info`, `valkey.observer.log`, and
`valkey.observer.log-trigger` observers turn every expectation or rule marked
`required: true` into required evidence with the observer as its source.

### Modes

In `experiment` mode (the default) the execution plan needs at least one
required oracle and at least one required evidence item, listed or derived,
and every required event and evidence ID must be declared by a component of
the plan ([Evidence contract](contracts.md#evidence-contract)).
`smoke` mode has no such requirement and makes no coverage claim: it only
tests that a deployment starts and stops, as in
[`examples/experiments/deployment-smoke.yaml`](../examples/experiments/deployment-smoke.yaml).

## Fragments

`extends: [name, ...]` merges named fragments, which may extend others, before
the document itself. The Valkey components ship fragments, and experiments
loaded with `--experiment-dir` can extend them. Merge rules:

- `components` and `startup_overlays` merge by ID. The document's entries come
  first, in its order, and an entry with the same ID deep-merges onto the
  fragment's, so an experiment can override one config field of a fragment's
  component. The fragment's other entries follow.
- `inputs` and `metadata` deep-merge, so an experiment can change one input's
  default or bounds. A fragment's `metadata.generator` applies only to an
  experiment that declares its own.
- `required_events`, `required_evidence`, and `inputs.required` are unions.
- Every other field is replaced.

The Valkey components ship thirteen fragments:

| Fragment | Adds |
| --- | --- |
| `valkey.liveness` | the `liveness` oracle ([`valkey.oracle.liveness`](components.md#valkeyoracleliveness)) |
| `valkey.writes` | `valkey.liveness`; the `writes` workload ([`valkey.workload.key-value`](components.md#valkeyworkloadkey-value)); the `data` oracle ([`valkey.oracle.ledger`](components.md#valkeyoracleledger)) with required evidence `valkey.data.ledger-verified`; the inputs `stable_keys` (default 1000), `mutable_keys` (200), `mutation_rate` (200 per second), and `value_size` (64 bytes), with the ranges [random scenarios](#declaring-what-may-be-randomized) draw them from |
| `valkey.base` | `valkey.writes` and the `deployment-state` oracle ([`valkey.oracle.deployment-state`](components.md#valkeyoracledeployment-state)) |
| `valkey.history` | the `history` oracle ([`valkey.oracle.history`](components.md#valkeyoraclehistory)) on the `data` oracle's ledger and loss policy; extend it after `valkey.writes` or `valkey.base`. The chaos experiments extend it: `aof-durability`, `cluster-partition`, `forkless-save-crash`, `mixed-version-cluster`, `repl-compression-mixed-version`, `replication-chaos`, `replication-full-resync`, and `rolling-upgrade` |
| `valkey.drained-writes` | `valkey.writes`, whose workload performs exactly `operations` operations (input, default 2000) and publishes `writes.drained`; the `drained` action ([`valkey.action.wait-event`](components.md#valkeyactionwait-event)) that waits for it, with `writes.drained` a required event; random scenarios draw 1 to 3000 operations at 500 to 5000 per second |
| `valkey.reconnect-replicas` | the `drop-replica-links` action ([`valkey.action.command`](components.md#valkeyactioncommand), `CLIENT KILL TYPE replica` on the primaries), then the `links-up` poll (every replica's `master_link_status` is `up`, within 120 s), with `drop-replica-links.completed` a required event; fragments name what `drop-replica-links` follows |
| `valkey.psync` | `valkey.base` and a partial resync: writes for `disconnect_after_s` (input, default 1), then `valkey.reconnect-replicas` after `run-then-disconnect`, the `info` observer requiring `sync_partial_ok` to advance and `sync_full` not to, and the `logs` observer requiring both sides' PSYNC lines and forbidding a full resync; standalone or cluster with replicas |
| `valkey.full-sync` | `valkey.base` and a forced full sync: writes for `run_before_sync_s` (input, default 1), then `change-replid` (`DEBUG CHANGE-REPL-ID`, which the `full-sync` startup overlay enables) and `valkey.reconnect-replicas` |
| `valkey.forkless` | the `forkless-enabled` startup overlay on every instance; the `build` probe ([`valkey.capability.build-probe`](components.md#valkeycapabilitybuild-probe)) that the binary supports forkless saves; the input `key_save_delay_us` (default 500); random scenarios draw `value_size` up to 1024 and the compression pool |
| `valkey.compression` | the input `rdbcompression` (`no`, `yes`, `lzf`, `lz4`, `zstd`; default `yes`, which every build accepts) as the `rdb-compression` startup overlay on every instance; the `build` probe with `deployment_config: [rdbcompression, repl-compression]`, so a codec the build lacks is `SKIP_UNSUPPORTED` |
| `valkey.repl-compression` | `valkey.compression`; the inputs `primary_repl_compression` and `replica_repl_compression` (`no`, `yes`, `lz4`, `zstd`; default `lz4`) as overlays on the primaries and the replicas; `compressible` (default `true`) on the `writes` workload; the `compression` oracle ([`valkey.oracle.repl-compression`](components.md#valkeyoraclerepl-compression)) with required evidence `valkey.repl.compression-verified`; random scenarios draw 100 to 1000 `mutable_keys`, so every shard takes writes, and 1 to 3 replicas (standalone) or 1 to 2 (cluster) |
| `valkey.command-table` | the `commands` workload ([`valkey.workload.command-table`](components.md#valkeyworkloadcommand-table)) at the input `command_rate` (default 0, which sends none), and the `reply-schemas` oracle ([`valkey.oracle.reply-schema`](components.md#valkeyoraclereply-schema)), which holds its replies to Valkey's reply schemas; random scenarios keep the default, so a batch turns it on for an experiment with a base (`experiment: {inputs: {command_rate: 200}}`). `valkey.cluster-chaos`, `valkey.replication-chaos`, and `valkey.cluster-partition` extend it |
| `valkey.random` | only the server config pools of [random scenarios](#declaring-what-may-be-randomized) |
| `valkey.profiles` | only the [search profiles](#legal-space-and-search-profiles); no experiment extends it |

Every shipped Valkey experiment extends `valkey.liveness`. An experiment
loaded with `--experiment-dir` gets it only by extending it, directly or
through `valkey.writes` or `valkey.base`.

## The Valkey experiments

The fuzzer ships these experiments, in
`valkey_fuzzer/components/experiments/`. Every one also runs
`valkey.oracle.liveness`: at the end every instance must be running and
answer `PING`, except instances a chaos action killed on purpose and left
down, and no instance log or captured stdout and stderr may contain
a crash report, an assertion, or a sanitizer report. An instance that exited
unexpectedly fails the run, and so does a log line that one of the
experiment's `valkey.observer.log` observers forbids.

A host or build that lacks a feature an experiment names reports
`SKIP_UNSUPPORTED` before anything starts; a run missing an input the
operator gives, such as a second build, is `ERROR`. [`examples/valkey/`](../examples/valkey/) has a
scenario for each.

| Experiment | Deployment | Needs | What it checks |
| --- | --- | --- | --- |
| `valkey.aof-durability` | standalone | | After `SIGKILL` of the primary under writes and a restart from its append-only file (whose RDB base is in the `rdbcompression` codec), no acknowledged write is lost outside `loss_window_s`, and the reload is logged. `appendfsync always` requires `loss_window_s` 0, and `everysec` at least 2.5. |
| `valkey.bgsave-disk-full` | standalone | Linux, root, a `valkey.resource.tmpfs` resource holding the primary's data | On a full data filesystem, AOF writes and BGSAVE fail with errors, rejected writes are never applied, and after space is freed AOF writes recover and the next BGSAVE reloads. |
| `valkey.cluster-chaos` | cluster, replicas ≥ 1 | | After every wave of failovers with process kills, replication, cluster state, slot coverage, roles, views, test keys, and failover log lines validate. `run --random --experiment valkey.cluster-chaos` generates its scenarios; see [Cluster chaos and CI](cluster-chaos-and-ci.md). |
| `valkey.cluster-election-durability` | cluster, replicas ≥ 1 | Valkey 9.2.0 or later, with `cluster-config-save-behavior` | Kills a primary right after it grants a failover vote, restarts it, runs a second election, and checks one winner per epoch, one vote per primary per epoch, a quorum of votes for every winner, and no split primary; under `sync`, also that no vote left before it was saved ([TLA+ model](../formal/cluster-election/README.md)). |
| `valkey.cluster-partition` | cluster, replicas ≥ 1, `addressing: per_instance_loopback` | Linux, root, iptables | A primary cut off from the cluster bus is replaced by its replica and rejoins as a replica after the partition heals; no acknowledged write is lost, except writes the isolated primary itself acknowledged in the partition window. |
| `valkey.forkless-save` | standalone, cluster | a build with forkless BGSAVE | A forkless BGSAVE ran (INFO and log evidence), its RDB is in the `rdbcompression` codec and reloads, and live data matches the write ledger. |
| `valkey.forkless-save-cancel` | standalone, cluster | a build with forkless BGSAVE | `BGSAVE CANCEL` aborts a forkless save cleanly, and the next forkless save succeeds and reloads in the `rdbcompression` codec. |
| `valkey.forkless-save-churn` | standalone | a build with forkless BGSAVE | A forkless BGSAVE under `RENAME`s over existing keys of one name length (`rename_weight`), pipelined `LRANGE`/`LINDEX` reads of `list_keys` lists of `list_length` elements under `list_compress_depth` (`list_read_weight`, `list_read_batch`), and an `EXEC` without `MULTI` on a fresh connection as the save starts (rejected, nothing else): the save completes, its RDB reloads in the `rdbcompression` codec as a consistent cut with every list intact, and live data matches the ledger. With `auto_save` (a `save` setting such as `"1 1"`) and `churn_s`, the workload first runs `churn_s` seconds across a forkless save about every second, for races one save rarely hits; then the save points are turned off and the captured save runs. The three shapes of valkey-io/valkey#4604 the workload could not reach before. |
| `valkey.forkless-save-crash` | standalone, cluster | a build with forkless BGSAVE | After `SIGKILL` during a forkless save the primary restarts, a new forkless save succeeds in the `rdbcompression` codec, and no acknowledged write is lost, except writes the killed primary acknowledged within `loss_window_s` (2.5 s) of the kill. The save starts 3 s after the preload, so every preloaded key must survive. The instances run with `cluster-replica-no-failover yes`, so in cluster mode the restarted primary keeps its slots. |
| `valkey.forkless-save-on-replica` | standalone, replicas ≥ 1 | a build with forkless BGSAVE | Every replica runs a forkless BGSAVE under replicated writes; `sync_delay_s` after it starts the primary changes its replication ID and drops the replica links, so each replica full-syncs mid-save, loaded as `diskless_load` says (`swapdb` frees the old keyspace in the background); the primary's own saves run without `rdb-key-save-delay`, so the sync streams at full speed while the replicas' saves still scan. Every replica survives, relinks, and converges on the ledger, and the primary counts a full sync. A replicated write blocks on a key the save holds only with values over 512 bytes (`value_size`), and the swapdb path runs only with `diskless_load: swapdb`: the bug zoo's cases set them (valkey-io/valkey#4604 bugs 4 and 5). |
| `valkey.mixed-version-cluster` | cluster, replicas ≥ 1 | two different builds | A cluster that mixes two builds fails over under writes, and every instance runs its assigned binary. A graceful failover loses nothing; with `force_failover`, only the old primary's last writes may be lost. |
| `valkey.rdb-compression` | standalone | a build with streaming RDB compression | A BGSAVE under writes (fork or forkless, `bgsave_method`) and a quiesced final BGSAVE write an RDB in the `rdbcompression` codec, each reloads with the matching load path as a consistent cut of the writes, and after an AOF rewrite and a restart from `dump.rdb` (`appendonly: "no"`) or the AOF (`"yes"`, whose base RDB Valkey writes plain) the primary reloads in that codec (plain from the AOF) with no acknowledged write lost, and the deployment state validates. |
| `valkey.rdb-compression-compat` | standalone | the loading binary as an artifact (`load_binary`) | The RDB saved in the `rdbcompression` codec is refused cleanly (`load_expect: refused`, optionally logging `refused_log`) or loaded (`loads`, optionally with `load_config`) by another build. |
| `valkey.repl-compression-full-sync` | standalone or cluster, replicas ≥ 1 | a build with replication compression | After `DEBUG CHANGE-REPL-ID` and dropped links force a full sync under writes, every link and its full sync ran in the negotiated codec and the predicted transport (`repl_diskless_sync`, `repl_diskless_load`, `dual_channel`), compression shrank the stream by `min_ratio`, and every replica holds its primary's data. |
| `valkey.repl-compression-mixed-version` | cluster, replicas ≥ 1 | replicas on an older build without `repl-compression` (`instance_binaries.replica`, also the `old_binary` input) | Before and after a failover to an old replica, every link is plaintext and every full sync to an old replica is diskless; a newer replica loading from disk rewrites the RDB into its own codec. An old build with `repl-compression`, or two copies of one build, is `ERROR`. |
| `valkey.repl-compression-partial-resync` | standalone or cluster, replicas ≥ 1 | a build with replication compression | Dropped compressed links resume with PSYNC (no full sync), re-enable the negotiated codec, and shrink the stream by `min_ratio`. |
| `valkey.repl-compression-runtime-change` | standalone or cluster, replicas ≥ 1 | a build that accepts the values (zstd by default) | Three `CONFIG SET repl-compression` changes under writes (primaries, replicas, primaries) renegotiate the links through partial resyncs, never a full sync, and the final links match the final configuration with no acknowledged write lost. The changes must make a primary and a replica renegotiate. |
| `valkey.replication-chaos` | standalone, replicas ≥ 1 | partitions: Linux, root, iptables, `addressing: per_instance_loopback` | Seeded `waves` of kills, restarts, graceful (`FAILOVER TO`) and forced (`REPLICAOF NO ONE`, optionally with a split brain) promotions, repoints, stalls, backlog pressure, `repl-compression` flips, and partitions under writes ([`valkey.action.replication-chaos`](components.md#valkeyactionreplication-chaos)). After every wave the deployment is healed and must have one primary, every replica linked to it, equal offsets, and equal test keys; at the end every link uses its negotiated codec, and no acknowledged write is lost outside a forced promotion's window (below). |
| `valkey.replication-full-resync` | standalone or cluster, replicas ≥ 1 | | A replica stalled with `SIGSTOP` while writes overflow the backlog is refused a partial resync, completes a full resync, and converges. |
| `valkey.replication-partial-resync` | standalone or cluster, replicas ≥ 1 | | After the primary drops every replica link, the replicas resume with a partial resync (PSYNC), no full sync happens, and they converge. |
| `valkey.rolling-upgrade` | standalone or cluster, replicas ≥ 1 | two different builds, the target as an artifact | Every instance moves to the new build under writes, replicas first with graceful failovers, and no acknowledged write is lost. |
| `valkey.snapshot-equivalence` | standalone, cluster | | A fixed number of seeded operations, then a BGSAVE of the quiesced dataset that must reload in the `rdbcompression` codec; records dataset, RDB, and reply digests for [differential runs](cli.md#diff). |

An experiment can also compare one command sequence across two builds, the
build under test and its parent, with
[`valkey.action.commands`](components.md#valkeyactioncommands):
[`examples/valkey/experiments/commands-compare.yaml`](../examples/valkey/experiments/commands-compare.yaml)
tests the fix of valkey-io/valkey#4823 that way, and
[`examples/valkey/commands_compare.yaml`](../examples/valkey/commands_compare.yaml)
runs it, declaring the parent build as `artifacts.parent` and passing it as the
`parent` input.
[`commands-compare-cluster.yaml`](../examples/valkey/experiments/commands-compare-cluster.yaml)
runs each side on two cluster nodes (valkey-io/valkey#5140), and
[`commands-compare-restart.yaml`](../examples/valkey/experiments/commands-compare-restart.yaml)
restarts each server from the config file it rewrote (valkey-io/valkey#5176);
`commands_compare_cluster.yaml` and `commands_compare_restart.yaml` run them. The action starts both servers itself, so the deployment's
mode and instances do not matter; a sequence that must run under a fault the
experiment injects leaves `compare_with` out and runs on the deployment's
instances instead.

A change to one of Valkey's tools (`valkey-cli`, `valkey-benchmark`,
`valkey-check-rdb`, `valkey-check-aof`) compares the same way with
[`valkey.action.tools`](components.md#valkeyactiontools), which runs the tool
of each build:
[`examples/valkey/experiments/tools-compare.yaml`](../examples/valkey/experiments/tools-compare.yaml)
tests the fix of valkey-io/valkey#5177, and
[`examples/valkey/tools_compare.yaml`](../examples/valkey/tools_compare.yaml)
runs it.

A change to replication, failover or persistence is tested under a fault, judged
by oracles rather than by comparing two builds:
[`examples/valkey/experiments/replica-killed-under-writes.yaml`](../examples/valkey/experiments/replica-killed-under-writes.yaml)
kills a replica while the `valkey.base` workload writes and restarts it at once,
and the ledger, deployment-state and liveness oracles check that it resynced
with no acknowledged write lost;
[`examples/valkey/replica_killed_under_writes.yaml`](../examples/valkey/replica_killed_under_writes.yaml)
runs it.

Three differentials compare codecs:
[`differential_rdb_codecs.yaml`](../examples/valkey/differential_rdb_codecs.yaml) (`no`, `lz4`, `zstd`:
equal dataset, reloaded RDB, and reply digests; different `rdb_format.codec`),
[`differential_lzf.yaml`](../examples/valkey/differential_lzf.yaml) (LZF against none: equal digests
and codec, different `rdb_format.size`), and
[`differential_repl_codecs.yaml`](../examples/valkey/differential_repl_codecs.yaml) (a full sync in
`no`, `lz4`, `zstd`: each variant passes, with different `links.0.algo`; its writes run for a time, so
the variants' datasets are not compared).

On Valkey unstable a forkless BGSAVE ignores whole-stream `rdbcompression`, so `valkey.rdb-compression`
with `bgsave_method: forkless` and the forkless experiments fail with `lz4` or `zstd` ("expected codec
lz4, the RDB is plain") until forkless compression is merged; random runs can leave those codecs out
with `--exclude server_config.rdbcompression=lz4` (and `=zstd`).

In `valkey.replication-chaos` a graceful promotion loses nothing. A forced
promotion, or the heal's promotion after the primary was killed, may lose
only writes the old primary acknowledged after the new primary's last sync
point with it and before the old primary exited or was demoted (plus 1 second
for replies in flight). A write `WAIT` confirmed on every replica
(`wait_replicas`) is never lost, and writes the new primary acknowledged must
survive.

`valkey.cluster-election-durability` is the live counterpart of the TLA+
model in [`formal/cluster-election/`](../formal/cluster-election/README.md). Its batch reproduced the
sequence the model flags as risky under `best-effort` (a vote delivered before
it was saved, then a crash that loses it); no run produced a second winner in
one epoch.

## Declaring what may be randomized

An experiment opts in to [generated scenarios](cli.md#generated-scenarios-run---random)
with a `metadata.generator` block, which fragments merge into like the rest
of `metadata`. The block is the experiment's legal space; a
[search profile](#legal-space-and-search-profiles) only changes how often each
part of it is drawn:

```yaml
metadata:
  generator:
    hook: valkey.generator.tmpfs-data        # optional scenario hook
    deployment:
      mode: [standalone, cluster]            # default: every mode the experiment supports
      replicas_per_primary: {minimum: 1, maximum: 2}
      cluster: {primaries: {minimum: 3, maximum: 4}}   # drawn only in cluster mode
    inputs:
      value_size: {values: [1, 64, 4096]}
      loss_window_s: fixed
      waves: {hook: valkey.generator.waves}
    server_config: [valkey.pool.replication, valkey.pool.compression]
    reject:
      - {server_config.repl-diskless-sync: "no", inputs.expect_disk_sync: false}
```

| Rule | Draws |
| --- | --- |
| `fixed` | Nothing: an input keeps its default, a deployment field its provider default. |
| a value | That value. |
| `[a, b, c]` or `{values: [a, b, c]}` | One of them, the first and last twice as often as the others. |
| `{minimum: a, maximum: b}` | 40% of the time an edge (a, a+1, b-1, b, and 0 or 1 when in range), otherwise uniform, or log-uniform when b/a exceeds 1000. Integers for an `integer` input and for deployment fields. |
| `{hook: <id>}` | Whatever the generator hook returns (inputs only). |

An input the block does not name is drawn when it declares an `enum`
(uniformly), is a `boolean`, or has both `minimum` and `maximum`; every other
input keeps its default. A block's bounds must lie within the input's, and
its values must be valid for it. Deployment fields under a key named after a
mode (`cluster:`, `standalone:`) are drawn only in that mode.

`server_config` lists pools. A pool maps server config keys to values; the
generator draws each key uniformly into `deployment.server_config`, except
keys one of the experiment's startup overlays sets (the overlay would win).
A pool with `scope: [deployment, role]` also draws whether the primary and
the replicas each get their own values (`deployment.instance_configs`). The
`valkey.random` fragment defines `valkey.pool.persistence`
(`appendonly`, `appendfsync`, `aof-use-rdb-preamble`),
`valkey.pool.replication` (`repl-diskless-sync`, `repl-diskless-load`,
`dual-channel-replication-enabled`, `repl-diskless-sync-delay`), and
`valkey.pool.compression` (`rdbcompression`, `repl-compression`, scoped). An
experiment can define its own under `metadata.generator.pools`. An experiment
that takes a codec as an input (`valkey.compression`) draws the input instead
of the pool key, and its oracles expect that codec; `--exclude
server_config.<key>=<value>` also excludes the value from an input an overlay
writes into `<key>`. Every experiment that can draw a codec probes its builds
with `deployment_config` naming both codec settings, so `zstd` on a build
without it is `SKIP_UNSUPPORTED`.

A pool with `default: true` is drawn by every experiment without being
listed, after the pools it lists; a key a listed pool or a startup overlay
already sets is not drawn again. The default pools change how the server runs,
never what it stores or replies, so every oracle stays valid under them; all
three are scoped:

| Pool | Keys |
| --- | --- |
| `valkey.pool.io-threads` | `io-threads` (1, 2, 4), `io-threads-always-active` and `events-per-io-thread 0` (which keep the threads busy at fuzzing load: on 9.1 and later they otherwise start only once the main thread is 30% busy), `io-threads-do-reads`, the copy-avoiding replies (`min-io-threads-avoid-copy-reply`, `min-string-size-avoid-copy-reply`, `-threaded`), `prefetch-batch-max-size` |
| `valkey.pool.memory` | every `maxmemory-policy`, each `lazyfree-lazy-*` and `replica-lazy-flush`, `activedefrag` with thresholds low enough that it runs on a small dataset |
| `valkey.pool.encodings` | the `hash`, `set`, `zset`, `list` and `stream` encoding thresholds and `list-compress-depth`: a value that never or soon leaves the compact encoding (converted as the workload writes), and Valkey's default |

The eviction policies run under the deployment's `maxmemory` (the local
provider's 500mb), which no experiment's dataset reaches, so they change what
each access records (an LRU clock or an LFU counter) and what an RDB holds per
key (`IDLE` or `FREQ`), but evict nothing. No experiment draws a `maxmemory` it
reaches: eviction deletes keys that `valkey.oracle.ledger`, the snapshot
oracles, and the data-consistency keys of `valkey.oracle.deployment-state`
expect to find. A build that lacks one of the hidden IO-thread configs, or
refuses `activedefrag yes` (no jemalloc defrag hints, as a `MALLOC=libc` build),
starts without it, or with `activedefrag no`
(`UNOBSERVABLE_CONFIGS` in `valkey_fuzzer/deployment/local/server_config.py`);
the plan records the value drawn.

Always-active IO threads spin through every event: on unstable, a primary and
two replicas under 100 writes a second take 1.2 cores with `io-threads 2`, 3.7
with 4 and 7.3 with 8, and six idle cluster nodes 1.9 cores with 2 and 10.7
with 8, since the cluster bus keeps waking them (0.15 with 8 without
always-active). So the pool draws at most four, and `valkey.cluster-chaos`,
whose clusters reach 96 nodes, lists `valkey.pool.cluster-io-threads`
(`io-threads` 1 or 2, never always-active), which wins over the default
pool's keys. A batch with the default pools needs fewer workers per core than
one without them; on a host with few cores per worker (a four-core runner, a
soak host with as many workers as cores), `--exclude
server_config.io-threads=4` keeps a server to one always-active IO thread.

The `valkey.writes` fragment bounds the workload inputs of every experiment
that opts in: `stable_keys` 0-2000, `mutable_keys` 1-1000, `mutation_rate`
1-1000, `value_size` 1, 64, 1024, or 16384. An experiment overrides a range
in the same form (`{minimum, maximum}` or `{values: [...]}`), and must when
it narrows the input below it.

`reject` lists combinations that must not run together: a candidate whose
compiled plan holds every `path: value` of one rule is redrawn. Paths are
those of `--exclude`.

Each dimension (`deployment.<field>`, `inputs.<name>`,
`server_config.<key>`, `instance_configs.<role>.<key>`) draws from its own
stream, `sha256("gen", seed, experiment, dimension, attempt)`, so adding an
input or a pool key never changes another dimension's draw for the same seed,
and one seed draws differently for each experiment. Without `--experiment`
the experiment itself comes from `sha256("gen", seed, "experiment", 0)`.

### Generator hooks

A hook is a function `hook(rng, scenario)` registered in the catalog's
generator registry as a `GeneratorRegistration`
(`<namespace>.generator.<name>`; the Valkey hooks are `GENERATORS` in
`valkey_fuzzer/components/generators.py`); it is
not a component and never appears in a plan. `valkey-fuzzer list generators`
prints the installed hooks. An
input hook returns the input's value; the scenario hook (`hook:` at the top
of the block) returns a partial scenario merged over the draws. Both get a
copy of the scenario drawn so far, base included; the base then overrides
what they return. The Valkey hooks:

| Hook | Used by | Draws |
| --- | --- | --- |
| `valkey.generator.aof-loss-window` | `valkey.aof-durability` | `loss_window_s`: the least the drawn `appendfsync` allows, 0 for `always` and `no`, 2.5 for `everysec`. |
| `valkey.generator.waves` | `valkey.cluster-chaos`, `valkey.replication-chaos` | One to four waves of one to three operations of the drawn deployment mode, with kills that always have a target. Standalone: a `restart` after half the kills, partitions only with per-instance loopback addresses, repoints to another replica only with two replicas or more, at most `replicas_per_primary` kills per wave, only replicas after the first. Cluster: one to three failovers of distinct shards per wave, four in five with a kill before, with, or after it, and fewer kills in the run than there are replicas. |
| `valkey.generator.tmpfs-data` | `valkey.bgsave-disk-full` | A 16, 32, or 64 MB tmpfs resource holding the primary's data, passed as `disk`. |
| `valkey.generator.upgrade-target` | `valkey.rolling-upgrade` | `to_binary` as the deployment's own build (the two-build probe then skips the run) unless the scenario sets it. |
| `valkey.generator.old-build` | `valkey.repl-compression-mixed-version` | `old_binary` the same way. |
| `valkey.generator.rdb-loader` | `valkey.rdb-compression-compat` | `load_binary` as the deployment's own build, with `load_expect: loads`, unless the scenario sets it. |
| `valkey.generator.repl-compression-changes` | `valkey.repl-compression-runtime-change` | The three runtime changes, among those after which a primary and a replica both renegotiate from the drawn initial values. |
| `valkey.generator.wait-replicas` | `valkey.replication-chaos` | `wait_replicas` from 0 to the drawn `replicas_per_primary`. |
| `valkey.generator.compression-ratio` | `valkey.repl-compression-full-sync`, `-partial-resync` | `min_ratio`: 1.5 for compressible values, 0 (off) otherwise. |

### What the Valkey experiments randomize

Inputs not listed use the `valkey.writes` ranges.

| Experiment | Deployment | Inputs | Pools |
| --- | --- | --- | --- |
| `valkey.aof-durability` | standalone, 0-2 replicas | `appendfsync`, `rdbcompression`, rate up to 2000, `run_before_crash_s` 0.5-5, `run_after_crash_s` 0-3, `loss_window_s` 0 or 2.5 (from `valkey.generator.aof-loss-window`) | replication |
| `valkey.bgsave-disk-full` | standalone, tmpfs 16-64 MB | stable keys 100-2000, mutable keys up to 500, rate 50-1000, value size 64 B-1 kB | |
| `valkey.cluster-chaos` | 3-16 primaries, 1-5 replicas | `waves` | |
| `valkey.cluster-election-durability` | 3-5 primaries, 1-2 replicas | `save_behavior`, `election`, `second_election`, `shard` 0-2, `vote_occurrence` 1-2, restart and second-election delays | |
| `valkey.cluster-partition` | 3-5 primaries, 1 replica, per-instance loopback | `hold_after_failover_s` 0-3 | |
| `valkey.forkless-save` | standalone or cluster, 0-2 replicas | `rdbcompression`, stable keys 2000-6000, `key_save_delay_us` 300-1000, rate up to 2000, value size up to 1 kB, `compressible` | compression |
| `valkey.forkless-save-cancel` | standalone or cluster, 0-2 replicas | `rdbcompression`, stable keys 3000-8000, mutable keys up to 500, `key_save_delay_us` 600-1500, `cancel_delay_s` 0-0.3, value size up to 1 kB | compression |
| `valkey.forkless-save-churn` | standalone, 0-2 replicas | `rdbcompression`, stable keys 2000-6000, mutable keys 2-1000, `key_save_delay_us` 300-2000, rate up to 2000, value size up to 1 kB, `compressible`, `rename_weight` 0-1, `list_keys` 1-2, `list_length` 1000-100000, `list_read_weight` 0-1, `list_read_batch` 1-50, `list_compress_depth` 1 or 2; `churn_s` stays 0 | compression |
| `valkey.forkless-save-crash` | standalone 0-2 or cluster 0-1 replicas | `rdbcompression`, stable keys 3000-8000, mutable keys up to 500, `key_save_delay_us` 600-1500, `kill_delay_s` 0-0.3, value size up to 1 kB | compression |
| `valkey.forkless-save-on-replica` | standalone, 1-2 replicas | `rdbcompression`, stable keys 2000-20000, `key_save_delay_us` 200-1000, rate up to 1000, `sync_delay_s` 0-0.3; `value_size` (64) and `diskless_load` (`disabled`) stay fixed | compression |
| `valkey.mixed-version-cluster` | 3 primaries, 1-2 replicas | `failover_shard` 0-2, `force_failover` | |
| `valkey.rdb-compression` | standalone, 0-2 replicas | `rdbcompression`, `bgsave_method`, `appendonly`, `compressible`, `operations` 1-3000, rate 500-5000 | replication, compression |
| `valkey.rdb-compression-compat` | standalone | `rdbcompression`, `compressible`, `operations` 1-3000, rate 500-5000; the loader from `valkey.generator.rdb-loader` | |
| `valkey.repl-compression-full-sync` | standalone 1-3 or cluster 1-2 replicas | every codec and transport input, stable keys 500-2000, mutable keys 100-1000, rate 200-2000, value size 256 B-16 kB, `run_before_sync_s` and `run_after_sync_s` 0.5-3 | |
| `valkey.repl-compression-mixed-version` | 3 primaries, 1-2 replicas | `rdbcompression`, `primary_repl_compression`, `failover_shard` 0-2, `run_before_sync_s` 0.5-2 | |
| `valkey.repl-compression-partial-resync` | standalone 1-3 or cluster 1-2 replicas | the codecs, `compressible`, mutable keys 100-1000, rate 100-2000, value size 256 B-1 kB, `disconnect_after_s` and `run_after_resync_s` 0.5-3 | replication |
| `valkey.repl-compression-runtime-change` | standalone 1-3 or cluster 1-2 replicas | the codecs and their changes, `compressible`, mutable keys 100-1000, `change_interval_s` 3-5 | |
| `valkey.replication-chaos` | standalone, 1-4 replicas | `waves`, `wait_replicas`, `rdbcompression`, stable keys up to 5000, rate 50-1000, value size 16 B-16 kB | compression |
| `valkey.replication-full-resync` | standalone 1-3 or cluster 1-2 replicas | rate 1000-3000, mutable keys 100-1000, value size 16 or 64 kB, `pause_s` 3-5 | replication, compression |
| `valkey.replication-partial-resync` | standalone 1-3 or cluster 1-2 replicas | rate up to 2000, value size up to 1 kB, `disconnect_after_s` 0.5-3 | replication, compression |
| `valkey.rolling-upgrade` | standalone or cluster, 1-2 replicas | | |
| `valkey.snapshot-equivalence` | standalone or cluster, 0-2 replicas | `rdbcompression`, `operations` 1-3000, rate 500-5000, value size up to 8 kB | persistence, replication, compression |

Durability windows (`loss_window_s`, `loss_window_after_s`) are never drawn
wider than the configuration needs: a wider window would only loosen what
the oracle checks. They stay fixed, except `valkey.aof-durability`'s, which
its hook sets to the least the drawn `appendfsync` allows. Ranges keep each
run to a few minutes on one host and keep each experiment's own preconditions
(a save that outlasts the cancel or kill delay, writes that overflow the
output limit during a stall, a backlog that holds a disconnect's writes, an
append-only file small enough that no rewrite child refuses a BGSAVE).
`valkey.mixed-version-cluster`, `valkey.rolling-upgrade`, and
`valkey.repl-compression-mixed-version` need two builds, which only a base can
name; without one their runs are `ERROR` (exclude them, or pass the base).

## Legal space and search profiles

An experiment's `metadata.generator` block is its legal space; a search
profile only changes how hard that space is sampled and the batch budgets
(see [Legal space and search profiles](contracts.md#legal-space-and-search-profiles)).
Profiles live in `metadata.generator.profiles` of a fragment no experiment
extends (for the Valkey experiments,
`valkey_fuzzer/components/fragments/valkey.profiles.yaml`), and one profile
serves every experiment. `run --seed N --profile NAME` and
`batch --profile NAME` apply one, and `reproducer.generated.profile` records
it.

```yaml
metadata:
  generator:
    profiles:
      pr-quick:
        description: Small deployments, edge-heavy values, and short runs, for a pull request.
        edge_probability: 0.6                          # default 0.4
        ranges:                                        # deployment.<field> or inputs.<name>
          deployment.replicas_per_primary: {maximum: 1}
          inputs.operations: {maximum: 1000}
        weights:                                       # deployment., inputs., or server_config. paths
          deployment.mode: {standalone: 3}
        budget: {seeds: 3, timeout_s: 600}
```

| Field | Meaning |
| --- | --- |
| `description` | One line. |
| `edge_probability` | Share of `{minimum, maximum}` draws that are range edges, 0 to 1. |
| `ranges` | A narrower `{minimum, maximum}` (either may be left out) for a numeric range rule. Each bound is clamped into the experiment's own range, so a profile range can only shrink one, never widen it or leave it empty: `{maximum: 1000}` on an experiment whose range is 3000-8000 draws 3000. |
| `weights` | A weight greater than zero for listed values. A value the experiment does not list is ignored, so a weight never adds one. |
| `budget` | `timeout_s`, the batch's run deadline, and `seeds`, the seeds per experiment of a batch's `random` source that names none. |

A path an experiment does not draw is ignored. A profile cannot touch an
input the block fixes or a hook draws, and any other field (`reject`,
`hook`, `pools`, `inputs`, `server_config`, components or oracle config) is
rejected when the profile is loaded. The same seed and profile always draw
the same scenario.

The Valkey profiles:

| Profile | Sampling | Budget |
| --- | --- | --- |
| `pr-quick` | Edges 60% of the time; at most 3 primaries and 1 replica; at most 1000 operations and stable keys, 200 mutable keys, and a rate of 1000; standalone 3 times as often as cluster. | 3 seeds, 600 s per run |
| `nightly-soak` | Each experiment's whole legal space as declared. | 50 seeds, 1800 s per run |
| `adversarial` | Edges 90% of the time. Fault-point enumeration and schedules biased by earlier failures are not built yet. | 20 seeds, 1800 s per run |
| `candidate` | Nothing is drawn: for given scenarios (AI or model proposals), which run as written. | 1200 s per run |
