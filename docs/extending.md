# Adding tests

This guide is for people who add tests: a data-only experiment, a new
component, or a provider. The Valkey components and experiments are part of
the fuzzer, in `valkey_fuzzer/components/`. The formats are in
[Scenarios and experiments](scenarios-and-experiments.md), the existing
components in [Valkey components](components.md), and the framework's
structure in [Architecture](architecture.md).

- [Choose what to write](#choose-what-to-write)
- [Design the test](#design-the-test)
- [Write a data-only experiment](#write-a-data-only-experiment)
- [Write a component](#write-a-component)
- [The local deployment service](#the-local-deployment-service)
- [Write a provider](#write-a-provider)
- [Register components and experiments](#register-components-and-experiments)
- [Testing](#testing)

## Choose what to write

| You need | Write | Python |
| --- | --- | --- |
| a new combination of existing workloads, actions, faults, observers, and oracles | a [data-only experiment](#write-a-data-only-experiment) | none |
| behavior no component has (a new command sequence, a new fault, a new invariant) | a [component](#write-a-component) in `valkey_fuzzer/components/` | yes |
| an input that must be acquired or set up before the run (a binary, a filesystem, a service) | an artifact or resource [provider](#write-a-provider) | yes |
| a system under test the local deployment cannot start | a deployment [provider](#write-a-provider) | yes |

A test goes in `valkey_fuzzer/components/`: the experiment in
`valkey_fuzzer/components/experiments/`, its components in the module for
their role, and an example scenario in `examples/valkey/`. A data-only
experiment can also stay in a directory of its own, which `--experiment-dir`
loads.

## Design the test

Answer these before you write YAML:

1. **Support.** Which builds, hosts, and deployments can run it? Encode build
   and host needs as capability probes that name each feature, so a target
   that lacks one is `SKIP_UNSUPPORTED`, not `FAIL` (any other setup error is
   `ERROR`), and deployment needs as `deployment:` rules, which reject an
   unsuitable scenario when it compiles.
2. **Configuration.** Which server settings, inputs, artifacts, and
   resources does it need?
3. **Transition.** Which command, workload, or fault exercises the code you
   want to test?
4. **Path evidence.** What independent signal shows that code ran: an INFO
   field, a counter delta, a log line? Configuration alone does not show it.
   Mark that evidence `required: true`: without it the run is `ERROR`,
   because it did not test what it claims.
5. **Oracles.** Which externally visible invariant establishes correctness:
   no acknowledged write lost, a clean reload, a converged deployment? An
   expectation about behaviour that an observer records, such as a replica
   logging a new sync, is a `valkey.oracle.evidence` rule, never required
   evidence: observers only record, and only actions and oracles fail a run.

Then plan a **negative control**: a variant where the feature is configured
but not exercised, or the oracle gets a known-bad input, and the experiment
must fail. It shows the evidence and oracles can detect a problem.

## Write a data-only experiment

A data-only experiment uses only registered components and needs no Python.
This one restarts a replica under writes and requires that the replica syncs
again and that no acknowledged write is lost:

```yaml
# experiments/example.replica-restart.yaml
extends: [valkey.base]
id: example.replica-restart
deployment:
  modes: [standalone]
  replicas_per_primary: {minimum: 1}
metadata:
  description: Restart a replica under writes and check that it catches up
inputs:
  properties:
    run_before_s: {type: number, minimum: 0, maximum: 60, default: 1}
components:
  - use: valkey.observer.log
    id: logs
    config:
      instances: replicas
      expect:
        - evidence_id: example.replica.synced-logged
          pattern: "sync: Finished with success|Successful partial resynchronization"

  - use: valkey.fault.process-signal
    id: restart-replica
    config:
      when_event: run-before.completed
      instance: replica
      signal: SIGTERM
      restart_mode: immediate

  - use: valkey.action.sleep
    id: run-before
    depends_on: [writes, logs, restart-replica]
    config: {seconds: {$ref: inputs.run_before_s}}

  - use: valkey.action.poll
    id: link-up
    depends_on: [run-before]
    config:
      instances: replicas
      section: replication
      field: master_link_status
      predicate: {equals: up}
      timeout_s: 60

  - use: valkey.oracle.evidence
    id: synced
    depends_on: [logs]
    config:
      require:
        - {evidence_id: example.replica.synced-logged, source: logs}
required_events: [restart-replica.recovered]
required_evidence:
  - {evidence_id: valkey.fault.injected, source: restart-replica}
```

How the pieces fit:

- `extends: [valkey.base]` adds the `writes` workload, the `data` ledger
  oracle, the `deployment-state` oracle, and the oracles of
  `valkey.liveness` (`liveness`, `error-counters`, `quiet-end`). The
  actions depend on `writes`, so writes continue through the restart.
- `deployment.replicas_per_primary: {minimum: 1}` rejects a scenario without
  a replica at compile time.
- The fault's trigger is installed before the actions start, and the fault
  fires inside the dispatch of `run-before.completed`, so the restart lands
  at the same point in every run. `restart_mode: immediate` restarts the
  replica before `link-up` runs.
- The `synced` oracle fails the run if the replica never logs a new sync:
  that is behaviour, which the `logs` observer only records. It depends on
  `logs`, so if the observer could not read the log, its `FAIL` is not
  trusted.
- `required_events` and `required_evidence` are the path evidence: without
  the restart the run did not test what it claims, and is `ERROR`.

A scenario for it:

```yaml
# replica_restart.yaml
scenario_id: replica-restart
seed: 5
experiment:
  id: example.replica-restart
  inputs: {run_before_s: 2}
deployment:
  mode: standalone
  replicas_per_primary: 1
  base_port: 7700
```

Load the experiment from its directory with `--experiment-dir`:

```bash
valkey-fuzzer validate replica_restart.yaml --experiment-dir experiments --verbose
valkey-fuzzer run replica_restart.yaml --experiment-dir experiments --verbose
```

`validate --verbose` lists every component of the execution plan, which
shows what the fragments added. An experiment loaded from a directory gets
the `liveness`, `error-counters`, and `quiet-end` oracles only through its
fragments; extend `valkey.liveness` directly if you extend nothing else.
Without it, a `valkey.observer.log` `forbidden` match is recorded but fails
nothing.

Faults that must fire at an exact moment can trigger on a log line: a
`valkey.observer.log-trigger` observer publishes an event for every matching
line, and a fault with `instance: event` targets the instance that logged it.
`valkey.cluster-election-durability` uses this to kill a primary right after
its "Failover auth granted" line.

## Write a component

A component is a Python class with the lifecycle methods of its role. Each
method returns a `ComponentResult`, usually built with `make_result`.

| Role | Methods | Runs |
| --- | --- | --- |
| capability | `probe(context, spec)` | before the deployment starts; `SKIP` means unsupported |
| workload | `start(context, spec)`, `stop(context, spec)` | from start until teardown |
| observer | `start(context, spec)`, `stop(context, spec)` | from start until teardown |
| fault | `inject(context, spec)`, `recover(context, spec)` | `inject` installs the trigger and the fault is injected when it fires; `recover` undoes it at teardown and fails if it never fired |
| action | `run(context, spec)` | once, in dependency order |
| oracle | `evaluate(context, spec, evidence)` | after teardown, with all evidence recorded so far |

This action increments a key and records the result; the oracle checks it:

```python
from valkey_fuzzer.declarations import Declarations, EvidenceDeclaration
from valkey_fuzzer.protocols import ComponentRole
from valkey_fuzzer.registrations import ComponentRegistration
from valkey_fuzzer.results import ComponentStatus, evidence_of, make_evidence, make_result
from valkey_fuzzer.schema import ConfigField, ConfigSchema

TOUCH_CONFIG = ConfigSchema({"key": ConfigField(str), "count": ConfigField(int, default=1, minimum=1)})
CHECK_CONFIG = ConfigSchema({"expect": ConfigField(int, minimum=1)})
VALUE = ConfigSchema({"value": ConfigField(int)})
# What Touch publishes and emits, with schemas; the runner rejects anything else.
TOUCH_DECLARES = Declarations(
    events={"{id}.touched": VALUE},
    evidence={"example.touched": EvidenceDeclaration(ConfigField(dict, schema=VALUE))},
)


class Touch:
    def run(self, context, spec):
        config = spec.config  # checked, with defaults filled in, by the compiler
        deployment = context.services["deployment"]["valkey.deployment"]
        client = deployment.new_client("valkey-0")
        try:
            for _ in range(config["count"]):
                value = client.incr(config["key"])
        finally:
            client.close()
        context.event_bus.publish(f"{spec.id}.touched", source=spec.id, payload={"value": value})
        evidence = make_evidence(spec, "example.touched", "action.completed", {"value": value})
        return make_result(spec, ComponentStatus.PASS, f"{config['key']} is {value}", evidence=[evidence])


class CheckTouched:
    def evaluate(self, context, spec, evidence):
        seen = [item.value["value"] for item in evidence_of(evidence, "example.touched")]
        if seen and seen[-1] == spec.config["expect"]:
            return make_result(spec, ComponentStatus.PASS, f"value is {seen[-1]}")
        return make_result(spec, ComponentStatus.FAIL, f"expected {spec.config['expect']}, saw {seen}")


REGISTRATIONS = (
    ComponentRegistration(
        "example.action.touch", ComponentRole.ACTION, Touch, config=TOUCH_CONFIG, declares=TOUCH_DECLARES
    ),
    ComponentRegistration(
        "example.oracle.touched", ComponentRole.ORACLE, CheckTouched, config=CHECK_CONFIG, declares=Declarations()
    ),
)
```

A `ComponentRegistration` has:

| Field | Meaning |
| --- | --- |
| `component_id` | `<namespace>.<role>.<name>`: lowercase segments, words joined by hyphens, and the middle segment equal to the role. The catalog rejects anything else. |
| `role`, `factory` | The `ComponentRole`, and a zero-argument callable that builds the component. |
| `config` | A `ConfigSchema` of `ConfigField`s. The compiler checks the config with it, rejects unknown and missing fields, fills in defaults, and records the result in the plan; the component reads `spec.config`. Without a schema the config is taken as written. |
| `check` | Optional `check(config, spec, plan)` for cross-field rules that need the resolved config or the plan; raise `ConfigError` to reject. |
| `deployment_modes` | Optional tuple of the deployment modes the component supports. |
| `required_evidence` | Optional `required_evidence(spec)` returning the evidence IDs the config marks as required; the compiler adds them with the component as their source. |
| `references_evidence` | Optional `references_evidence(spec)` returning the `(evidence_id, source)` pairs the config names without requiring them (`source` `None`: any component); the compiler rejects a pair that its source, or no component, declares, as it does for required evidence. |
| `declares` | Required: the events it publishes, the evidence it emits, and the state it changes ([below](#declare-what-a-component-emits-and-changes)); `Declarations()` for a component that does none of these. A registration without it is rejected. |

`ConfigField(types, default=..., required=..., choices=..., minimum=...,
maximum=..., allow_none=..., reference=..., reference_kinds=..., schema=...,
items=..., variants=...)` describes one field: `float` also accepts integers,
`list` accepts tuples, and `dict` any mapping; a field without a default is
required unless `required=False`; `schema` checks a nested mapping, `items`
each list item, and `variants=(tag, {value: ConfigSchema})` a mapping whose
`tag` field picks its schema (the wave operations of the chaos actions are
one, tagged by `op`). Only a field with `reference=True` may hold an
`{$ref: artifacts.<id>.<output>}` or `resources` reference.

Rules:

- **Factories** take no arguments and build only in-memory state. Do external
  work in the lifecycle methods.
- **`check` is pure.** It runs at compile time, when reference fields may
  still hold unresolved references, and again on the executable plan.
- **Statuses.** Only actions and oracles judge the system under test: return
  `FAIL` from them when the system misbehaved. Return `ERROR` when the harness
  could not do its job. A `FAIL` from any other role makes the run `ERROR`
  (see [Outcomes and verdicts](contracts.md#outcomes-and-verdicts)). An
  observer that could not read the server when an expectation needed it
  (the server did not answer, a log could not be read) returns `ERROR`, so
  a failure that depends on it is not trusted. An oracle that reports a
  server that crashed or stopped answering returns
  `make_result(..., crash=True)`, so its `FAIL` stands after a failed
  teardown.
- **Evidence and results** belong to their component: `make_evidence(spec,
  ...)` and `make_result(spec, ...)` set the source. Evidence that names an
  event sequence must name a committed event.
- **Declare everything** a component publishes, emits, and changes, and own
  every change it will undo (see the next two sections).
- **Random choices** come from `context.rng(spec.id, stream)`, so a seed
  repeats them.
- **Report data.** Summaries, evidence, diagnostics, and event payloads go
  into reports. Do not put secrets in them.
- **Crashes.** A component that finds that the system under test crashed
  returns `FAIL` with `crashes` in its diagnostics: a mapping (or list) of
  crash causes, such as an assertion's `file:line 'condition'`. The
  [failure signature](cli.md#minimize) of such a run is its causes alone, so
  one crash groups as one failure whatever else failed after it.

### Declare what a component emits and changes

`Declarations(events=..., evidence=..., changes=...)`:

| Field | Holds |
| --- | --- |
| `events` | `{event name: ConfigSchema}`: every event the component publishes, besides the runner's `<id>.started`, `.completed`, `.failed` (and `.stopping`, `.stopped`, `.stop-failed` for roles with a teardown), with the schema of its payload. |
| `evidence` | `{evidence ID: EvidenceDeclaration(value, many=False)}`: every evidence ID it emits, with a `ConfigField` for its value. Without `many`, the ID is a single value: one item per component use and run. |
| `changes` | The kinds of host or server state it changes (`process.stopped`, `network.partition`, ...), each owned by an [undo obligation](#own-every-state-change). |

A name may hold `{id}` (the component use's `id`) or `{config.<path>}`
(every string a config path names; `rules[].publish` steps into each list
item), so a component whose config names its events declares them. What the
runner does with anything undeclared or malformed is in
[Evidence contract](contracts.md#evidence-contract).
`valkey-fuzzer list components --format json` shows each component's
declarations with their schemas.

### Own every state change

A component that changes host or server state and will undo the change (a
stopped or paused process, a partition, a filled disk, a setting it
restores, a private server) opens an obligation the moment it applies it,
and discharges it when it undoes it:

```python
paused = context.obligations.open(spec, "process.paused", instance_id, lambda: resume(instance_id))
...
paused.discharge()  # runs the undo; a no-op once it succeeded, raises if it fails
```

The undo must be idempotent: a failed one leaves the obligation open and is
called again. Discharge at the latest in `recover` or `stop` (an action, in
`run`); whatever the runner has to undo for you makes the run `ERROR` (see
[Fault ownership](contracts.md#fault-ownership)).

### The run context

| Attribute | Meaning |
| --- | --- |
| `context.plan` | The executable plan, with references resolved. |
| `context.event_bus` | `publish(name, source=, payload=)`, `subscribe(name, handler)`, `unsubscribe`, and `committed_history(name)`. |
| `context.rng(component_id, stream)` | A random stream derived from the seed; it continues across calls in one run. |
| `context.services["deployment"]` | The provisioned deployment's services. The local deployment publishes [one service](#the-local-deployment-service) under `"valkey.deployment"`. |
| `context.artifact_dir` | This run's own folder: its report folder when a command runs it, else a new `<artifact root>/runs/<UTC time>-<scenario_id>-<suffix>/`. `component_artifact_dir(context, spec)` returns this component's folder in it, `components/<component id>/`. |
| `context.deadline` | This call's deadline (monotonic clock), or `None`: the earlier of the run's and the component's `timeout_s`, and in `stop` and `recover` the `timeout_s` window alone. A thread the component starts sees the run's deadline. |
| `context.cancelled`, `context.sleep(seconds)` | Cancellation; see below. |
| `context.obligations` | The run's undo obligations: `open(spec, kind, target, undo)` returns one to `discharge()`. |

Event dispatch is synchronous: `publish` returns after every subscriber ran,
and a publish from inside a subscriber is queued and returns at once. A
subscriber that raises fails the publish.

### Deadlines and cancellation

A call that outlives its deadline is cancelled (see
[Deadlines and cancellation](contracts.md#deadlines-and-cancellation)). A
cooperative component:

- waits with `context.sleep(seconds)`, never `time.sleep`; it returns `False`
  as soon as the call is cancelled;
- checks `context.cancelled` in long loops and returns promptly once it is
  true (the result is discarded);
- bounds its own waits by `context.deadline`;
- runs work it hands to its own threads (a thread pool) under a copy of the
  call's context, `pool.submit(contextvars.copy_context().run, work)`, so
  those threads share the call's deadline and cancellation; any other thread
  sees the run's deadline and is never cancelled.

## The local deployment service

The local deployment publishes `context.services["deployment"]["valkey.deployment"]`.
Instance IDs are `valkey-N`.

| Group | Members |
| --- | --- |
| instances | `mode`, `deployment_id`, `valkey_sha`; `instances` (each with `instance_id`, `role`, `shard_id`, `host`, `port`, `bus_port`, `slot_start`, `slot_end`, `primary_id`, `cluster_node_id`), `instance(id)` |
| clients | `execute_command(id, *args)`, `new_client(id, timeout=5.0)`, and in cluster mode `cluster_client(timeout=5.0)`; every client is closed at cleanup |
| processes | `pid(id)`, `started_at(id)`, `is_alive(id)`, `send_signal(id, signum)`, `wait_exit(id, timeout=10.0)`, `restart(id, wait_ready=True)` |
| topology | `replicate(id, primary_id)` (standalone): record the instance as a replica of `primary_id` (a primary with `None`) and send `REPLICAOF` if it runs, so a later `restart` keeps the role |
| files | `log_file(id)` and `process_output_file(id)` (its stdout and stderr), both written by this deployment alone, and `data_dir(id)` |
| binaries | `binary(id)`, `set_binary(id, binary)` (used from the next restart) |
| roles | `roles()` (each reachable instance's live role), `primaries()`, `replicas()`, `refresh_roles()`, `live_instances()` |
| cluster | `failover(shard_id, force=, timeout=, stopped_instances=, rng=)`, `seed_slots(keys_per_slot, seed=)` |
| checks | `state_check(validation, seed=)` (as `valkey.oracle.deployment-state` and the chaos actions use it), which returns a check with `write_test_keys()`, `load_test_keys(keys)`, `mark_stopped(id, role, shard=)` (the live shard it served when stopped), and `validate(operations)` |

`restart` keeps the instance's address, binary (unless `set_binary` changed
it), data directory, and resolved config; a replica in standalone mode
restarts with `--replicaof` so it never comes up as a writable primary.

The service's own waits (a failover settling, a state check's waits and
retries, a restart becoming ready, `wait_exit`) end at the calling component's deadline or
cancellation by raising, so the run records `ERROR` instead of reporting that
Valkey did not settle. Cleanup waits at most 30 seconds for operations still
running.

Components parse what Valkey replies with the deployment's own readers, so
they read it the way the deployment does. In
`valkey_fuzzer.deployment.local.cluster_nodes`: `parse_cluster_nodes(text)` (one record per `CLUSTER NODES` line: `node_id`,
`address`, `host`, `port`, `flags`, `primary_id`, `config_epoch`,
`link_state`, the raw `slots` tokens, and `is_myself`, `is_primary`,
`is_replica`, `is_fail`), `parse_slot_ranges(tokens)` (those slot tokens as
`(start, end)` ranges, migration markers skipped), `split_slots(views)` (each node's parsed
`CLUSTER NODES` in, the slots two primaries serve at once out, with their
node IDs: a split brain as the state check judges it). In
`valkey_fuzzer.deployment.local.queries`: `parse_cluster_info(reply)`,
`parse_replication(info)` (an `INFO replication` reply as a record: `role`,
`primary_host`, `primary_port`, `link_up`, `sync_in_progress`,
`last_io_seconds_ago`, `replica_offset`, `offset`, `connected_replicas`,
`replication_id` (`master_replid`), `failover_state` (`master_failover_state`),
and `replicas`, a primary's `slaveN` lines in order, each with Valkey's `ip`,
`port`, `state`, `offset`, `lag`, `type`, and every field as text in
`fields`; a field the reply lacks is `None`), and `replication_role(info)` (the reply's
role as `primary` or `replica`, or `None`). Components find `valkey-server`
the same way, with `valkey_fuzzer.deployment.local.processes`:
`resolve_binary_path(binary, label)` turns a path or command name
into an absolute executable path (raising `FileNotFoundError` or
`PermissionError`), and `SOURCE_BUILD_DIR` is where the deployment builds
Valkey when no binary is configured or on `PATH` (the server is
`valkey/src/valkey-server` under it). `STATE_VALIDATION_SCHEMA`
(`valkey_fuzzer.deployment.local.validation.config`) checks a `validation`
mapping for `state_check` when the scenario compiles, including that its
`stabilization_wait` is below its `validation_timeout`.

## Write a provider

### Artifact and resource providers

An artifact provider supplies an immutable input, such as a binary or a
dataset; a resource provider sets up a managed dependency, such as a
filesystem or a service, and removes it afterwards. Both implement one
`EnvironmentProvider` interface and are registered for one `kind`
(`EnvironmentKind.ARTIFACT` or `EnvironmentKind.RESOURCE`, the scenario
section they serve):

| Method | Does |
| --- | --- |
| `check_support(context, spec, config)` | Decide whether it can serve `spec` here, changing nothing. |
| `acquire(context, spec, config)` | Acquire it and return an `EnvironmentResult`. |
| `rollback(context, spec)` | Undo a failed `acquire`. |
| `cleanup(context, spec, result)` | Release what a successful `acquire` returned. |

`spec` is the `EnvironmentSpec` (`kind`, `id`, `provider_id`, `config`,
`depends_on`). Returning `SupportDecision.deny(reason)` from `check_support`
makes the run `SKIP_UNSUPPORTED` before anything starts, so deny only for a
host feature the declared item needs (such as tmpfs); a bad config is an
error, which `configure` reports. The
`EnvironmentResult` holds exactly the declared `outputs` and report-safe
`provenance`.

```python
EnvironmentProviderRegistration(
    kind=EnvironmentKind.ARTIFACT,
    provider_id="example.artifact.dataset",
    factory=DatasetProvider,
    outputs=("path",),
    config=ConfigSchema({"name": ConfigField(str), "rows": ConfigField(int, default=1000, minimum=1)}),
    check=check_dataset,  # optional check(spec, config)
)
```

The registration's `config` schema checks a declaration's `with:` and fills
in defaults, which the plan records; `check(spec, config)` adds pure checks.
By convention provider IDs are `<namespace>.<kind>.<name>`.

### Deployment providers

A deployment provider starts the system under test for one deployment mode:

| Method | Meaning |
| --- | --- |
| `validate(plan)` | A pure preflight; raise to reject the plan. |
| `provision(context)` | Start the deployment and return a `ProvisionedDeployment` with its `services` (what components see under `context.services["deployment"]`), report-safe `metadata`, and optionally `valkey_sha`. |
| `rollback(context)` | Clean up after a failed `provision`. |
| `cleanup(context, deployment)` | Clean up after a successful one. |

A `DeploymentProviderRegistration` has `deployment_mode`, `provider_id`, and
`factory`, plus an optional `implementation_digest`, an optional pure
`normalizer(document)` that fills in the provider's defaults (the plan records
the normalized document, and the experiment's `deployment:` rules are checked
against it), and an optional pure `plan_instances(document, overlays)` that
returns each instance it will start as `{role, shard, server_config}`, with
the effective server config: the provider's own server defaults under the
document's config and the startup overlays. The plan records it as
`instances`, which compile-time checks read, and a plan whose provider plans
other instances is refused. The plan records the provider ID and, when given, the
`implementation_digest`, and runs only on a provider with the same ones. Every
run report also records the SHA-256 of the provider's code, so `replay`
reports a changed provider as drift.

The commands know nothing of a provider's document. What they need to know
about it comes from optional hooks on the registration, and a provider
without one goes without:

| Hook | Used for |
| --- | --- |
| `binary_override(path)` | `--valkey-binary`: the deployment fields that run that binary; without it the option is an error. |
| `relocate_ports(plan)` | `run --random`/`--seed`: the deployment fields that move a generated scenario's ports off ones taken on this host, and a note. |
| `recorded_instances(outcome)` | `replay`: the per-instance provenance the provider left in a recorded outcome. |
| `binary_drift(plan, recorded, *, override, replayed=None)` | `replay` and `minimize`: how the binaries a replay runs differ from the recorded ones; without it they are reported as not verified. |
| `reductions(deployment, requirements)` | `minimize`: smaller deployment sections to try; without it only the inputs shrink. |
| `host_fields` | the coverage tuple: the deployment fields that only place the deployment on a host. |

Several providers may implement one mode. A scenario then selects one with
`deployment.provider`, and may leave it out when only one is registered.
`local_deployment_provider_registrations()` in
`valkey_fuzzer/deployment/local/provider.py` registers the local deployment's
providers, one per mode.

## Register components and experiments

`register_valkey_components` in `valkey_fuzzer/catalog.py` registers what
`valkey_fuzzer/components/` lists:

| What | Where |
| --- | --- |
| components | `REGISTRATIONS` of each module that `_MODULES` in `components/__init__.py` names |
| generator hooks | `GENERATORS` in `components/generators.py` |
| artifact and resource providers | `ENVIRONMENT_PROVIDERS` in `components/providers.py` |
| experiment fragments | `components/fragments/<name>.yaml`, named after the file |
| experiments | `components/experiments/<id>.yaml`; each also extends `valkey.liveness` |

A new component goes in the `REGISTRATIONS` of the module for its role (a new
module also goes in `_MODULES`). A new experiment is a YAML file; for the
components above:

```yaml
# valkey_fuzzer/components/experiments/example.touch.yaml
id: example.touch
deployment:
  modes: [standalone, cluster]
inputs:
  properties:
    count: {type: integer, minimum: 1, default: 3}
components:
  - use: example.action.touch
    id: touch
    config: {key: touched, count: {$ref: inputs.count}}
  - use: example.oracle.touched
    id: check
    depends_on: [touch]
    config: {expect: {$ref: inputs.count}}
required_events: [touch.touched]
required_evidence:
  - {evidence_id: example.touched, source: touch}
```

Code that draws part of a random scenario is a
[generator hook](scenarios-and-experiments.md#generator-hooks):
`GeneratorRegistration("valkey.generator.<name>", hook)` in `GENERATORS`,
where `hook(rng, scenario)` returns the value. Hooks have their own registry,
named `<namespace>.generator.<name>`; they are not components and never
appear in a plan. `valkey-fuzzer list generators` prints them.

- Every entry must be the registration type its registry holds
  (`ComponentRegistration`, `ExperimentSpec`, and so on), and every component
  ID must name its role.
- Everything registers together, or nothing does: a component ID, an
  experiment ID, a provider, or a fragment name registered twice stops every
  command. Experiments from `--experiment-dir` register after the shipped
  ones, and may extend their fragments. After registration the catalog is
  frozen.

## Testing

### In-process

`valkey_fuzzer.testing` runs experiments and components without starting
servers:

- `run_experiment(experiment, registrations, inputs=..., services=...)`
  compiles and runs an `ExperimentSpec` through the real compiler and runner,
  in a catalog that holds only it, the component `registrations`, and any
  `environment_providers` and `deployment_providers` you pass. Modes those
  deployment providers do not provide get a fake deployment whose services
  you supply. Without `artifact_dir` the run's files go to a temporary folder
  removed when it returns.
- `resolved_spec(registration, spec)` returns a component spec with its config
  checked and its defaults filled in, as a compiled plan holds it.
- `FakeContext(*specs, services=..., registrations=REGISTRATIONS)` is a run
  context for calling one component's methods directly; `cancel()` does what
  the runner does when a call outlives its deadline, and `obligations` holds
  what the component opened. `context.call(component.run, spec)` holds the
  call to the runner's evidence contract: an event, state change, or evidence
  item its registration does not declare raises `ContractViolationError` and
  stays in `context.violations`. This repository's unit tests route every
  Valkey component they call this way (`tests/conftest.py`).

```python
from valkey_fuzzer.catalog import load_experiment_yaml
from valkey_fuzzer.components import EXPERIMENT_DIRECTORY
from valkey_fuzzer.plan import ComponentSpec
from valkey_fuzzer.protocols import ComponentRole
from valkey_fuzzer.testing import FakeContext, resolved_spec, run_experiment


class FakeClient:
    values = {}

    def incr(self, key):
        self.values[key] = self.values.get(key, 0) + 1
        return self.values[key]

    def close(self):
        pass


class FakeDeployment:
    def new_client(self, instance_id, timeout=5.0):
        return FakeClient()


experiment = load_experiment_yaml((EXPERIMENT_DIRECTORY / "example.touch.yaml").read_text())
outcome = run_experiment(experiment, REGISTRATIONS, inputs={"count": 3}, services={"valkey.deployment": FakeDeployment()})
assert outcome.status.value == "PASS"

spec = resolved_spec(
    REGISTRATIONS[0],
    ComponentSpec(use="example.action.touch", id="touch", role=ComponentRole.ACTION, config={"key": "k"}),
)
context = FakeContext(spec, services={"valkey.deployment": FakeDeployment()}, registrations=REGISTRATIONS)
result = context.call(Touch().run, spec)
```

### Live

`tests/test_live_valkey_experiments.py` runs every Valkey experiment against
real servers. It uses `VALKEY_FUZZER_BINARY` (or `valkey-server` on `PATH`),
and `VALKEY_FUZZER_OLD_BINARY` for the mixed-version, rolling-upgrade, and
baseline-versus-candidate tests. Tests that need forkless save, root, tmpfs,
or iptables skip when the host lacks them.

```bash
VALKEY_FUZZER_BINARY=/path/to/valkey-server \
VALKEY_FUZZER_OLD_BINARY=/path/to/older/valkey-server \
  python -m pytest -m live tests/test_live_valkey_experiments.py
```

`python -m pytest` runs the unit tests only; `-m slow` runs every test that
starts real servers, `-m live` the live experiment tests, and `-m ""` all of them.

Give every new experiment two live tests: one where it passes, and a
negative control where it must fail (see [Design the test](#design-the-test)).
