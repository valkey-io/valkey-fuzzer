# Concepts and glossary

This page explains the model behind every run: what a scenario and an
experiment are, how a run proceeds, and what it produces. It ends with the
Valkey terms these docs use and a glossary. The rules the core enforces are
in [Base contracts](contracts.md), and the file formats in
[Scenarios and experiments](scenarios-and-experiments.md).

- [How a run works](#how-a-run-works)
- [Components and roles](#components-and-roles)
- [Events and evidence](#events-and-evidence)
- [Outcomes](#outcomes)
- [Tools built on runs](#tools-built-on-runs)
- [Valkey terms](#valkey-terms)
- [Glossary](#glossary)

## How a run works

1. A **scenario** (a YAML file) names an **experiment** and its inputs, a
   **deployment**, and a seed. It can also declare **artifacts** (such as
   binaries), **resources** (such as a tmpfs mount), and **policies**.
2. The compiler checks the scenario against the installed experiment,
   component registrations, and providers, fills in every default (each
   instance's server config included, as its deployment provider will start
   it), and produces an immutable **execution plan**. Nothing starts yet.
   `valkey-fuzzer validate` stops here.
3. The runner executes the plan:
   1. it runs the **capability** probes, which can declare the target
      unsupported before anything changes, and acquires the artifacts and
      resources (when the plan refers to an acquired value, such as a binary
      from an artifact, the probes run right after acquisition instead);
   2. its **deployment provider** starts the deployment (for the built-in
      local provider, `valkey-server` processes on this host);
   3. it starts the observers, workloads, faults, and actions in dependency
      order, then tears the long-running ones down in reverse;
   4. it evaluates the **oracles** against all the evidence recorded;
   5. it cleans up the deployment and releases the artifacts and resources.
4. The **verdict** turns the component results, events, evidence, and any
   errors into one [outcome](#outcomes).
5. The run service writes `plan.json` (before the run) and `report.json`
   (after it) into a new report folder.

The experiment says what to do and what must hold; the scenario says where and
with which values. One experiment runs on any deployment it supports, so the
same test covers a standalone instance, a primary with replicas, and a
cluster.

## Components and roles

A **component** is a registered Python class with exactly one role. An
experiment lists configured uses of components; each use has an `id` that is
unique within the experiment.

| Role | Called | Judges the system under test | Examples |
| --- | --- | --- | --- |
| capability | `probe`, before the deployment starts | no; `SKIP` means unsupported | build and host probes |
| workload | `start`, then `stop` at teardown | no | key/value writes with a ledger |
| observer | `start`, then `stop` at teardown | no | INFO sampling, log scanning |
| fault | `inject`, then `recover` at teardown | no | process signals, network partitions, disk fill |
| action | `run`, once | yes | BGSAVE, failover, polls, rolling upgrade |
| oracle | `evaluate`, after teardown, with all evidence | yes | write ledger, liveness, election safety |

A **generator hook** is not a component: it is a function
`hook(rng, scenario)` in its own registry that draws part of a
[random scenario](scenarios-and-experiments.md#generator-hooks) (cluster
chaos waves, runtime codec changes). It runs while a scenario is drawn and
never in a plan.

A fault's `inject` installs its trigger. The fault is injected when the
trigger fires, usually inside the dispatch of an event such as
`run-before-crash.completed`, so it lands at the same point in every run.
`recover` undoes it and reports whether it was injected at all.

Every change a component makes to host or server state and will undo is
owned by an **undo obligation** until it is undone (see
[Fault ownership](contracts.md#fault-ownership)).

Components declare dependencies with `depends_on`. A component whose
dependency did not pass is skipped. Capabilities may depend only on
capabilities, and nothing that runs before the oracles may depend on an
oracle.

## Events and evidence

**Events** are named messages on the run's event bus. Dispatch is
synchronous: a subscriber runs before `publish` returns, which is how a fault
lands right after the event it triggers on. The runner publishes:

| Event | When |
| --- | --- |
| `<id>.started` | before a component's `probe`, `start`, `inject`, `run`, or `evaluate` |
| `<id>.completed` or `<id>.failed` | after that call returns: `PASS`, or any other status |
| `<id>.stopping`, then `<id>.stopped` or `<id>.stop-failed` | around a component's teardown |
| `run.started`, `run.completed` | at the start and end of the run |
| `deployment.provisioning`, `deployment.ready` | around provisioning |
| `deployment.rollback.started`, `deployment.rollback.completed` | when provisioning failed |
| `validation.completed` | after the oracles |
| `deployment.cleanup.started`, `deployment.cleanup.completed` | around deployment cleanup |

A call that raises or outlives its deadline publishes neither
`<id>.completed` nor `<id>.failed` (nor `<id>.stopped` or `<id>.stop-failed`
for a teardown). Components publish their own events under their `id`, for
example
`bgsave.save-started`, and a log-trigger observer publishes events named by
its rules, for example `cluster.vote.granted`.

**Evidence** is a named, immutable record a component returns with its
result: an `evidence_id` (for example `valkey.rdb.reload-verified`), its
`source` (the component `id`), a value, and optional artifact paths.
Oracles read the evidence recorded before them.

An experiment lists **required events** and **required evidence**. They exist
because configuration alone does not show that a code path ran: a forkless
save experiment must see INFO report a forkless save in progress, not only
that forkless saving was configured. They are path evidence: a run that never
produced them did not test what it claims, and is `ERROR`. Behaviour, such as
no save left in progress after a cancel, is judged by oracles and actions:
observers only record it, and a `valkey.oracle.evidence` oracle checks what
they recorded.

Every component's registration **declares** the events it publishes and the
evidence it emits, each with a schema, and the state it changes; the runner
holds it to them (see [Evidence contract](contracts.md#evidence-contract)).

## Outcomes

The **verdict** turns a run's component results, events, evidence, and
errors into one outcome: `PASS`, `FAIL` (a finding about the system under
test), `ERROR` (no trustworthy verdict), or `SKIP_UNSUPPORTED` (the target
cannot run the experiment). Only oracles and actions can produce a `FAIL`.
The rules are in [Outcomes and verdicts](contracts.md#outcomes-and-verdicts).

Every component draws its random choices from a stream derived from the
scenario seed, so a seed repeats them; it does not fix timing (see
[Seeds, replay, and provenance](contracts.md#seeds-replay-and-provenance)).

## Tools built on runs

- **Replay** runs the execution plan recorded in a report again, after
  checking that the fuzzer, experiment, components, deployment
  provider, and Valkey binary match the recorded provenance (a bare
  `plan.json` has none to check). It uses what is installed now, so it is not
  hermetic.
- A **differential run** runs one seeded scenario under several variants (fork
  versus forkless save, two builds) and compares chosen evidence fields: some
  must be equal, others must differ.
- A **batch** runs many scenarios (random seeds under a **search profile**, a
  matrix, scenario files, the bug zoo, an earlier batch's failures) on
  parallel, isolated workers, sweeps each worker's leftovers, and groups the
  failures by signature (see [batch](cli.md#batch)).
- **Minimization** shrinks a failing scenario one step at a time while the
  failure keeps the same signature.
- The **spec bridge** model-checks a TLA+ model, turns its counterexamples
  into scenarios, and checks observed runs against it
  ([Spec bridge](spec-bridge.md)).
- The **bug zoo** is a set of deliberately broken Valkey builds that the
  experiments must catch, run as a batch source ([Bug zoo](bug-zoo.md)).

Each produces or runs ordinary scenarios through the same runner; the
commands are in the [command-line reference](cli.md).

## Valkey terms

These docs use Valkey's own terms for Valkey:

- **Standalone mode** and **cluster mode** are Valkey's two modes. A
  standalone deployment is one primary with zero or more replicas.
- A **primary** accepts writes; a **replica** replicates a primary.
- An **instance** is one `valkey-server` process. A **cluster node** is an
  instance in cluster mode.
- A **shard** is one primary and its replicas. A cluster has 16,384 hash
  slots, divided among its primaries.
- **Failover** means promoting a replica to replace its primary. Killing or
  restarting an instance is a **process failure** or **restart**, not a
  failover.
- **Quorum** means a majority of the primaries.

The local deployment names its instances `valkey-0`, `valkey-1`, and so on.
Valkey's own output (`role:master`, `connected_slaves`) is quoted as Valkey
prints it.

## Glossary

| Term | Meaning |
| --- | --- |
| action | A component that does one thing once (a command, a BGSAVE, a failover) and can judge the result. |
| artifact | An immutable input that a provider resolves before the run, such as a `valkey-server` binary. |
| batch | Many runs from one or more sources on parallel, isolated workers, with one summary (`valkey-fuzzer batch`). |
| capability | A component that checks before the deployment starts whether the target can run the experiment. |
| component | A registered Python class with one role, configured by an experiment. |
| coverage tuple | The combination a run exercised (experiment, deployment, inputs, components, providers), recorded in its report. |
| declarations | What a component's registration says it publishes, emits, and changes, with schemas; the runner holds it to them. |
| deployment | The system under test: its mode, its instance counts, and provider-specific fields. |
| deployment mode | `standalone` or `cluster` for the local provider; another registered deployment provider may add modes. |
| deployment provider | The code that starts and cleans up a deployment. The built-in one is `valkey-fuzzer.deployment.local`. |
| differential run | One seeded scenario run under several variants, with evidence compared across them. |
| effective server config | An instance's server config as it starts: the deployment provider's defaults under the deployment's config and the startup overlays. The plan records it per instance. |
| event | A named message on the run's event bus. |
| evidence | A named record a component returns, used by oracles and required by experiments. |
| execution plan | The compiled, immutable form of a scenario and its experiment, with every default filled in, each instance's effective server config included. Reports record it and replay runs it. |
| experiment | A declarative composition of components that states what a run must observe; usually a data-only YAML document. |
| fault | A component that perturbs the system when its trigger event fires, and recovers it at teardown. |
| fragment | A named piece of an experiment that other experiments `extends`. |
| generator hook | A `GeneratorRegistration` in the catalog's generator registry that draws part of a random scenario; not a component. |
| instance | One `valkey-server` process (`valkey-N` in the local deployment). |
| legal space | What an experiment allows a random scenario to vary: its `metadata.generator` block and inputs schema. |
| minimization | Shrinking a failing scenario while its failure signature holds. |
| obligation | A change to host or server state that its component owns, with the idempotent undo that reverts it, until it is undone. |
| observer | A component that records evidence (INFO samples, log lines) for the whole run; it never judges the server. |
| oracle | A component that evaluates an invariant after the run, from evidence and the live deployment. |
| outcome | The result of one run: its status (`PASS`, `FAIL`, `ERROR`, `SKIP_UNSUPPORTED`), every component result, evidence, and diagnostics. |
| policy | An opaque declaration in a scenario that the core passes to components without interpreting it. |
| provider | Code that acquires an artifact or resource, or provisions a deployment. |
| replay | Running a recorded execution plan again, after checking the recorded provenance against what is installed. |
| report folder | The folder a run writes `plan.json` and `report.json` into. |
| resource | A managed dependency a provider sets up before the run and removes after it, such as a tmpfs mount. |
| run report | The `report.json` of one run: scenario, outcome, coverage, provenance. |
| scenario | A YAML file that selects one experiment, its inputs, a deployment, and a seed. |
| search profile | How hard to sample an experiment's legal space: edge share, narrower ranges, value weights, and batch budgets; never what is legal or checked. |
| smoke mode | An experiment mode (`mode: smoke`) that only exercises the deployment lifecycle and makes no coverage claim. |
| startup overlay | Server configuration an experiment applies to selected instances before they start. |
| verdict | The rules that decide an outcome from component results, events, evidence, and errors. |
| workload | A component that generates traffic for the whole run. |
