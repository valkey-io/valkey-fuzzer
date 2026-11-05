# Base contracts

These are the rules the core enforces for every scenario, whatever produced
it. An experiment or a component cannot opt out of them. The terms are defined
in [Concepts and glossary](concepts.md); how to meet each rule in code is in
[Adding tests](extending.md).

- [One execution path](#one-execution-path)
- [Outcomes and verdicts](#outcomes-and-verdicts)
- [Evidence contract](#evidence-contract)
- [Fault ownership](#fault-ownership)
- [Effective server config](#effective-server-config)
- [Legal space and search profiles](#legal-space-and-search-profiles)
- [Deadlines and cancellation](#deadlines-and-cancellation)
- [Host state](#host-state)
- [Seeds, replay, and provenance](#seeds-replay-and-provenance)

## One execution path

Every scenario is an ordinary scenario file, compiled to one immutable
execution plan and run by one runner, with one report format and one
verdict. That holds for a handwritten scenario, one `run --random` or a
batch draws, a matrix case, a minimized one, a bug zoo run, and one that
`spec translate` derives from a counterexample. Every scenario a run executes
is saved as `scenario.yaml` next to its report, and `valkey-fuzzer run` runs it
again, a generated one without the generator.

`valkey-fuzzer run` runs one scenario; `valkey-fuzzer batch` is the one way
to run many. Nothing else starts a deployment.

## Outcomes and verdicts

| Outcome | Meaning | Exit code |
| --- | --- | --- |
| `PASS` | Every required check passed, and the run reached the code path it claims to test (its required events and evidence were observed). | 0 |
| `FAIL` | A required action or oracle observed Valkey do something wrong, and nothing makes that finding untrusted. | 1 |
| `ERROR` | No trustworthy verdict. | 2 |
| `SKIP_UNSUPPORTED` | A required capability probe found that the build or host lacks a feature the experiment names, or an environment provider declined a resource the host cannot provide, before anything changed. | 3 |

The rules behind them:

- **Only actions and oracles can fail a run.** A capability probe, workload,
  observer, or fault that fails (returns `FAIL` or `ERROR`, or raises) did
  not establish the conditions of the test, so the run is `ERROR`. A
  required capability that returns `SKIP` makes the run `SKIP_UNSUPPORTED`;
  any other required component that returns `SKIP` makes it `ERROR`. A
  component skipped because a dependency did not pass changes nothing by
  itself: the dependency's result decides. A teardown (`stop`, `recover`)
  that does not pass, or raises, is the component's result: a fault that
  never fired did not do its part even though its `inject` passed.
- **A failure that depends on a component that did not do its part is not
  trusted.** A `FAIL` from an action or oracle that depends (directly or
  through others) on a workload, fault, observer, or capability probe that
  failed makes the run `ERROR`: an action that failed waiting for the effect
  of a fault that never fired says nothing about Valkey. A `FAIL` that does
  not depend on it, such as a crash found before the fault fired, stands.
- **Path evidence and behaviour are different things.** Required events and
  required evidence are path evidence: they show the run reached what it
  claims to test, such as INFO reporting a forkless save in progress rather
  than merely configured. A run missing any of them is `ERROR`, and its
  summary names each missing item and its source, for example
  `required evidence was not observed: valkey.forkless.in-progress from info`.
  A trusted `FAIL` an action or oracle found anyway still stands, and the
  missing items stay in the diagnostics: a crash is a crash whether or not
  the save was forkless. Behaviour is judged by actions and oracles only.
  Observers only record; an expectation about behaviour, such as no save
  left in progress or a partial resync counted, is a `valkey.oracle.evidence`
  rule over what they recorded, and its absence is a `FAIL`. In `experiment`
  mode a plan needs at least one required oracle and one required evidence
  item.
- **A failed read is not a failure.** A component that cannot read Valkey
  retries within its budget and then returns `ERROR`; an observer that could
  not read the server when an unmet expectation needed it returns `ERROR`.
  Each oracle judges on its own: all of them are evaluated even after one
  returns `ERROR` or raises, and a `FAIL` from another required oracle (say
  `valkey.oracle.liveness` finding the server dead that another oracle could
  not read) stands. Any other component that returns `ERROR` or raises, and
  a call that outlives its deadline, makes the run `ERROR`.
- **Oracles judge only a cleanly torn-down run.** If a component's `stop` or
  `recover` failed, or a change was not undone by its owner on the first try
  (see [Fault ownership](#fault-ownership)), the deployment may not be in
  the state the plan describes, so an oracle's `FAIL` is not trusted and the
  run is `ERROR`. An action's `FAIL`, found before the teardown, stands, and
  so does an oracle `FAIL` that reports a crash or hang of a server the
  fuzzer did not stop itself (`ComponentResult.crash`, which
  `valkey.oracle.liveness` sets), since it may be why the teardown failed.
  A forbidden log pattern matched by `valkey.observer.log` is no crash: the
  observer records it as evidence, and `valkey.oracle.liveness` fails the run
  on it.
- **A failed cleanup after the oracles** (the deployment's or the
  environment's) cannot change what an oracle saw: a trusted `FAIL` stays
  `FAIL` with the cleanup error in the diagnostics, and any other run is
  `ERROR`.
- **A contract violation makes the run `ERROR`**: evidence or an event that
  breaks its component's declarations. No verdict can rest on it.
- **Required components** are those with `required: true` (the default), the
  sources of required evidence, and everything they depend on. A `FAIL` or
  `SKIP` from a component that is not required does not decide the outcome,
  but its `ERROR`, an exception, a timeout, or a failed teardown still makes
  the run `ERROR`.

An outcome's diagnostics name the cause: `missing_required_events`,
`missing_required_evidence`, `required_component_failures`,
`untrusted_component_failures`, `contract_violations`, `open_obligations`,
`cleanup_errors`, `abandoned_calls`, or `error`.

## Evidence contract

Every component registration declares the events it publishes and the
evidence it emits, each with a schema, and the kinds of state it changes. A
registration without declarations is rejected when the catalog registers it.
`valkey-fuzzer list components --format json` prints them, and
[Valkey components](components.md#events-evidence-and-state-changes) lists
them for the Valkey components.

At compile time, a required event or evidence ID that no component of the
plan declares (and the runner does not publish) is an error.

At run time the runner checks every event and evidence item as it is
produced. It makes the run `ERROR`, naming the component, when one:

- publishes an undeclared event, a malformed payload, a runner lifecycle
  event, or anything after its last call returned (the event is never
  dispatched);
- emits undeclared or malformed evidence, a single-valued evidence ID twice,
  or evidence naming an uncommitted event (the item is dropped before any
  oracle sees it and listed under `contract_violations`);
- changes state of a kind it did not declare, or after its last call
  returned.

## Fault ownership

Every change a component makes to host or server state and will undo (a
stopped or paused process, a partition, a filled disk, a setting it
restores, a private server) is owned by an **undo obligation** from the
moment it is applied until it is undone. The undo is idempotent; a failed
undo leaves the obligation open and is tried again.

The runner undoes, newest first, whatever is still open once the components
are torn down, and retries failed undos after the deployment is cleaned up.
A start call the runner gave up on (see
[Deadlines and cancellation](#deadlines-and-cancellation)) may still be
applying changes, so both passes leave what its component owns alone; that
is undone the moment the call returns. After the last pass, a change opened
by anything still running (an abandoned call, a thread a component left) is
undone at once, and `open` raises.
Any obligation its owner did not undo on the first try, even when a retry
succeeded, means the deployment was not in the state the plan describes: an
oracle's `FAIL` is not trusted then (see
[Outcomes and verdicts](#outcomes-and-verdicts)), and without a trusted
`FAIL` the run is `ERROR`. `diagnostics.obligations` in the report lists the
`planned` (declared) changes, and every obligation opened, as `undone` or
`still_active`.

A change meant to last until the deployment is removed (a kill without
restart, a new binary, a `CONFIG SET` an action makes) needs no obligation.

## Effective server config

The execution plan records, under `instances`, every instance's role, shard,
and effective server config: the deployment provider's defaults under the
deployment's `server_config`, the per-role and per-instance configs, and the
experiment's startup overlays. Compile-time checks read it, so relying on a
default is checked like writing it out. For example,
`valkey.oracle.ledger` rejects a crash loss window that the local
deployment's default `appendfsync everysec` makes too short. A plan whose
provider would now start other instances is refused.

A config Valkey renamed is written either way and records as written. Since
valkey-io/valkey#5261 `bgsave-default-method` (`fork`, `forkless`) is
`forkless-policy` (`require-fork`, `prefer-forkless`), and a build knows only
one of them. Every start of a build (the deployment's instances, its
restarts, and every private server) gives it the name and value it knows, and
the build probe's `config_get` and `config_set` read and set that name. One
table, `RENAMED_CONFIGS` in `valkey_fuzzer/deployment/local/server_config.py`,
holds these renames. A command an action sends, `CONFIG SET` included, is sent
as written.

## Legal space and search profiles

An experiment's `metadata.generator` block and its inputs schema are its
**legal space**: what may vary, within which ranges, and which combinations
are rejected. A **search profile** changes only how hard that space is
sampled (the share of edge values, narrower ranges, value weights) and the
batch budgets. It never widens a range, adds a value, or changes what is
checked. Every drawn scenario is compiled and statically preflighted before
it runs; one outside the legal space is redrawn, never run. The formats are
in [Scenarios and experiments](scenarios-and-experiments.md#declaring-what-may-be-randomized).

## Deadlines and cancellation

A component call has a deadline when the component sets `timeout_s` in the
experiment or the command has `--timeout SECONDS` (a deadline for the whole
run); with both, the earlier one applies. Components see it as
`context.deadline` and are expected to finish before it. Each call sees its
own deadline; a thread a component starts sees the run's, unless it runs
under a copy of the call's context (`contextvars.copy_context().run`), which
shares the call's deadline and cancellation. The wave engine runs its
operations that way.

- A call with no deadline runs on the runner's own thread. If it hangs, the
  run hangs.
- A call with a deadline runs on a separate thread. When the deadline passes,
  the runner **cancels** it: on that thread `context.cancelled` becomes true,
  `context.sleep()` returns `False` at once, and `context.deadline` is in the
  past. The runner waits up to 5 seconds for the call to return, and the run
  is `ERROR` either way.
- Python cannot kill a thread, so a cancelled call that ignores cancellation
  keeps running. The runner waits for it once more, up to 5 seconds, after the
  deployment is cleaned up (which usually ends calls blocked on a server).
  If it is still running, the outcome lists it under `abandoned_calls`, and
  every later run in the same process is refused as `ERROR` until it returns.
  A batch runs every job in its own process, so one abandoned call never
  blocks the next job.
- An interrupt (Ctrl-C) while a call with a deadline runs cancels that call
  the same way. The runner still tears down, undoes every open obligation
  (an interrupt during one undo does not skip the rest), and cleans up the
  deployment, and then exits `130`.
- With a run deadline, the subscribers of the events the runner publishes
  also run on a watched thread. Dispatch is still synchronous, so a fault
  still fires right after its trigger. Teardown and cleanup events
  (`<id>.stopping`, `<id>.stopped`, `<id>.stop-failed`, the deployment
  rollback and cleanup events, `run.completed`) get a 60-second window each.
  A dispatch still running at its deadline aborts the event bus: no later
  event is dispatched, and the run is `ERROR`.
- Teardown (`stop`, `recover`) ignores `--timeout` and gets the component's
  own `timeout_s` as a fresh window. A hung teardown without `timeout_s`
  hangs the run. A component whose start call the runner gave up on is torn
  down only after that call returns: the runner waits for it after
  deployment cleanup, and if it is still running then, the run records a
  cleanup error saying the component was not torn down.
- Provisioning the deployment and acquiring artifacts and resources are not
  interrupted. The runner checks the run deadline before and after
  provisioning.

## Host state

Every server, data directory, iptables chain, and tmpfs mount the fuzzer
creates is tagged with the fuzzer process that owns it. `valkey-fuzzer sweep`
removes what a fuzzer that is gone left behind, and never touches anything
untagged or owned by a running fuzzer (see [sweep](cli.md#sweep)). A batch
sweeps each worker's leftovers after every job; anything it finds is
recorded and makes the job `ERROR`.

## Seeds, replay, and provenance

Every component draws its random choices from `context.rng(component_id,
stream)`, a stream derived from the scenario seed, the component `id`, and a
stream name. The same seed repeats every choice: the keys a workload writes,
the replica a failover promotes, the instance a chaos action kills. The
generator draws each dimension of a random scenario from its own stream, so
the same seed and profile always draw the same scenario.

A seed does not fix timing. Thread scheduling, process start-up, and the
server's own timers still vary, so a fault that races with the server can
land differently on a rerun or a replay.

A run report records the digests of the plan, the experiment, the fuzzer's
source (the Valkey components included), the deployment provider, and each
instance's binary. `replay` refuses to run when any of them differ from what is
installed now, unless `--allow-drift` is given (see [replay](cli.md#replay)).
Replay is not hermetic: it runs with what is installed and cannot reproduce
timing.
