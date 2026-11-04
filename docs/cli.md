# Command-line reference

This page lists every `valkey-fuzzer` command and option, the exit codes, the
folders and files the commands write, and the report formats. It is for
people who run the fuzzer by hand and for scripts and CI jobs that call it.
The scenario format is in [Scenarios and experiments](scenarios-and-experiments.md),
and cluster chaos and the CI workflows are in [Cluster chaos and CI](cluster-chaos-and-ci.md).

- [Commands](#commands)
- [Common options](#common-options)
- [Exit codes](#exit-codes)
- [Report folders](#report-folders)
- [run](#run) (and [generated scenarios](#generated-scenarios-run---random)),
  [batch](#batch), [validate](#validate), [list](#list), [replay](#replay),
  [diff](#diff), [minimize](#minimize), [cleanup-logs](#cleanup-logs),
  [sweep](#sweep), [spec](#spec)
- [Run reports](#run-reports)

## Commands

| Command | Does |
| --- | --- |
| `run [SCENARIO]` | Compile and run one scenario, or a [generated one](#generated-scenarios-run---random) with `--random` or `--seed`. |
| `batch [SPEC]` | Run many scenarios (random seeds, a matrix, scenario files, the bug zoo, earlier failures) on parallel, isolated workers. |
| `validate FILE...` | Compile scenarios and check them without starting anything. |
| `list experiments\|components\|generators` | List the installed experiments, components, or generator hooks, or print the catalog as JSON. |
| `replay REPORT` | Check a recorded run against this environment, then run its execution plan again. |
| `diff FILE` | Run one seeded scenario under several variants and compare their evidence. |
| `minimize REPORT` | Shrink a failing scenario while its failure signature holds. |
| `cleanup-logs SEED` | Delete the instance logs of one seed. |
| `sweep` | Remove what fuzzer processes that are gone left on this host. |
| `spec check\|translate\|conform` | Check a TLA+ spec bundle, turn its counterexamples into scenarios, and check runs against it. |

`valkey-fuzzer --version` prints the version, and `valkey-fuzzer COMMAND
--help` the options of one command.

## Common options

`run`, `batch`, `validate`, `list`, `replay`, `diff`, and `minimize` accept:

| Option | Meaning |
| --- | --- |
| `--experiment-dir DIR` | Also load the data-only experiments in `DIR` (`*.yaml` and `*.yml`). Repeatable. |

Each of these is accepted only by the commands that use it; any other command
rejects it:

| Option | Commands | Meaning |
| --- | --- | --- |
| `--valkey-binary PATH` | `run`, `batch`, `validate`, `replay`, `diff`, `minimize` | Use this `valkey-server` for the deployment (the local deployment's `deployment.valkey_binary`; a deployment provider without a `binary_override` hook refuses it). For `batch` it applies only to scenarios that name no binary of their own; for `replay` it is an intentional override; see [replay](#replay). |
| `--artifact NAME=PATH` | `run`, `replay` | Resolve the scenario's `valkey.artifact.local-binary` artifact `NAME` at `PATH` instead of the path it declares, so a run recorded elsewhere (a CI batch's parent build, `artifacts.parent`) runs on this host's build of it. Repeatable, once per artifact; an artifact the scenario does not declare, or of another provider, is an error. The report records the scenario with the override; see [replay](#replay) for how the binary is still compared. |
| `--artifact-dir DIR` | `run`, `batch`, `replay`, `diff`, `minimize` | Root for report folders, which hold each run's component artifacts, and for provider directories; for `batch`, where the default batch folder goes. See [Report folders](#report-folders). |
| `--timeout SECONDS` | `run`, `batch`, `replay`, `diff`, `minimize` | Deadline for each run. See [Deadlines and cancellation](contracts.md#deadlines-and-cancellation). |
| `--allow-unsupported` | `run`, `batch`, `replay`, `diff` | Exit `0` instead of `3` on `SKIP_UNSUPPORTED`. |

## Exit codes

| Code | Meaning |
| --- | --- |
| `0` | `PASS`, or `SKIP_UNSUPPORTED` with `--allow-unsupported` |
| `1` | `FAIL` |
| `2` | `ERROR`, or the command itself failed (a usage error, a missing file, a compile error, replay drift) |
| `3` | `SKIP_UNSUPPORTED` |
| `130` | interrupted (Ctrl-C or `SIGTERM`; the run is cleaned up first) |

Errors and warnings go to stderr, so stdout (`--format json`, say) holds only the command's output.

These hold for `run`, `replay`, and `diff`. The other cases:

- `run`, `replay`, and `diff`: an `--output` (or `--export-scenario`) that
  cannot be written counts as an `ERROR`, unless the run failed.
- `batch` exits with its worst result: `1` if any run failed or a zoo case did
  not hold, else `2` if any run errored, left something behind, a zoo case
  was undecided, or the batch could not start, else `3` if any run was unsupported, a zoo case was
  skipped, or a matrix combination did not compile, else `0`; `130` when it was stopped before it finished (resume it).
  A `--dry-run` exits `0`. See [batch](#batch).
- `validate`, `list`, and `minimize` exit `0` on success and `2` on error.
  `minimize` also exits `0` when the run had nothing to minimize.
- `cleanup-logs` exits `0`, or `2` when the prompt is declined or a file could
  not be deleted.
- `sweep` exits `2` when it could not remove something it found.
- `spec check` and `spec conform` exit `1` when a gate failed or a claimed
  property was violated, else `2` on a harness error (a tool that did not run,
  a bundle that does not exist); see [spec](#spec).
- `valkey-fuzzer` without a command prints its help and exits `2`.

## Report folders

Every execution plan a command runs gets its own report folder holding:

- `plan.json`: the execution plan, written before the run starts;
- `report.json`: the [run report](#run-reports), written when it ends. A run
  interrupted with Ctrl-C or `SIGTERM` leaves only `plan.json`;
- `components/<component id>/`: the files the run's components wrote
  (captured RDBs, ledgers, INFO samples, log excerpts, traces), if any;
- `scenario.yaml`: the scenario as run (a generated one too), which `run`
  accepts as is.

| Command | Report folders |
| --- | --- |
| `run` | `<root>/runs/<name>/` |
| `replay` | `<root>/replays/<name>/` |
| `diff` | `<root>/differential/<name>/<variant>/`, with `differential.json` in `<name>/` |
| `batch` | `<batch folder>/runs/<job id>/`, with `summary.json` in the batch folder; the folder is `--out`, or `<root>/batches/<name>/` |
| `minimize` | `<root>/minimize/<name>/attempt-<n>/` |

`<root>` is `--artifact-dir`, or `<temp dir>/valkey-fuzzer` without it
(`<temp dir>` is `/tmp` on most Linux hosts). `<name>` is the UTC start time
to the millisecond, the scenario, differential, or batch ID, and a random
suffix, for example `20261028T041350.730Z-aof-durability-5405f2`. Folders are
created exclusively, so names sort by start time and never collide, even
between processes. `run` and `replay` print `Report: <path>`;
`diff` and `minimize` print `Reports: <folder>`, and `batch` prints
`Batch: <folder>`.

Other files go elsewhere:

| Files | Location |
| --- | --- |
| Artifact and resource provider directories | `<root>/<run id>/providers/<provider>/<kind>/<id>/` |
| Instance logs | `<base_data_dir>/logs/<seed>-<deployment id>-valkey-N.log` and `.stdio.log` |
| Instance data | `<base_data_dir>/deployment-<id>/valkey-N/data/`, or under `instance_data_roots` |

`base_data_dir` is a deployment field, `/tmp/valkey-fuzzer` by default.
Evidence in `report.json` names the artifact files it refers to.

Every user's runs on a host share the default directories: `<temp dir>/valkey-fuzzer`
and its `<kind>` folders (`runs/`, `batches/`, ...), the default
`base_data_dir` and its `logs/`, `<temp dir>/valkey-fuzzer-mounts`,
`/tmp/valkey-fuzzer-locks`, and `/tmp/valkey-build`. One the fuzzer creates
gets mode `1777`, sticky and world-writable like `/tmp`, so a run as root
(partitions and tmpfs need it) does not leave it unwritable for other users,
and no user can remove another's files in it. A directory that already exists
keeps its mode: one left `0755` by an older version needs `sudo chmod 1777`,
or removing. A run's own folders, a deployment directory, and an
`--artifact-dir` or `base_data_dir` you name, with what is made in it, get the
usual mode.

## run

```bash
valkey-fuzzer run examples/valkey/aof_durability.yaml --output report.json --output-format json --verbose
valkey-fuzzer run --seed 42 --experiment valkey.rdb-compression --profile pr-quick
```

Compiles the scenario, runs it, and writes its report folder. Without a
file, `--random` or `--seed` generates the scenario (see [Generated
scenarios](#generated-scenarios-run---random)). To run many, use
[batch](#batch).

| Option | Meaning |
| --- | --- |
| `--output PATH` | Also save the run report here. |
| `--output-format json\|yaml` | Format of `--output` (which it needs); default `json`. |
| `--verbose` | Print every component's result, evidence count, and diagnostics, and the run diagnostics. |

The command prints the scenario, the experiment, the outcome, component
counts, the evidence count, the Valkey SHA, the summary, and the report
path. The summary names why a run did not pass; for missing required
evidence it names each item and its source, such as `required evidence was
not observed: valkey.rdb.reload-verified from snapshot`.

The report records the scenario as run, including a `--valkey-binary` or
`--artifact` override, so `replay` and `minimize` can use it on their own.
`--artifact` needs a scenario file: a generated scenario declares no artifacts.

### Generated scenarios: run --random

```bash
valkey-fuzzer run --random                                    # an experiment drawn from those that opt in
valkey-fuzzer run --seed 42 --experiment valkey.replication-partial-resync --profile pr-quick
valkey-fuzzer run --random --experiment valkey.snapshot-equivalence --exclude server_config.rdbcompression=zstd
valkey-fuzzer run --random --experiment valkey.cluster-chaos
valkey-fuzzer run --random --export-scenario s.yaml
```

`--random` or `--seed N` replaces the scenario file: the generator draws a
whole scenario (deployment shape, inputs, server config pools, fault timing)
for an experiment that declares `metadata.generator` (see
[Declaring what may be randomized](scenarios-and-experiments.md#declaring-what-may-be-randomized)),
compiles it, and runs it like any other. The same seed (and profile) always
draws the same scenario. `batch --random EXPERIMENT --seeds K` runs many
seeds (see [batch](#batch)).

| Option | Meaning |
| --- | --- |
| `--random` | Draw from a random seed (recorded in the report). |
| `--seed N` | Draw from seed N. |
| `--experiment ID` | Generate for this experiment. Without it each seed draws one of the opted-in experiments, so the choice for a seed changes when experiments are added. |
| `--profile NAME` | Draw under this [search profile](scenarios-and-experiments.md#legal-space-and-search-profiles) (`pr-quick`, `nightly-soak`, `adversarial`). |
| `--exclude PATH=VALUE` | Never generate VALUE at PATH: `inputs.<name>`, `deployment.<field>`, `server_config.<key>` (any value a planned instance starts with: the deployment provider's defaults, the deployment's config, one instance's, or an experiment's overlay), or `experiment` (not drawn when choosing one). A string compares as written, anything else as YAML reads it. Repeatable. |
| `--base FILE` | A partial scenario merged over every draw, for what the generator cannot know: a second build, a port range, a pinned input. |
| `--export-scenario FILE` | Write the generated scenario to FILE. |

Every candidate is compiled and statically preflighted (what `validate`
checks) before it runs. A candidate that is invalid, matches one of the
experiment's `reject` rules, or holds an excluded value is redrawn from the
next attempt's streams, up to 50 attempts; then the run is a harness error
(exit 2) naming the last reason. A scenario whose planned ports are in use,
or reach the host's ephemeral port range, moves to free ones below that
range, and the scenario records the port it ran on.

The run prints a `Reproduce:` command, shell-quoted, that draws the same
scenario: the seed, experiment, exclusions, profile, and every option the
draw depends on (`--base`, `--valkey-binary`, `--experiment-dir`). Each run's report folder also holds `scenario.yaml`, the generated scenario
with a header naming its seed and experiment; `valkey-fuzzer run
<folder>/scenario.yaml` runs it again without the generator. The run report's
`reproducer.generated` records `seed`, `experiment`, `attempt`,
`excludes`, `boundary` (how many drawn values were a
range edge or the first or last listed value), and `profile`.

zstd needs a build made with `BUILD_ZSTD=yes`. On other builds a scenario that
draws it is `SKIP_UNSUPPORTED` (the experiments probe their builds with the
drawn configuration); `--exclude server_config.rdbcompression=zstd --exclude
server_config.repl-compression=zstd` avoids drawing it.

## batch

```bash
valkey-fuzzer batch --random all --seeds 5 --profile pr-quick --workers 4
valkey-fuzzer batch examples/valkey/forkless_batch.yaml --workers 2 --out forkless-batch
valkey-fuzzer batch --scenarios proposals/ --profile candidate --valkey-binary ./valkey-server
valkey-fuzzer batch --zoo BUILDS --workers 2
valkey-fuzzer batch --failures forkless-batch --serial-reruns 2
valkey-fuzzer batch --resume forkless-batch
```

The one way to run many scenarios. A batch expands its sources into jobs, one
run each, runs them on `--workers` parallel workers that never share ports,
logs, or directories, removes whatever each run left on the host, and writes
one batch folder with every run's report folder and a summary. It resumes
after an interruption.

### Sources

A batch spec (`SPEC`) lists sources and options; the flags add sources to it
and override its options. Relative paths in a spec are read from the spec's
directory.

```yaml
batch_id: nightly                    # names the default folder
profile: nightly-soak                # a search profile: sampling and budgets
workers: 8
timeout_s: 1800                      # per run; default the profile's, else 1200
port_base: 11000
valkey_binary: /opt/valkey/src/valkey-server   # for scenarios that name no binary
prune_pass: true
serial_reruns: 2
sources:
  - random:
      experiments: all               # or a list of experiment IDs
      seeds: 50                      # per experiment; default the profile's, else 1
      first_seed: 0                  # default: a fresh one, recorded in jobs.json
      excludes: [server_config.rdbcompression=zstd]
      base: {deployment: {replicas_per_primary: 1}}   # a partial scenario over every draw
  - matrix:
      seed: 2026
      seeds_per_case: 1
      base: {scenario_id: forkless, experiment: {id: valkey.forkless-save}, deployment: {mode: standalone}}
      dimensions:
        deployment.replicas_per_primary: [0, 1]
        experiment.inputs.mutation_rate: [0, 1500]
  - scenarios: [proposals/, extra.yaml]
  - zoo: {builds: BUILDS, cases: [replica-skips-writes], directory: zoo}
  - failures: {batch: earlier-batch, statuses: [FAIL, ERROR], per_signature: 3}
```

| Source | Flag | Jobs |
| --- | --- | --- |
| `random` | `--random EXPERIMENT` (repeatable, or `all`), `--seeds N`, `--first-seed N`, `--exclude PATH=VALUE`, `--base FILE` | `random-<experiment>-s<seed>`: seeds `first_seed` to `first_seed + seeds - 1` of each experiment, drawn under the batch's profile exactly as `run --seed N --experiment ID --profile P` draws them. `all` is every experiment that declares `metadata.generator`, less those an `experiment=` exclusion names. |
| `matrix` | (spec only) | `matrix-<base scenario_id>-<combination>-<n>`: every combination of the dotted-path `dimensions` (paths as in [diff](#diff) variants), `seeds_per_case` times, each with a seed drawn from `seed`. A combination that does not compile is listed under `invalid` and not run, and counts as `ERROR` in the batch's status (exit `2`): a misspelt value never skips. |
| `scenarios` | `--scenarios PATH` (repeatable) | `scenario-<file stem>`: each file, or each `*.yaml` and `*.yml` file of a directory, run as written. |
| `zoo` | `--zoo BUILDS`, `--zoo-case ID` (repeatable) | `zoo-<case>-<file stem>-control`, and `-<n>` for the broken runs, numbered from 0 across the case: each [bug zoo](bug-zoo.md) case's clean control and broken runs, on the builds `zoo/build.sh` made in `BUILDS`. `directory` defaults to the repository's `zoo/`, which only a checkout has (an installed package does not ship it). Each build must name the pinned commit, or nothing runs. |
| `failures` | `--failures BATCH` | `rerun-<job id>`: the jobs of an earlier batch whose result had one of `statuses` (default `FAIL` and `ERROR`), at most `per_signature` of each failure signature (default all), each as it ran: the same scenario, and for a random job the scenario its seed drew (its run's `scenario.yaml`), so a re-run after a code change runs what failed. A random job that never drew a scenario draws again. Zoo runs are left out. |

A job that names no binary uses `valkey_binary` (`--valkey-binary`); a zoo
run names its build.

| Option | Flag | Meaning |
| --- | --- | --- |
| `batch_id` | `--batch-id` | Name of the default folder; default `batch`. |
| `profile` | `--profile NAME` | The [search profile](scenarios-and-experiments.md#legal-space-and-search-profiles) every random job is drawn under, and the budgets. |
| `workers` | `--workers N` | Parallel workers; default 1. On resume, the only option that may change. |
| `timeout_s` | `--timeout SECONDS` | The deadline of each run. |
| `port_base` | `--port-base PORT` | The first client port of worker 0; default 11000. |
| `valkey_binary` | `--valkey-binary PATH` | See above. |
| `prune_pass` | `--prune-pass` | Keep only `report.json` and `scenario.yaml` of a run that passed and left nothing behind. |
| `serial_reruns` | `--serial-reruns N` | After the parallel runs, run `N` jobs of each `FAIL` or `ERROR` signature again, one at a time (load can explain those, not an unsupported run). |
| | `--out DIR` | The batch folder; default `<root>/batches/<time>-<batch id>-<suffix>`. |
| | `--resume DIR` | Finish the batch in `DIR`. Takes only `--workers` and `--allow-unsupported`: the folder holds the sources and every other option. |
| | `--dry-run` | List the jobs, the invalid matrix cases, and the skipped zoo cases; run nothing. |

### Isolation, deadlines, and leaks

Each job runs in its own worker process (`python -m valkey_fuzzer.batch
TASK`) on one of the workers' slots. Slot `k` owns the client ports
`port_base + 120k` to `port_base + 120k + 119` and their cluster bus ports,
the per-instance loopback addresses `127.0.<k+1>.0/24`, its own
`base_data_dir`, `TMPDIR`, and provider directories under `work/w<k>/`,
which are wiped after each job. The batch overrides each scenario's
`base_port`, `loopback_block` (`k + 1`), and `base_data_dir` with its slot's,
so a partition (whose rules match addresses) in one worker never cuts
another worker's instances, and never moves a scenario to other ports; the
job's result records the slot's `ports` and `loopback_block`. A scenario
that plans more than 120 instances is an `ERROR` before anything starts.
The workers' blocks, and their bus ports 10000 higher, must fit below port
65536 without overlapping, so a batch has at most 83 workers. They must also
stay below the host's ephemeral port range (read from
`/proc/sys/net/ipv4/ip_local_port_range` on Linux, where it defaults to
32768-60999, and `sysctl net.inet.ip.portrange` on macOS, 49152-65535), where
an outgoing connection can take a port before Valkey binds it: `W` workers
need `port_base + 120W + 10000` at most the range's first port, so on a
default Linux host `port_base` at most `22768 - 120W` (the default 11000
fits all 83). The batch
refuses a block that reaches the range and names the highest `port_base`
that fits. One batch runs on a host at a time: every batch's workers use
loopback blocks 1, 2, ... (see `loopback_block` in
[The local deployment](scenarios-and-experiments.md#the-local-deployment)),
so a batch started while another runs is refused before it starts a job
(it holds `/tmp/valkey-fuzzer-locks/batch` while it runs).

A run's deadline is cooperative (see
[Deadlines and cancellation](contracts.md#deadlines-and-cancellation)); a
worker still running 60 seconds after it (or a quarter of the deadline, if
that is longer) gets `SIGINT`, then `SIGTERM` 60 seconds later (both to the
worker alone: its run stops its servers), then `SIGKILL` to its process group
30 seconds later, and the job is an `ERROR` (`Deadline`). A second Ctrl-C
skips to the next signal. A worker
that exits without a run report is an `ERROR` (`WorkerExit`), and one whose
job could not be generated or compiled leaves `error.json` and is an `ERROR`.

After every run, the [sweep](#sweep), limited to that worker's owner tag and
its directories (and `/tmp`, where private servers' directories are), removes what the run left: servers, iptables chains, tmpfs
mounts, data directories. Anything it finds is recorded under the job's
`leaks` and the summary's `leaks`, and makes the job an `ERROR`. The job's
summary (in its result, `summary.json`, `batch.log`, and on the console) then
names them: `passed its checks but left 2 leftovers: directory, server` for a
run that passed, or `left 1 leftover: server; <the run's summary>` otherwise.
A slot whose sweep could not remove a leftover (a mount that will not
unmount, say) runs no more jobs, and keeps its `work/w<k>/`: its ports or
addresses may still be taken. Once no slot is left the batch stops; sweep
the host, then resume it. Before its first job, a batch (a resumed one too)
removes the servers, iptables chains, and tmpfs mounts of every fuzzer
process that is gone: a chain a killed batch left matches the addresses of
this batch's slots, and would cut its instances apart. If one cannot be
removed, the batch refuses to start. Dead runs' directories stay.

### The batch folder

```text
<batch folder>/
  batch.json          options, sources, invalid matrix cases, zoo cases (written once)
  jobs.json           every job, in order (written once)
  results/<id>.json   one finished job
  runs/<id>/          the job's run folder: plan.json, report.json, scenario.yaml,
                      components/, logs/ (instance logs), console.log (or error.json)
  experiments/        a copy of every --experiment-dir's documents (only with one)
  summary.json        the summary, rewritten when the batch stops
  batch.log           one line per finished job
  workers.json, work/ the jobs running now and the workers' scratch (while running)
```

Each `runs/<id>/report.json` is an ordinary [run report](#run-reports):
`replay`, `minimize`, and `spec conform` read it, and `valkey-fuzzer run
runs/<id>/scenario.yaml` runs the job again without the batch. A batch given
`--experiment-dir` copies the directories' `*.yaml` and `*.yml` documents
into `experiments/` (two directories holding the same file name are refused),
so `replay runs/<id>/report.json --experiment-dir experiments` reruns a job
from the folder alone.

`summary.json` has:

| Key | Contents |
| --- | --- |
| `status`, `exit_code`, `complete` | The worst result (see [Exit codes](#exit-codes)), its exit code, and whether every job finished. |
| `batch_id`, `folder`, `profile`, `created_at`, `updated_at`, `jobs`, `finished` | The batch and how far it got. |
| `counts` | Runs by status, zoo runs and serial re-runs left out. |
| `runs` | Every finished job: `id`, `source`, `status`, `summary`, `experiment`, `seed`, `report` (its run report, relative to the folder; `null` when it left none), `signature_key`, `duration_s`, `worker`, `leaks` (a count), `origin` (the job a re-run repeats). |
| `signatures` | The non-PASS runs grouped by the minimizer's [failure signature](#minimize) and experiment (a crash by its causes alone, across experiments), most runs first: `key`, `label`, `status`, `experiment`, `signature`, `count`, `runs`, and `serial_reruns`: `runs` (each re-run's `id`, `status`, `same_signature`, and its own `signature_key` and `signature_label`), `reproduced` (how many failed the same way), and `verdict`: `reproduced` (all did), `intermittent` (some did), `failed differently` (none did, but some failed another way; the summary printout gives their signatures), or `not reproduced` (all passed, which points at load). |
| `zoo` | `valkey_commit`, `cases` (each case's controls, per-scenario detection rates, and whether it `holds`: true, false, or null for a case with an unobserved `ERROR` or `SKIP_UNSUPPORTED` run, which `undecided` then names), `skipped` (cases this host cannot run), `held`, `not_held`, and `undecided`. |
| `leaks` | Every leftover the sweeps removed, with the run that left it. |
| `invalid` | Matrix combinations that did not compile. |
| `coverage` | Per experiment, the runs and the distinct coverage tuples they covered. |

### Resuming

A batch stopped with Ctrl-C or `SIGTERM` interrupts its running jobs,
records no result for them, removes what they left, and exits `130`;
`batch --resume DIR` runs what is left. A batch killed outright (`SIGKILL`,
a power loss) can leave its workers running: the resume stops them first,
sweeps what they left, and runs their jobs again; a slot whose sweep could
not remove what its job left runs no more jobs. A finished job never runs
again. Only one process runs a batch folder at a time.

A resume refuses every option but `--workers` and `--allow-unsupported`
(the batch keeps the options it was created with), and `--dry-run`. A batch
with random jobs records a digest of the fuzzer and the
experiments its draws depend on; a resume where they changed is refused,
since its random jobs would draw other scenarios than their seeds did.

## validate

```bash
valkey-fuzzer validate examples/valkey/cluster_chaos.yaml --verbose
valkey-fuzzer validate drafts/*.yaml --format json
```

Compiles each scenario and runs every check that needs no server: fields,
inputs, references, component config, deployment fields, instance planning,
and startup overlay selectors. Checks that need acquired artifacts, running
components, or the Valkey binary happen when the scenario runs. `--verbose`
lists each component with its role, registration, `required` flag, and
dependencies. The exit code is 0 only when every file is valid, and 2
otherwise.

`--format json` is for tools that write scenarios (an AI agent proposing
them, say). All files are checked with one catalog:

```json
{
  "ok": false,
  "files": [
    {"file": "drafts/a.yaml", "ok": true, "errors": [], "experiment": "valkey.aof-durability", "scenario_id": "a"},
    {"file": "drafts/b.yaml", "ok": false,
     "errors": [{"path": "components.data.config",
                 "message": "component 'valkey.oracle.ledger' rejected 'data': every instance runs appendfsync everysec, ...: loss_window_s must be at least 2.5 (got 1)"}]}
  ]
}
```

`path` is the dotted location in the scenario (`""` for the whole file, such
as a YAML syntax error or a missing file); a path that starts with
`components.<id>` or `startup_overlays.<id>` names the experiment's component
or overlay whose config, after the scenario's inputs were substituted, was
rejected. Compilation stops at the first error, so each failing file lists
one.

## list

```bash
valkey-fuzzer list experiments
valkey-fuzzer list components --format json
valkey-fuzzer list generators
```

`experiments` prints `<id> [<modes>] <description>`, `components` the
role, registration ID, and Python class, and `generators` each generator
hook's ID and Python function.

`--format json` prints the machine-readable catalog, for tools that write
scenarios. Everything is derived from the installed catalog and the same
`ConfigSchema`s the compiler checks with, so it cannot drift from what a
scenario must meet. Keys are sorted and rows are in a fixed order
(experiments and generators by ID, components by role and then ID), so the output of one
installation is byte-for-byte stable.

Each experiment (abbreviated):

```json
{
  "id": "valkey.rdb-compression",
  "description": "BGSAVE (fork or forkless) and restart reload ...",
  "mode": "experiment",
  "directory": null,
  "deployment_modes": ["standalone"],
  "deployment_requirements": {"type": "object", "properties": {}, "required": [], "additionalProperties": true},
  "inputs": {
    "type": "object", "required": [], "additionalProperties": false,
    "properties": {"operations": {"type": "integer", "minimum": 1, "maximum": 1000000, "default": 3000}, "...": {}}
  },
  "generator": {
    "declaration": {"inputs": {"operations": {"minimum": 1, "maximum": 3000}}, "server_config": ["valkey.pool.compression"]},
    "pools": {"valkey.pool.compression": {"rdbcompression": ["no", "yes", "lzf", "lz4", "zstd"], "scope": ["deployment", "role"]}}
  },
  "components": [{"id": "writes", "use": "valkey.workload.key-value", "role": "workload", "depends_on": [],
                  "required": true, "timeout_s": null, "config": {"max_ops": {"$ref": "inputs.operations"}}}],
  "startup_overlays": [{"overlay_id": "save-and-reload", "selector": {"first": false, "instance_ids": [], "roles": []},
                        "server_config": {}, "conflict_policy": "override"}],
  "required_events": ["writes.drained", "restart.completed"],
  "required_evidence": [{"evidence_id": "valkey.dataset.digest", "source": "dataset", "min_count": 1}]
}
```

- `inputs` and `deployment_requirements` are JSON Schemas: `type` (a list
  when a field takes several; `environment_reference` for a field that takes
  `{$ref: artifacts|resources...}`, with `reference_kinds`), `enum`,
  `minimum`, `maximum`, `default`, `description`, and the `required` names.
- `generator` is null for an experiment without `metadata.generator`;
  otherwise `declaration` is the block after its fragments are merged in and
  `pools` the server config pools its `server_config` names.
- `directory` is the `--experiment-dir` the experiment was loaded from;
  `null` for a shipped experiment.
- `required_evidence` is what the experiment itself requires. A compiled
  plan also requires what each component's config marks as required (such as
  an observer expectation with `required: true`), which depends on the
  scenario's inputs, so it shows only in the plan.

Each component has `component_id`, `role`, `implementation`,
`deployment_modes` (null: every mode), `config`, the JSON Schema of its
config, and `declares`: its `events` (name to payload schema), `evidence`
(ID to `{many, value}` schema), and `changes` (see
[Evidence contract](contracts.md#evidence-contract)).

Each generator hook has `generator_id` and `implementation` (the function). Generator hooks are not components and `list components` does not
show them.

## replay

```bash
valkey-fuzzer replay report.json --output replay.json
```

Reads a run report (any command's `report.json`, or a `--output` copy of one)
or a bare `plan.json`, and runs the recorded execution plan again with the
fuzzer and binaries installed now. Each replay writes its own
report folder under `<root>/replays/`; `--output` saves a copy of its report.

Before it starts anything, replay compares this environment with the report's
`reproducer` and the deployment's recorded instances:

| Compared | Recorded as |
| --- | --- |
| execution plan digest | `reproducer.plan_digest` |
| the installed experiment's digest | `reproducer.experiment.digest` |
| each used component's import path | `reproducer.components[].implementation` |
| the fuzzer's code (SHA-256 over every file of the `valkey_fuzzer` package, the Valkey components included) | `reproducer.fuzzer_digest` |
| the deployment provider's identity | the plan's `deployment_provider` |
| the deployment provider's import path and code (its whole top-level package) | `reproducer.deployment_provider` |
| each instance's Valkey binary, by SHA-256 | `outcome.diagnostics.deployment.instances` |
| each artifact, by the SHA-256 its provider recorded (after the run) | `outcome.diagnostics.environment_report` |

The report's scenario must also compile, here, to the plan it records. If
anything differs, replay prints the differences and exits `2` without
running:

```text
Error: refusing to replay because this environment differs from the recorded run:
  - fuzzer code: recorded 3f1c9a0e5b2d..., replay uses 8d07e4c1a96f...
Pass --allow-drift to replay anyway, or --valkey-binary to choose a binary on purpose.
```

| Option | Meaning |
| --- | --- |
| `--allow-drift` | Replay anyway. The differences are printed and recorded in the replay report's `provenance`. A different deployment provider identity still stops the run, as `ERROR`. |
| `--valkey-binary PATH` | Run the instances that use the deployment's default binary on this binary instead. It is recorded as an intentional override, not as drift; instances with their own `instance_binaries` entry are still compared. The replay report's `scenario` carries it as `deployment.valkey_binary`, so `minimize` and `replay` of that report run it too. |
| `--artifact NAME=PATH` | Resolve the recorded plan's `valkey.artifact.local-binary` artifact `NAME` at `PATH` (a downloaded CI batch's `artifacts.parent`, rebuilt here). Recorded under `provenance.artifact_overrides` and in the replay report's `scenario`; the binary that ran is still digested and compared with the recorded run after it, so a rebuild whose bytes differ is reported as drift, which `--allow-drift` accepts. |
| `--output PATH` | Save a copy of the replay report. |

A binary that an artifact provider supplies is only known once the replay's
deployment runs, so it is compared after the run, and so is every artifact
whose provider recorded a `sha256` in its provenance (such as an upgrade
target no instance starts on); a difference makes replay exit `2` unless
`--allow-drift` was given. When the recorded run lists no
instances at all, the Valkey git SHA is compared after the run instead. What
could not be compared, such as an instance recorded without a digest, is
printed as `Note: not verified: ...`; a bare `plan.json` has no provenance, so
nothing is verified.

Replay is not hermetic even when nothing drifted: the seed fixes each
component's random choices, not timing.

## diff

```bash
valkey-fuzzer diff examples/valkey/differential_fork_vs_forkless.yaml --seed 7 --output diff.json
```

Runs one base scenario, with the same seed, under two or more variants and
compares evidence fields across them. `--seed` overrides the base seed, and
`--output` saves the differential report. Each variant gets a report folder
under `<root>/differential/<name>/`, next to `differential.json`.

```yaml
differential_id: fork-vs-forkless
base:
  scenario_id: snapshot-equivalence
  seed: 41
  experiment:
    id: valkey.snapshot-equivalence
    inputs: {operations: 3000, stable_keys: 1000}
  deployment:
    mode: standalone
    base_port: 7620
variants:
  fork:
    deployment.server_config: {bgsave-default-method: fork, forkless-infrastructure-enabled: "yes"}
  forkless:
    deployment.server_config: {bgsave-default-method: forkless, forkless-infrastructure-enabled: "yes"}
compare:
  equal:
    - {evidence_id: valkey.dataset.digest, source: dataset, field: digest}
    - {evidence_id: valkey.rdb.reload-verified, source: snapshot, field: content_digest}
    - {evidence_id: valkey.workload.ledger, source: writes, field: replies_digest}
  differ:
    - {evidence_id: valkey.bgsave.result, source: bgsave, field: after.rdb_last_bgsave_type}
```

| Field | Required | Meaning |
| --- | --- | --- |
| `differential_id` | yes | Name of the comparison and its folder. |
| `base` | yes | A scenario with `seed`, `experiment`, and `deployment`. |
| `variants` | yes | At least two. Each name (letters, digits, `-`, `_`) maps dotted paths to values, applied to a copy of `base`. Paths start with `experiment`, `deployment`, `artifacts`, `resources`, or `policies`. Under `experiment` only `experiment.inputs` and the paths below it are allowed, so every variant runs the same experiment. A `null` value removes a key. |
| `compare.equal` | one of `equal`, `differ` | Rules whose value must be the same in every variant. |
| `compare.differ` | one of `equal`, `differ` | Rules whose value must differ in every variant, which shows each variant took a different path. |
| `compare.require_status` | no | The outcome every variant must reach: `PASS` (the default), `FAIL`, or `SKIP_UNSUPPORTED`. |

A rule is `{evidence_id, source, field}`. `source` (a component `id`) and
`field` (a dotted path into the evidence value; a number indexes a list) are
optional. When several evidence items match, the rule compares the sorted
list of their values. A matching item without `field` makes the rule
`missing`. Before any variant runs, each rule's `evidence_id` must be declared
by its `source` (or, without one, by some component) in every variant's
plan; a rule that names undeclared evidence or an unknown source is rejected
with exit `2`.

Each variant's `scenario_id` is `<base scenario_id>-<variant>`, or
`<differential_id>-<variant>` when the base has no `scenario_id`. Every
variant compiles before any runs. The differential's outcome is, in order:
`FAIL` if a variant failed (unless `require_status` is `FAIL`), even when
another errored; `ERROR` if a variant errored; `SKIP_UNSUPPORTED` if a variant was unsupported
(unless `require_status` is `SKIP_UNSUPPORTED`); `FAIL` if a variant did not
reach `require_status` or a rule did not hold (`mismatch` or
`unexpectedly_equal`); `ERROR` if a rule could not be compared because a
variant recorded no value for it (`missing`); and `PASS` otherwise. The report has
`differential_id`, `status`, `summary`, `seed`, `variants` (each with
`status`, `summary`, `scenario`, and `artifact_dir`, its report folder), and
`comparisons` (each with `kind`, `rule`, `result`, and the `values` per
variant).

## minimize

```bash
valkey-fuzzer minimize report.json --output minimized.yaml --max-attempts 30
```

Needs a run report that contains the scenario (any command's `report.json`,
or a `--output` copy) and a `FAIL` or `ERROR` outcome. It tries one reduction
at a time, most aggressive first. Steps 1 and 2 are the local deployment's
(its provider's `reductions` hook); a deployment provider without the hook
leaves the deployment as it is:

1. fewer replicas, then fewer primaries: the experiment's minimum
   `replicas_per_primary` or `primaries` (0 without one), then half; a size
   the deployment provider refuses (a cluster of fewer than 3 primaries)
   does not compile;
2. dropping each `deployment.server_config` key and each
   `deployment.instance_configs` entry;
3. each positive numeric input the scenario sets, lowered to the input's
   minimum, then halved.

A reduction is kept when the failure signature still matches: the same
outcome, the same failing components with the same status and verdict each
(the error type and phase of a call that raised, or else its summary with
numbers and hex IDs masked), the same missing required events and evidence,
at least the same required evidence observed, and for `ERROR` the same error
type and phase. A run whose failing components report crashes of the system
under test (`crashes` in their diagnostics; `valkey.oracle.liveness` reports
each server crash by its assertion, panic, sanitizer finding, signal, or
fatal exit log line) has
those causes, numbers and hex IDs masked, as its whole signature: what failed
after the crash is left out, so the same crash in another experiment, or
with other components failing after it, is the same failure, and a run that
fails without crashing is another.
After a kept reduction the search starts over from the first candidate.
`--max-attempts` (default 30, at least 1) bounds the runs; a candidate that
does not compile does not count. Before anything runs, minimize compares this
environment with the report's `reproducer` as [replay](#replay) does, and
refuses (exit `2`) when anything differs, since a reduction that fails the
same way on other code proves nothing; `--allow-drift` minimizes anyway. The minimized scenario goes to `--output`, or to
stdout. Each candidate run gets a report folder under `<root>/minimize/`.

## cleanup-logs

```bash
valkey-fuzzer cleanup-logs 176 --force
```

Deletes `<seed>-*.log` (instance logs and captured output) from
`/tmp/valkey-fuzzer/logs`, or from `DIR/logs` with `--data-dir DIR`. Without
`--force` it lists the files and asks first. It fails (exit `2`) when the
prompt is declined or a file could not be deleted, such as another user's
in the shared `logs/`.

## sweep

```bash
valkey-fuzzer sweep --dry-run
valkey-fuzzer sweep --older-than 3600 --format json
sudo valkey-fuzzer sweep            # Linux: also iptables chains and tmpfs mounts
```

Removes what fuzzer processes that are gone left on this host. A fuzzer
killed with `SIGKILL` (or by a power loss) never cleans up, so every host
change it makes names its owner: the fuzzer's PID plus a hash of that
process's start time, written `<pid>-<start>`, so a reused PID is not taken
for the owner.

| Host change | Tag |
| --- | --- |
| deployment directory, `valkey.resource.tmpdir` directory, private server directory | a `.valkey-fuzzer-owner` file (JSON: `owner`, `pid`, `kind`, `label`, `created_at`) |
| `valkey-server` process, and the children it forks (`valkey-rdb-bgsave`, `valkey-aof-rewrite`, ...), which outlive it | `valkey-fuzzer-owner=<owner>` in its process title (`--proc-title-template`, which scenarios may not set) |
| iptables chain (`valkey.fault.network-partition`) | the name `VF-<owner>-<hash>` |
| tmpfs mount (`valkey.resource.tmpfs`) | the mount point `<tmp>/valkey-fuzzer-mounts/<owner>/<name>`; an empty one (made before its mount) is removed too |

The sweep lists every tagged item whose owner is no longer running and,
without `--dry-run`, kills the servers (`SIGKILL`), deletes the chains (and
their jumps from `INPUT`), unmounts the mounts, and removes the directories,
in that order. It never touches anything untagged, anything whose owner is
running, or anything whose owner it cannot rule out; it re-checks a server's
tag right before killing it and a directory's tag right before removing it
(one whose tag changed in between is reported as `skipped`, with the reason),
and refuses a directory that reaches into another filesystem. A directory
counts only when it and its tag file belong to the user running the sweep
(root too), so a tag another user wrote is never acted on. A sticky one never
counts, whatever tag is in it: it is a [shared directory](#report-folders),
such as `/tmp/valkey-fuzzer` or its `runs/`, holding every user's runs. Before removing a
directory it makes each one inside writable through a descriptor opened
without following symlinks, so a symlink swapped in never has its target
changed.

| Option | Meaning |
| --- | --- |
| `--dry-run` | List, remove nothing. |
| `--older-than SECONDS` | Only leftovers at least this old (servers by process age, directories by their tag, mounts by their owner directory); a chain has no age, so it is left alone. |
| `--root DIR` | Look for tagged directories in the children of `DIR` instead of the defaults (`/tmp/valkey-fuzzer`, `/tmp`, the temporary directory); may be repeated. Use it for a custom `base_data_dir` or `valkey.resource.tmpdir` `parent`. Servers, chains, and mounts are found host-wide, servers only among the processes of the user running the sweep, in its PID namespace (a fuzzer in a container is left to its own sweep). |
| `--format text\|json` | JSON: `{dry_run, ok, leftovers: [{kind, target, owner, age_s, detail, action, reason}], unchecked}`; `kind` is `server`, `chain`, `mount`, `mount point`, or `directory`. |

Listing chains and unmounting need root. Without it, a leftover mount is
reported as `failed` and chains as not checked. The exit code is 0 when
everything found was removed (or with `--dry-run`), and 2 when anything could
not be. A deployment kept with `enable_cleanup: false`, and a
`valkey.resource.tmpdir` with `keep: true`, lose their tag when the run ends,
so the sweep leaves them. Instance logs under `<base_data_dir>/logs` are
evidence, not leftovers (`cleanup-logs` removes them).

## spec

```text
valkey-fuzzer spec check BUNDLE [--tla2tools JAR] [--workers N] [--require-approved] [--output FILE]
valkey-fuzzer spec translate BUNDLE (--control NAME | --trace TLC_OUTPUT --config CFG) --out DIR [--variants N] [--seed N]
valkey-fuzzer spec conform BUNDLE RUN... [--tla2tools JAR] [--group-by INPUT,...] [--output FILE]
```

Checks a TLA+ spec bundle (`formal/<protocol>/`, or a bundle directory),
translates its counterexamples into scenarios, and checks its claims on real
runs. `--tla2tools` defaults to `$TLA2TOOLS_JAR`. `translate` takes the
catalog option (`--experiment-dir`) to compile what it
writes. Exit codes: 0 pass, 1 a gate failed or a claimed property was
violated, 2 a harness error (no java or jar, a bundle that does not exist, a
SANY or TLC run that never started or was cut short by `--timeout`, a run
without mapped evidence or with a mapped record it cannot apply, or a
`translate` whose experiments do not load, whose TLC run timed out, or that
has no counterexample). A failed gate outranks an error. See [Spec bridge](spec-bridge.md).

## Run reports

`report.json`, `run --output`, and `replay --output` hold one run report.
Every key below is always present, and
`replay` and `minimize` reject a report (or a reproducer) that lacks one.

| Key | Contents |
| --- | --- |
| `generated_at` | UTC timestamp |
| `status`, `summary` | The outcome and its one-line summary. |
| `scenario` | The scenario as run, including a `--valkey-binary` override. A replay records the scenario of the report it replayed with its own `--valkey-binary` applied (also under `provenance`), or `null` for a bare plan. |
| `outcome` | The full outcome: `plan` (the execution plan), `status`, `summary`, `component_results`, `evidence`, `diagnostics`, `started_at`, `ended_at`, and `valkey_sha`. |
| `coverage` | The run's coverage `key` (a digest of its `tuple`), the `tuple`, `status`, and the evidence IDs observed. |
| `reproducer` | `python`, `platform`, `experiment` (`id`, `digest`), `fuzzer_digest`, `components` (`component_id`, `role`, `implementation`), `deployment_provider` (`provider_id`, `implementation`, `implementation_digest`), `plan_digest`, and `valkey_sha`; for a generated scenario also `generated` (`seed`, `experiment`, `attempt`, `excludes`, `boundary`, `profile`). |
| `provenance` | For a replay: `source`, `verified`, `drift`, `drift_allowed`, `not_verified`, `valkey_binary_override`, and `artifact_overrides` (the `--artifact` paths by artifact name). `null` otherwise. |

Each entry of `outcome.component_results` has `component_id`, `status`,
`summary`, `evidence`, `diagnostics`, `started_at`, and `ended_at`; a
long-running component's teardown result is under `diagnostics.teardown`.
`outcome.diagnostics` holds:

- `deployment`: the deployment ID, mode, addressing, `valkey_sha`, and each
  instance's ID, role, shard, host, port, PID, binary, and `binary_sha256`;
- `events`: every published event, with `name`, `sequence`, `timestamp`,
  `source`, `payload`, and `committed` (whether its dispatch completed; only
  committed events count as observed);
- `environment_report`: artifact and resource acquisition and release;
- `obligations`: the `planned` changes, and the [undo
  obligations](contracts.md#fault-ownership) `undone` and `still_active`;
- when the run did not pass, the cause: `missing_required_events`,
  `missing_required_evidence`, `required_component_failures`,
  `untrusted_component_failures`, `contract_violations`, `open_obligations`,
  `cleanup_errors`, `abandoned_calls`, or `error`.

A failed state validation (of `valkey.oracle.deployment-state` or a chaos
action's wave) records
`failed_checks`, `error_messages`, `inconclusive` (nothing was judged), and each check's result under `checks`,
plus `topology`: every instance's live role, shard, primary, status, and
slot count as the checks saw it. A failed data-consistency check names up to
ten missing test keys in its error and lists up to five wrong keys under
`checks.data_consistency.examples`, each with what every instance held.

A coverage tuple holds:

| Tuple field | Holds |
| --- | --- |
| `experiment` | the experiment ID |
| `deployment` | `mode`, `provider`, `roles` (how many planned instances have each role), and `shards` |
| `config_mix` | a digest of each planned instance's role and effective server config, and the startup overlays |
| `deployment_mix` | a digest of the deployment document but for its provider's host fields (the local deployment's `base_port`, `base_data_dir`, `instance_data_roots`, `loopback_block`, `enable_cleanup`), so another binary is another combination and other ports are not |
| `inputs` | the resolved inputs |
| `artifact_mix` | a digest of the artifact declarations with their config |
| `resources`, `policies` | resource provider IDs and policy types |
| `workloads`, `actions`, `faults`, `observers`, `oracles` | the component registration IDs of each role |
| `resource_mix`, `policy_mix` | digests of the resource and policy config |
