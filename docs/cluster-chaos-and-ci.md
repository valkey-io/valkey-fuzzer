# Cluster chaos and CI

This page is for people who run cluster chaos, reproduce its failures, or
maintain the GitHub workflows. Cluster chaos is the `valkey.cluster-chaos`
experiment: waves of failovers and process kills on a Valkey Cluster,
validated after every wave. Like every experiment, it runs from a scenario
file or from generated scenarios; the scenario format is in
[Scenarios and experiments](scenarios-and-experiments.md), and the action's
config in [`valkey.action.cluster-chaos`](components.md#valkeyactioncluster-chaos).

- [Running cluster chaos](#running-cluster-chaos)
- [Scenario inputs](#scenario-inputs)
- [State validation](#state-validation)
- [Kill safety and verdicts](#kill-safety-and-verdicts)
- [PR-triggered runs](#pr-triggered-runs)
- [Scheduled and repository workflows](#scheduled-and-repository-workflows)
- [Reproducing a CI failure](#reproducing-a-ci-failure)

## Running cluster chaos

```bash
valkey-fuzzer run --random --experiment valkey.cluster-chaos                  # one random seed
valkey-fuzzer run --seed 42 --experiment valkey.cluster-chaos                 # seed 42
valkey-fuzzer run --random --experiment valkey.cluster-chaos --valkey-binary /path/to/valkey-server
valkey-fuzzer run examples/valkey/cluster_chaos.yaml                          # a scenario file
valkey-fuzzer batch --random valkey.cluster-chaos --first-seed 42 --seeds 10  # seeds 42 to 51
```

These are ordinary [`run`](cli.md#run) invocations, with its
[generated-scenario options](cli.md#generated-scenarios-run---random), report
folders, exit codes, and `replay`. A seed generates, through the
experiment's generator block and the `valkey.generator.waves` hook:

- a cluster of 3 to 16 primaries with 1 to 5 replicas each, on a random
  `base_port` from 7000 to 8000;
- one to four waves of one to three `failover`s of distinct shards (`force`,
  a timeout, and a delay each), four in five with a `kill` (`SIGKILL` or
  `SIGTERM` of a `primary`, `replica`, or `any` instance) before, with, or
  after it (once it finished). A run kills fewer instances than the
  cluster has replicas.

Every operation draws from its own random stream, `wave-<n>-op-<i>`: its
target before the wave, and while it runs whatever it picks (the replica a
failover promotes).

The operations of a wave run in parallel. After every wave the cluster is
validated (see [State validation](#state-validation)) with the killed
instances counted as stopped on purpose. A critical failure, or any failure
with `blocking_on_failure`, stops the remaining waves and fails the action;
teardown and the oracles still run. The `valkey.oracle.deployment-state`
oracle repeats the validation at the end, and `valkey.oracle.liveness` fails
the run if any instance that was not killed is down, or if any log or
captured output, including those of killed instances, holds a crash,
assertion, or sanitizer report.

## Scenario inputs

Every generated run writes its scenario as `scenario.yaml` in its run
folder; it looks like this one, and `valkey-fuzzer run` runs it again:

```yaml
scenario_id: cluster-chaos-example
seed: 4242
experiment:
  id: valkey.cluster-chaos
  inputs:
    waves:
      - - {op: kill, target: any, signal: SIGKILL}
        - {op: failover, shard: 0, delay_s: 1.5}
        - {op: failover, shard: 2, force: true, delay_s: 0.5}
      - - {op: failover, shard: 0, force: true}
        - {op: kill, target: replica, after: 0}   # once the failover finished
    validation: {blocking_on_failure: false}       # optional
deployment:
  mode: cluster
  primaries: 4
  replicas_per_primary: 2
  base_port: 7400
```

| Input | Fields |
| --- | --- |
| `waves` | Lists of operations that start together, each after its `delay_s` or, with `after: i`, once operation `i` of the wave finished: `failover {shard, force, timeout_s}` (at most one per shard per wave) and `kill {target, signal}` ([operations](components.md#valkeyactioncluster-chaos)). |
| `validation` | The [state validation](#state-validation) settings. |
| `keys_per_slot` | Keys written into every hash slot before the first wave. Default 5. |

The experiment needs at least one replica per primary.
[`examples/valkey/cluster_failover.yaml`](../examples/valkey/cluster_failover.yaml)
runs one graceful failover with no kills.

## State validation

The `validation` input configures the validation that runs after every wave
and at the end. Every key is optional:

| Key | Default | Meaning |
| --- | --- | --- |
| `check_replication` | `true` | Replica links are up and caught up. |
| `check_cluster_status` | `true` | Every node reports an acceptable `cluster_state` and the primaries hold quorum. |
| `check_slot_coverage` | `true` | Every node sees all 16,384 hash slots assigned, and no slot is served by two primaries at once (a split brain: one view gives it to two primaries not flagged failed, or two nodes each claim it). |
| `check_roles` | `true` | The live primary and replica counts match the plan, allowing for instances stopped on purpose, and no instance failed without being stopped. |
| `check_view_consistency` | `true` | The nodes agree on membership, roles, slot owners, each replica's primary, and each primary's config epoch. A view a gossip round behind a failover, which still gives a slot to the failed old primary, fails this check without being critical, so a retry sees it converge. About a node stopped on purpose, which can no longer correct what its peers hold, a view may lack it, see it failed, or keep a stale role, primary, epoch, or slots. |
| `check_data_consistency` | `true` | The test keys written before the first wave read back, and every replica holds every test key of its shard. |
| `check_logs` | `true` | Every node stopped on purpose was detected: some node's log, a stopped node's included, has it reach the quorum or receive the FAIL message about it. The shards that failed over or lost their primary logged the promotion. |
| `stabilization_wait` | 5.0 | Seconds to wait before validating; less than `validation_timeout`. |
| `validation_timeout` | 30.0 | Seconds for one validation, checked before each check: one that started finishes, within its own `timeout`s. |
| `blocking_on_failure` | `false` | Stop after any failed wave, not only a critical one. |
| `retry_on_transient_failure`, `max_retries`, `retry_delay` | `true`, 3, 2.0 | Retry a failed or inconclusive validation (below), a critical or repeated failure too: a cluster still failing over after a kill converges between attempts. |

Nested settings, each optional, with their defaults:

| Key | Fields |
| --- | --- |
| `replication_config` | `max_acceptable_lag` 10.0, `require_all_replicas_synced` `false`, `check_replication_offset` `true`, `min_replicas_per_shard` 1, `timeout` 10.0 |
| `cluster_status_config` | `acceptable_states` `[ok]` (list `fail` to accept Valkey's fail state), `timeout` 10.0 |
| `slot_coverage_config` | `timeout` 10.0 |
| `view_consistency_config` | `allow_transient_inconsistency` `true` (a validation whose views disagree is retried; `false` judges the first attempt), `timeout` 15.0 |
| `data_consistency_config` | `num_test_keys` 100, `key_prefix` `fuzzer:test:`, `timeout` 10.0 |

A failed validation records what each check found and the live topology
(see [Run reports](cli.md#run-reports)).

With `check_replication_offset`, a replica must reach its primary's
replication offset. A primary sends its replicas a 14-byte replication PING
every few seconds, so a replica read before its primary can look a PING
behind: such a replica is read again, until it reaches the offset its primary
reported or the query timeout passes, and only one that stays behind fails.
Offsets that could not be compared (a reply or offset missing, or the
replica's primary not a primary in the topology read, as a gossip round
behind a failover) leave the check inconclusive; a replica read before its
primary and ahead of it fails.

Every query waits the check's own `timeout`. An attempt that could not see
the deployment is inconclusive and judges nothing: an instance whose process
runs did not answer (`unanswered`), the `validation_timeout` ran out, a
check broke (it raised, or `validation_framework`), or a test key could not
be read. A check that saw Valkey do something wrong in the same attempt
still fails it. It is retried while attempts remain,
and a validation still inconclusive reports `inconclusive: true`, which the
oracles and the chaos actions report as `ERROR`, never `FAIL`. An instance
whose process is not running is no such gap: the checks judge it, and one
nobody stopped is a critical failure.

A replica in a sync is not judged until the sync ends: when a validation
fails while a reachable replica's link is down and the primary it follows
answers, the validation waits for every such replica's link to come up (up
to 60 seconds in all) and runs again without counting the attempt. Valkey
moves replicas to orphaned primaries by itself (replica migration), and a
replica resyncs after its primary failed over; during the full sync its link
is down and it still holds its old shard's keys. A replica that is still in
a sync after 60 seconds, or whose primary does not answer, is judged as it
is.

A failure still critical after the retries stops the remaining waves. It is
critical when slots lose coverage, the views show a split, the cluster
reports `fail` or loses quorum,
a shard lost all its replicas to failures that were not kills, the role
check finds a wrong primary count or an instance that failed without being
killed, or test keys are missing. Only the role check judges whether the
failed instances are the ones killed; the replication check fails a replica
that answers with its link down while its primary is live.

## Kill safety and verdicts

Kill targets are picked before the wave starts:

- a kill never takes a shard's last live member. Shards are the live ones: a
  node belongs to the shard whose hash slots it (or its primary) owns now, so
  a replica that migrated to another primary counts there, and a primary
  without slots holds no shard;
- a kill takes a primary only while the other live slot-owning primaries
  still form a majority of the planned shards (a dead primary counts toward
  that majority until a replica replaces it);
- a target is checked again against the live view right before the kill,
  because failovers in the same wave change roles. A failover holds its
  shard until it settled, and a kill holds the shard its target serves while
  it signals and waits for the exit, so a kill of a member of a shard that is
  failing over waits for the failover and is judged by the roles it left: a
  replica the failover just promoted is a primary by then, and a kill that
  would now take the last primaries of a majority is skipped.

A failover promotes only a replica that has synced with the shard's primary
(link up, no sync in progress, offset past 0), waiting up to its
`timeout_s` for one before Valkey gets its own `timeout_s` to settle the
shard: `CLUSTER FAILOVER FORCE` checks no data by design, and a
replica that just migrated to an orphaned primary would win it with an empty
dataset. It never promotes a replica that was killed on purpose, even one
that still answers: a replica sent `SIGTERM` keeps serving until it exits,
and Valkey still accepts `CLUSTER FAILOVER` on it. Promoting one and then
losing it before the old primary's first sync hits a Valkey bug (a primary
demoted that way never fails over again);
`tests/test_live_cluster_demoted_primary.py` reproduces that state on
purpose (`pytest -m live -k demoted_primary`).

These checks apply to every target, including named instances: an unsafe
kill is skipped (`skipped` in its record, listed in the wave's `skipped`),
and its wave does not count as one that killed a node. If no planned kill
lands, the action is `SKIP`. A kill with no target at all (a named instance
that is no longer live, or no live instance of the role) is an `ERROR` (a
`FAIL` if the run also found a failure), and so is a kill the operating
system refused.

A failover is done once its own shard has settled: one live primary that is
not the old one, and every live replica of the shard not stopped on purpose
linked to it; the other shards' links are the state validation's to judge.
A target that exited before its kill is a failure, not an instance stopped on
purpose. A failed failover is tolerated only when a kill could have caused it:
a successful kill that landed before the failover finished, either in the
same shard or of a primary in the same wave. The final validation then decides
whether the cluster recovered. Any other failed failover and a failed wave
validation fail the run.

## PR-triggered runs

The reusable workflow
[`.github/workflows/valkey-pr-fuzzer.yml`](../.github/workflows/valkey-pr-fuzzer.yml)
builds Valkey from any ref (including a PR merge ref such as
`refs/pull/123/merge`), runs a configurable number of
`run --random --experiment valkey.cluster-chaos` runs in parallel (default
10), uploads each run's files, and writes a summary.
Every run finishes even when another fails. A run fails when the fuzzer exits
non-zero, that is, when its outcome is not `PASS`, and the workflow fails when
any run fails or is missing.

### Manual trigger

In **Actions > Valkey Cluster PR Fuzzer**, choose **Run workflow** and fill in:

| Input | Meaning |
| --- | --- |
| `pr_link` | A PR URL such as `https://github.com/valkey-io/valkey/pull/123`. Fills in the repository, ref, PR number, and PR URL. |
| `valkey_repository`, `valkey_ref` | Without `pr_link`: the repository (`valkey-io/valkey` or a fork) and the ref to test (`refs/pull/123/merge`, `unstable`, or a commit SHA). |
| `pr_number`, `pr_url` | Optional, shown in the summary. |
| `run_count` | Number of runs, from 1 to 256 (GitHub's limit for one matrix). Default 10. |
| `label_name` | Shown in the summary. |
| `fuzzer_repository`, `fuzzer_ref` | Optional: test another fuzzer repository or ref. |

### Label trigger

To run the fuzzer when a label is applied to a Valkey PR, add a caller
workflow to the Valkey repository:

```yaml
name: Label-triggered Valkey Cluster Fuzzer

on:
  pull_request_target:
    types: [labeled]

permissions:
  contents: read

jobs:
  pr-fuzzer:
    permissions:
      contents: read
      issues: write
      pull-requests: write
    if: github.event.label.name == 'run-cluster-fuzzer'
    uses: valkey-io/valkey-fuzzer/.github/workflows/valkey-pr-fuzzer.yml@main
    with:
      fuzzer_repository: valkey-io/valkey-fuzzer
      fuzzer_ref: main
      valkey_repository: ${{ github.repository }}
      valkey_ref: refs/pull/${{ github.event.pull_request.number }}/merge
      pr_number: ${{ github.event.pull_request.number }}
      pr_url: ${{ github.event.pull_request.html_url }}
      label_name: ${{ github.event.label.name }}
      run_count: 10
```

A caller passes `fuzzer_repository` and `fuzzer_ref`: in a called workflow,
`github.repository` is the caller's. Pointing the workflow ref and
`fuzzer_ref` at `main` runs the current fuzzer. Pin both to one tag or commit SHA for reproducible runs. The result
shows on the PR as a normal check. When the PR is in the repository that runs
the workflow, as with this caller, a sticky comment (marked
`<!-- valkey-cluster-fuzzer-results -->`) also carries the latest summary; a
manual run in the fuzzer repository for a Valkey PR skips the comment. A PR without a merge ref (for example, one with
conflicts) fails at the Valkey build. A cleanup job in the caller can remove
the label afterwards, so maintainers rerun by applying it again.

### Artifacts

Each run is a [batch](cli.md#batch) of one fresh `valkey.cluster-chaos` seed
(`batch --random valkey.cluster-chaos --seeds 1`), like the scheduled run, and
uploads `valkey-pr-fuzzer-run-N` with:

| File | Contents |
| --- | --- |
| `console.log` | The fuzzer's output. |
| `metadata.json` | Status, stage (`setup` or `fuzzer`), scenario ID, seed, outcome, duration, failed components, summary, Valkey SHA. |
| `batch/` | The [batch folder](cli.md#the-batch-folder): `summary.json` and the run folder `runs/<id>/` with `plan.json`, `report.json`, `scenario.yaml` (rerun it with `valkey-fuzzer run`), the components' files, and `logs/`, every instance's log. |

The full output is also in the **Execute fuzzer run** step of each
**Random fuzzer run N** job.

## Scheduled and repository workflows

| Workflow | Trigger | Does |
| --- | --- | --- |
| [Valkey Fuzzer Cluster Chaos Every 4 Hours](../.github/workflows/fuzzer-run.yml) | every four hours, manual | A [batch](cli.md#batch) of three fresh `valkey.cluster-chaos` seeds under the `nightly-soak` profile (`batch --random valkey.cluster-chaos --seeds 3`), on a build of Valkey `unstable` that the workflow makes and passes with `--valkey-binary`. Uploads `fuzzer-run-artifacts-<run id>`, which holds the batch folder `batch/`, and writes the batch status, each run's scenario, seed, status, summary, Valkey SHA, failed components, and rerun command, and the failure signatures to the job summary. |
| [Valkey Fuzzer PR Batch](../.github/workflows/fuzzer-pr-batch.yml) | dispatch (valkey-ci-agent) | Takes `scenarios` (a JSON object mapping a name to scenario YAML, at most 60,000 characters), optional `spec_bundles` (below), `valkey_repository`, `valkey_ref`, and `valkey_sha`, which must be a full commit SHA: 40 lowercase hex characters, never a short or uppercase one; builds exactly `valkey_sha`; runs the scenarios through `batch --scenarios` with the `candidate` profile on two workers; and uploads the batch folder as `fuzzer-batch-<run id>`. A scenario that names a binary of its own (`deployment.valkey_binary`, `deployment.instance_binaries`, or `artifacts`) is refused. The token is read-only, and inputs reach the scripts only through environment variables. |
| [Valkey Experiments Live](../.github/workflows/valkey-experiments-live.yml) | daily, manual | Builds Valkey `unstable` and 8.1 and runs `tests/test_live_valkey_experiments.py` and `tests/test_live_spec_register.py` as the runner user, with java and the pinned `tla2tools.jar`, so the spec bridge's conformance checks run too. Only the tmpfs and iptables tests run as root, in a separate step that fails if either skips. |
| [Tests](../.github/workflows/test.yml) | push and PR to `main` | Unit tests on Python 3.9 to 3.14, a check that the wheel ships every file under `formal/`, and in a parallel job on Python 3.12 the server tests and a live experiment subset (with the spec bridge's register bundle end to end) against the Valkey commit in `zoo/valkey-commit`, with java and the pinned `tla2tools.jar` for the tests backed by TLC. |
| [Lint](../.github/workflows/lint.yml) | push and PR to `main` | `ruff check` and `ruff format --check`. |
| [TLA+ Model Check](../.github/workflows/tla-model-check.yml) | push and PR to `main`, manual | Runs [`valkey-fuzzer spec check`](spec-bridge.md#spec-check) on every bundle under `formal/` (safe configs must find no error; every negative control must violate its property) and the spec bridge's TLC tests, and uploads the assurance reports. |

`spec_bundles` carries the [spec bundles](spec-bridge.md#bundles) that
scenarios translated from a bundle need, when the bundle brings its own
data-only experiments: a JSON object mapping a bundle name (lowercase letters,
digits, `-`) to `{path in the bundle: file text}`, at most 60,000 characters.
The paths may be `claims.yaml`, `model.tla`, `mapping.yaml` (all three
required), `<name>.cfg`, `negative-controls/<name>.cfg` or `.tla`, and
`experiments/<name>.yaml`; anything else, a YAML file that is not a mapping,
or a bundle that fails the bundle checks is refused before anything is built.
The workflow writes each bundle to its own directory, runs the batch with
`VALKEY_FUZZER_SPEC_PATH` naming their parent, so a component's `spec:` finds
the bundle by name, and passes `--experiment-dir <bundle>/experiments` for each
bundle that has experiments.

valkey-ci-agent's fuzzer monitor reads one run report per run. In the
scheduled run's artifact, a PR fuzzer run's (`batch/`), and the dispatched PR
batch's, that is the batch folder:
`summary.json` gives the batch's `status` and lists every run under `runs`,
and each entry's `report` (a path relative to the batch folder, such as
`runs/random-valkey.cluster-chaos-s1234/report.json`) is that run's own
[run report](cli.md#run-reports); it is `null` for a run that left none,
whose entry still carries its `status` and `summary`. The failure signatures
are under `signatures`.

Every third-party action in these workflows is pinned to a commit SHA.
`tests/test_ci_workflows.py` guards these workflows: the reusable workflow's
inputs and jobs, that every `valkey-fuzzer` command in them parses with the
current CLI, that referenced files exist, that the PR fuzzer's
metadata script reads a real run report, that only the root-only live tests run as
root, and that every action is pinned.

## Reproducing a CI failure

1. Download the run's artifact (`valkey-pr-fuzzer-run-N`,
   `fuzzer-run-artifacts-<run id>`, or `fuzzer-batch-<run id>`); the run
   folders are `batch/runs/<id>/` in the first two and `runs/<id>/` in the
   last, whose artifact is the batch folder itself.
2. Build the same Valkey commit. The SHA is `valkey_sha` in `metadata.json` or
   under `outcome` in the run folder's `report.json`.
3. Run the scenario again with that build:

   ```bash
   valkey-fuzzer run batch/runs/<id>/scenario.yaml --valkey-binary /path/to/valkey-server --verbose
   ```

   or replay the recorded execution plan:

   ```bash
   valkey-fuzzer replay batch/runs/<id>/report.json --valkey-binary /path/to/valkey-server
   ```

   Instance logs are in the run folder's `logs/`.

`--valkey-binary` tells replay that the binary is chosen on purpose, so a
build at a different path is not drift. Replay still refuses to run if your
fuzzer, experiment, or components differ from the CI run's; check out
the fuzzer commit the workflow used, or pass `--allow-drift` and keep the
difference in mind. A rerun repeats the same cluster, failovers, and kill
settings, but not the timing, so a race may need several runs.
