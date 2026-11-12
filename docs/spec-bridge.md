# Spec bridge

The spec bridge links a TLA+ model to real fuzzer runs. It does three things:
it model-checks the model's claims, turns a counterexample into ordinary
scenarios, and checks the claims on states observed in real runs. It does not
prove that Valkey implements the model.

- [Bundles](#bundles)
- [spec check](#spec-check)
- [spec translate](#spec-translate)
- [spec conform](#spec-conform)
- [Writing a bundle](#writing-a-bundle)
- [Onboarding a protocol with data only](#onboarding-a-protocol-with-data-only)

All three tools need Java 11 or later and `tla2tools.jar` (pass
`--tla2tools`, or set `TLA2TOOLS_JAR`; CI pins v1.7.4 with TLC 2.19). Each
writes a JSON report with `--output`, and each takes `--timeout SECONDS`
(default 3600, one hour): once the command has run that long, SANY or TLC is
killed with its whole process group and that run fails. Reports and translated scenarios name the
bundle by its protocol, its files by their paths inside the bundle, and the
jar by its file name, so the same bundle gives the same reports anywhere.
The exception is an error about where a bundle is (one that cannot be
found or read, or a protocol name that finds another copy): its message
names the paths involved. The code is in
`valkey_fuzzer/spec/`.

## Bundles

A bundle describes one protocol:

```text
<protocol>/
  claims.yaml          properties, scope, assumptions, bounds, exclusions, approval
  model.tla            MODULE model
  *.cfg                expected-safe TLC configs
  mapping.yaml         model actions and variables <-> implementation evidence
  negative-controls/   <name>.cfg, plus <name>.tla for a mutated model
  experiments/         optional: data-only experiments the counterexamples run in
```

A bundle is named by a path to its directory, or by its protocol name (no
`/`). A name is looked up in each directory of `$VALKEY_FUZZER_SPEC_PATH`
(separated like `PATH`), then in the bundles that ship with valkey-fuzzer:
`formal/` of a checkout, which the wheel installs as the package
`valkey_fuzzer.formal`. A name never means a directory of the current one
(write `./<name>` for that), so a scenario reads the same bundle wherever it
runs. The same rule applies everywhere a bundle is named: the `spec` commands,
and the `spec:` field of an experiment's components. A component that reads
the mapping when it runs also records the mapping's SHA-256 in the plan
(`spec_sha256`), so an edited mapping changes the plan and its digest, and a
run whose mapping no longer matches its plan stops with `ERROR`. The bundles ship in the wheel because a
shipped experiment that names one (`spec: cluster-election`) must work in any
install; the search path is for bundles that live elsewhere, such as drafts
and fixtures. Name a bundle's directory after its protocol.

[`formal/cluster-election/`](../formal/cluster-election/README.md) is the
failover election; its experiment ships with the Valkey experiments.
`tests/fixtures/spec/register/` is a toy register that brings its own
experiment and runs on Valkey (see
[Onboarding a protocol with data only](#onboarding-a-protocol-with-data-only)).

**`claims.yaml`** holds these fields:

- `protocol` and `summary`;
- `status`: `draft` or `approved`. An approved bundle also needs
  `approved_by`, the maintainer who approved it. Drafts written by an AI
  always stay `draft`;
- `scope`: the Valkey files and functions the claims cover;
- `properties`: each has a `name` (an invariant or property defined in the
  model), `kind` (`invariant` or `property`), `statement`, and `holds_when`
  (the constant values under which the property is claimed). It also lists
  `negative_controls`, the controls that must violate it, and `conform`:
  `true` (invariants only), or `{excluded: <why>}` when observed states
  cannot decide the property;
- `targets`: named state predicates of the model that a counterexample
  reaches. `conform` counts the runs that reach each one;
- `assumptions`, `bounds` (the constants every safe config must use), and
  `exclusions` (what the model leaves out).

**`mapping.yaml`** is the only place the implementation patterns live.
Experiments, observers, and oracles read them from here.

- `evidence.<name>`: one kind of evidence. Each entry is either a fuzzer
  `event` (a name or a glob, with an optional `where` payload filter) or a
  `sample: true` state sample (INFO or CLUSTER fields from a trace
  observation). `log` is the line pattern that is published as the event.
  `fields` lists the captured fields and their types: `int`, `str`, `bool`
  (a JSON boolean or the word `true` or `false`), `node` (a `str` that names
  a node), or `node_id` (a node ID, as the trace header's `nodes.<node>.node_id`
  records it), which resolves to a node through that header.
  A field can also have a `path` and a `default`. An event entry can name the
  experiment `component` that publishes it; only events from that component
  (their `source`) are then this evidence. `after: {evidence: <name>, match:
  {<its field>: $field}}` says the record follows a record of that evidence
  whose fields match: conform holds it until that record is applied, and a
  record whose match never appears is one it cannot apply.
- `action`: the model action this evidence shows. `step` says which model
  steps produce it (`changes` or `unchanged`, a variable) and which
  variables' changed key is the acting process (`process`). `translate`
  uses `step`.
- `updates`: how this evidence changes the observed state. Each update is
  one of `set`, `max`, `add`, `remove`, `clear`, or `incr` on a state
  variable, with an optional `key` and `value`. In those, `$field` is a
  captured field, and `$field@Set` is that node's model value in the process
  set `Set`.
- `unobservable`: the model actions that no evidence shows, each with the
  reason.
- `processes`: the model constants that are sets of nodes, their model-value
  `prefix`, and the `members` rules that say which nodes belong to each set:
  an `evidence` and its `field` that names the node, an optional `where`
  filter, and `require`, the fields that must be present and not empty.
- `state`: every model variable, either `over` some process sets with an
  `init` value, or `hidden: <TLA+ expression>` with a reason.
- `constants`: every other model constant, as a literal `value`, as a
  per-node `config` that `equals` a value (read from the trace header), as a
  scenario `input` that `equals` a value (for an experiment that sets the
  configuration from that input), or as the `max_of` some state variables,
  never below `at_least`. A constant can say `why` it has that form.
- `scenario`: what `translate` writes. It names the experiment, maps
  deployment fields and inputs to model constants (`count`, or `constant`
  with a `map`), and lists the `faults`: the model action, the evidence it
  follows (`after`), the experiment's fault `component` that injects it, the
  experiment input that sets the trigger occurrence (`occurrence_input`;
  without one the fault fires on the first occurrence), and `target:
  same-process`. It also lists the timing `variants`; `occurrence_shift`
  needs an `occurrence_input` on every fault.

**`experiments/`** holds data-only experiment documents, in the format of
[Scenarios and experiments](scenarios-and-experiments.md), composed only of
installed components. `spec translate` and `spec check` load them next to the
installed experiments; `validate` and `run` load them with
`--experiment-dir <bundle>/experiments`. Their components name the bundle by
protocol (`spec: <protocol>`), so the protocol name must find this bundle.

## spec check

```bash
valkey-fuzzer spec check cluster-election --tla2tools tla2tools.jar --output assurance.json
```

This command runs SANY, then TLC on every safe config and on every negative
control, and then compiles the scenarios the controls' counterexamples
translate into. It takes `--experiment-dir` like `run`.
A check fails (exit `1`) if any gate fails. It is an error (exit `2`), not
a failure, when the bundle does not exist, or when SANY or TLC never started
(a corrupt jar) or was cut short by `--timeout`: such a run says nothing
about the model. A failed gate outranks an error.

| Gate | Passes when |
| --- | --- |
| `bundle` | `claims.yaml` and `mapping.yaml` parse and agree with the model text. |
| `parsed` | Every module extends or instantiates only the standard modules (`Bags`, `FiniteSets`, `Integers`, `Naturals`, `RealTime`, `Reals`, `Sequences`, `TLC`) and the bundle's own, and SANY accepts `model.tla` and every mutated control module. |
| `claims_checked` | Every property is checked by a safe config whose constants meet its `holds_when`, and every bound equals the safe configs' value. |
| `model_checked` | TLC finds no error in any safe config. |
| `negative_controls` | Each control's TLC run reports its named property as violated. A control that passes means the claim cannot fail as configured (vacuous). |
| `experiment` | Each control's counterexample translates into scenarios of the mapped experiment that compile, and each mapped fault is a fault component of the compiled plan. A counterexample without a mapped fault action is listed as `no-fault`; at least one control must translate. A bundle with its own `experiments/` must be the bundle its protocol name finds. A mapping without a `scenario` section passes with a note. |
| `mapping_complete` | Every action TLC explored is mapped to evidence or declared unobservable, and every mapped action exists. An `error` when TLC reported no coverage to compare with. |
| `model_coverage` | TLC reported coverage; an `error` when it printed none, which says nothing about the bundle. The report lists, per action, the distinct and generated states, and the actions that `never_fired` in a safe config. |
| `approved_claim` | Always passes, and records the approval status. With `--require-approved`, it fails on a draft. |

The assurance report (`kind: spec-assurance`) records the tool jar's SHA-256
and TLC version, the claims (status, scope, properties, assumptions, bounds,
exclusions), the mapping's SHA-256, and one entry per gate. Each TLC entry
has its exit code, its violation, its state counts and depth, its trace
length, and the output tail when it fails. A run cut short by the time limit
is marked `timed_out`; it, or a run whose tool never started, gives its gate
the status `error`, the report gets an `error`, and the check stops at that
gate. The `scope` field states the
limit: this is evidence about the model within its bounds. The
[TLA+ Model Check](../.github/workflows/tla-model-check.yml) workflow runs
`spec check` on every bundle and uploads the reports.

## spec translate

```bash
valkey-fuzzer spec translate cluster-election --control vote-before-save --out scenarios/
valkey-fuzzer spec translate BUNDLE --trace tlc.out --config the.cfg --out scenarios/
```

The command takes TLC's text counterexample, either by running a negative
control or from a saved TLC output. (The pinned TLC 2.19 has no
`-dumpTrace json`.) It writes one direct scenario, up to `--variants` nearby
variants, and `translation.json`. Each scenario's header and
`translation.json` name the bundle by protocol and the config by its path in
the bundle. The bundle's own `experiments/` are loaded with the installed
experiments and `--experiment-dir`. For each fault step, the scenario
triggers the fault on the last occurrence of the mapped trigger evidence
before that step, counted among all of that evidence's steps; with
`same-process`, only occurrences by the fault's target count. The
translation lists the model steps between the trigger and the fault. The
real fault fires when the trigger is seen, so those steps must fit in that
gap. The variants explore the gap by shifting the occurrence and by setting
the timing inputs. Every scenario compiles before it is written, each mapped
fault's `component` must be a fault of the compiled plan, and variants that
compile to the same inputs are dropped. A counterexample that takes some of
the mapped fault actions but not all is refused: the experiment's other
fault components would still fire, so the scenario would not follow it.

## spec conform

```bash
valkey-fuzzer spec conform cluster-election RUN_DIR... --group-by save_behavior,election --output conform.json
```

The command reads run folders, `report.json` files, directories of them, or
`trace.jsonl` files. When a run folder has a trace observer's
`components/*/trace.jsonl`, the command uses it; otherwise it uses the
report's events. In a directory every run folder (one with `plan.json` or
`report.json`) counts: a run killed before it wrote its report counts by its
trace, and a folder whose report cannot be read, or that left neither a
report nor a trace, is an error for that run, never left out. It replays each run's records through the mapping into
observed states. TLC then evaluates the `conform` properties and the
targets on every state, through `INSTANCE model WITH <var> <- Trace[k].<var>`,
so the definitions come from the model itself.

The result for each property is `holds` or `violated`, with the first
failing step. A property is `claimed` for a run only when the constants
derived from the run meet its `holds_when`. A violation outside the claim is
reported, but it does not fail the check. A run with no mapped evidence is an
error, never a pass, and so is a run with a mapped record the mapping cannot
apply (a field missing or of the wrong type, a `node_id` the trace header
does not list), since the states that record would show are never checked.
An update keyed on a node outside the process set it ranges over (a sample of
a replica updating a primary's variable) is skipped with a note. The report also gives live action coverage (how many
runs observed each action, plus the unobservable actions), the targets
reached, and the `--group-by` tables. A run whose trace recorded a different
mapping digest gets a drift note.

The report says what this command checks: `observed-state check and action
coverage`. It does **not** check refinement. Consecutive observed states are
not checked against the model's `Next` with stuttering and hidden steps.
That needs per-action guards on partial observations, and it is future work.

## Writing a bundle

To draft a bundle, by hand or with an AI, follow these steps:

1. Write `model.tla` (`MODULE model`) with named actions in `Next`, the
   invariants, and the target predicates.
2. Write one safe config, plus a negative control for every property: a
   config, or a mutated module, whose TLC run violates exactly that
   property. List only that property in the control's config. A mutated
   module changes actions, never the property: it must keep `model.tla`'s
   definition of the property, and of every operator the property uses, word
   for word, or take them with `EXTENDS model`. Otherwise a control could
   define the property as `FALSE` and pass while the model's own property
   is vacuous, so the bundle is rejected.
3. Write `claims.yaml` with `status: draft`.
4. Write `mapping.yaml`: map every variable and every constant, and every
   action to evidence or to `unobservable`. An experiment's
   `valkey.observer.log-trigger` rule without a `pattern` takes the mapping's
   pattern for its `publish` event (`spec: <protocol>`).
5. If no installed experiment can inject the faults the mapping names, add
   one under `experiments/` (see below).
6. Run `valkey-fuzzer spec check` until every gate passes. Then run
   `spec translate --control` and `valkey-fuzzer validate` on the scenarios
   it writes.
7. A maintainer reviews the claims and the mapping, then sets
   `status: approved` and `approved_by`. Only then can
   `spec check --require-approved` be a required check.

## Onboarding a protocol with data only

A new protocol needs no fuzzer code: the bundle holds the model, the
mapping, and an experiment composed of installed components. The mapping
says which component realizes what: each fault names the fault `component`
that injects it, and each piece of evidence is an event that a component
publishes (optionally pinned with `component`), such as a
`valkey.observer.log-trigger` rule that publishes a log line with the
mapping's pattern. `spec check` compiles the experiment through the
counterexamples, so a bundle whose experiment cannot inject its faults fails
a gate before anything runs.

The register in `tests/fixtures/spec/register/` is the worked example. Its
model acknowledges writes, saves them, and may crash; the claim is that with
`SyncBeforeAck` an acknowledged write survives a crash, and the negative
control `ack-before-save` acknowledges before saving. Its experiment,
`experiments/toy.register.yaml`, runs that on one Valkey primary:

| Model | Experiment component | Evidence in `mapping.yaml` |
| --- | --- | --- |
| `Write`, `Ack` | `writes` (`valkey.workload.key-value`, preload only) | `writes.seeded`: Valkey acknowledged every write |
| `Crash` | `crash` (`valkey.fault.process-signal`, SIGKILL on `writes.seeded`, restart) | `crash.injected` |
| state after the crash | `reload` (`valkey.action.command` `DEBUG RELOAD`) and `reload-log` (`valkey.observer.log-trigger`, `spec: register`) | `register.loaded`, from `Done loading RDB, keys loaded: N` |
| `SyncBeforeAck` | input `appendonly` (`yes` with `appendfsync always`) | constant `{input: appendonly, equals: "yes"}` |
| the verdict | `data` (`valkey.oracle.ledger`) | every acknowledged write must survive |

End to end, from the repository root, with a `valkey-server` on `PATH`:

```bash
export VALKEY_FUZZER_SPEC_PATH=tests/fixtures/spec    # the register names itself: spec: register
EXPERIMENTS=tests/fixtures/spec/register/experiments
valkey-fuzzer spec check register --tla2tools tla2tools.jar --timeout 600
valkey-fuzzer spec translate register --control ack-before-save --tla2tools tla2tools.jar --out scenarios/
valkey-fuzzer validate scenarios/*.yaml --experiment-dir "$EXPERIMENTS"
valkey-fuzzer run scenarios/spec-register-ack-before-save.yaml --experiment-dir "$EXPERIMENTS" --artifact-dir runs/
valkey-fuzzer spec conform register runs/ --tla2tools tla2tools.jar
```

The translated scenario sets `appendonly: "no"`, so the run FAILs: the
crash loses the acknowledged writes, as the counterexample says it may.
`conform` reports `AckedDurable` violated outside the claim. The same
scenario with `appendonly: "yes"` PASSes, and `AckedDurable` holds within
the claim. `tests/test_live_spec_register.py` runs these steps.
