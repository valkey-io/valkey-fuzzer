# valkey-fuzzer

valkey-fuzzer runs seeded fault-injection tests against Valkey. Most tests are
YAML experiments that combine workloads, actions, faults, observers, and
oracles. The same runner supports standalone and cluster deployments, with or
without replicas.

A scenario names an experiment, its inputs, a deployment, and a seed. The
runner starts local `valkey-server` processes, runs the experiment, and
reports one outcome: `PASS`, `FAIL`, `ERROR`, or `SKIP_UNSUPPORTED`.
`valkey-fuzzer run --random` generates scenarios from a seed for any
experiment, cluster chaos (failovers with process kills) included.

## Prerequisites

- Python 3.9 or later.
- A `valkey-server` build, on `PATH` or passed with `--valkey-binary`. Most
  experiments probe the build before they start and report `ERROR` without
  one. Only cluster chaos, cluster partition, and the disk-full experiment
  fall back to cloning `valkey-io/valkey` and building it under
  `/tmp/valkey-build`, which needs git, make, and a C compiler.
- Linux and root only for the experiments that change the host (see
  [Safety](#safety)). Everything else also runs on macOS.

## Quickstart

```bash
git clone https://github.com/valkey-io/valkey-fuzzer.git
cd valkey-fuzzer
python -m pip install -e .
```

List the installed experiments, then check a scenario without starting any
servers:

```bash
valkey-fuzzer list experiments
valkey-fuzzer validate examples/valkey/aof_durability.yaml
```

Run it, with `valkey-server` on `PATH` (or add `--valkey-binary PATH`). This
starts a primary and a replica on ports 7430 and 7431, writes keys, kills the
primary with `SIGKILL`, restarts it from its append-only file, and checks
that no acknowledged write was lost. It takes about 20 seconds.

```bash
valkey-fuzzer run examples/valkey/aof_durability.yaml --output report.json
```

The command prints the outcome and the path of the run report
(`Report: .../report.json`), and exits `0` for `PASS`, `1` for `FAIL`, `2` for
`ERROR`, and `3` for `SKIP_UNSUPPORTED`. `--output` saves a copy of the
report. Run the recorded execution plan again:

```bash
valkey-fuzzer replay report.json
```

Replay uses the fuzzer and binaries installed now. It refuses to run when the
fuzzer, the experiment, the components, or the Valkey binary
differ from the recorded run, unless you pass `--allow-drift`, and it does not
reproduce timing.

Run one generated cluster chaos scenario (3 primaries with 1 replica each, a
process kill and a failover, about 35 seconds):

```bash
valkey-fuzzer run --seed 173 --experiment valkey.cluster-chaos
```

[Getting started](docs/getting-started.md) explains the output, the report
files, and what to try next.

## Safety

The fuzzer acts on the machine it runs on. Use a disposable host, VM, or
container, not a server that runs anything you care about.

- It starts real `valkey-server` processes on local ports (from 7000 by
  default, bound to loopback addresses, with protected mode off), sends them
  `SIGKILL`, `SIGTERM`, and `SIGSTOP`, and restarts them. It refuses to start
  when a planned port is already in use.
- It writes instance data and logs under `/tmp/valkey-fuzzer`, reports under
  the temporary directory, and may build Valkey under `/tmp/valkey-build`.
- On Linux as root, `valkey.bgsave-disk-full` mounts a tmpfs and fills it,
  and `valkey.cluster-partition` and the partitions of
  `valkey.replication-chaos` add iptables rules. Both remove what they added
  when the run ends, but a fuzzer process that is killed can leave a mount or
  a rule behind.
- Every server, data directory, iptables chain, and tmpfs mount the fuzzer
  creates is tagged with the fuzzer process that owns it. After a fuzzer was
  killed (`SIGKILL`, a power loss), `valkey-fuzzer sweep` kills its servers
  and removes its chains, mounts, and data directories; it never touches
  anything untagged or anything a running fuzzer owns (see
  [sweep](docs/cli.md#sweep)).

## Documentation

| Document | For | Contents |
| --- | --- | --- |
| [Getting started](docs/getting-started.md) | first-time users | install, first runs, output and report files, troubleshooting |
| [Concepts and glossary](docs/concepts.md) | everyone | scenarios, execution plans, component roles, events, evidence, and the terms these docs use |
| [Base contracts](docs/contracts.md) | everyone | the rules every run is held to: verdicts, the evidence contract, fault ownership, effective config, search profiles, deadlines, host state, replay |
| [Scenarios and experiments](docs/scenarios-and-experiments.md) | scenario and experiment authors | file formats, the local deployment, the Valkey experiments, random scenarios and search profiles |
| [Valkey components](docs/components.md) | experiment authors | every Valkey component and provider, with its config, declarations, and defaults |
| [Adding tests](docs/extending.md) | experiment and component authors | writing experiments, components, and providers, registering them, and testing them |
| [Command-line reference](docs/cli.md) | users and scripts | every command and option, exit codes, report folders and formats |
| [Cluster chaos and CI](docs/cluster-chaos-and-ci.md) | cluster chaos users, CI maintainers | running cluster chaos, kill safety, the GitHub workflows, reproducing CI failures |
| [Spec bridge](docs/spec-bridge.md) | model and experiment authors | TLA+ bundles: `spec check`, `spec translate`, `spec conform`, the assurance report |
| [Bug zoo](docs/bug-zoo.md) | experiment authors, soak maintainers | deliberately broken Valkey builds the experiments must catch |
| [Architecture](docs/architecture.md) | framework contributors | code layout, the compiler and runner, the local deployment provider, the wave engine |
| [TLA+ election model](formal/cluster-election/README.md) | Valkey cluster maintainers | the failover election model and its live counterpart |

## Contributing

Fork the repository, add focused tests (see
[Testing](docs/extending.md#testing)), and run the checks before you open a
pull request:

```bash
python -m pip install -e '.[dev]'
python -m pytest
ruff check .
ruff format --check .
```

Describe the code path you exercised, the oracles that judge it, and how to
reproduce a failure. Sign off every commit (`git commit -s`).

## License

BSD 3-Clause. See [LICENSE](LICENSE).
