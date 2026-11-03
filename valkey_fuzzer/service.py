"""Run orchestration: compile, execute, report, replay, differentials, and minimization.

:class:`RunService` holds one frozen :class:`~valkey_fuzzer.catalog.Catalog`
with its compiler and runner, so compiling and executing always see the
same components and experiments, and does everything a command does with them: compiling
scenarios to execution plans, executing them, writing reports and reproducers, replaying a
report or plan with provenance and Valkey binary drift checks, and running
differentials and minimization through :mod:`valkey_fuzzer.differential` and
:mod:`valkey_fuzzer.minimize`. Many runs go through the batch runner
(:mod:`valkey_fuzzer.batch`), whose workers each execute one plan here.

It never parses arguments or prints. Methods take plain arguments and return
result objects, progress reaches the caller through optional callbacks, and
failures raise :class:`~valkey_fuzzer.reports.RunServiceError` subclasses (or the compiler's own
errors) that the command-line interface maps to exit codes. Nothing here knows
about individual experiments, components, or evidence IDs. The report files and
folders are :mod:`valkey_fuzzer.reports`; the reproducer and the replay drift
checks are :mod:`valkey_fuzzer.provenance`.
"""

import copy
import itertools
import os
import random
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Callable, Optional

from ._data import error_record, to_plain_data
from .catalog import Catalog, ExperimentDirectory, build_catalog
from .compiler import ScenarioCompiler, binary_fields, deployment_registration, with_valkey_binary
from .differential import Differential, run_differential
from .documents import merge
from .generator import Exclusion, Generated, find_profile, generate
from .hoststate import folder_name, normalize_artifact_root
from .minimize import failure_signature
from .minimize import minimize as minimize_scenario
from .plan import ExecutionPlan
from .provenance import replayed_drift, report_drift
from .registrations import DeploymentProviderRegistration
from .reports import (
    REPORT_FILE,
    SCENARIO_FILE,
    InputError,
    RunServiceError,
    as_run_report,
    export_scenario,
    load_document,
    make_report_folder,
    new_report_folder,
    read_scenario,
    recorded_run,
    run_report,
    write_document,
    write_plan,
)
from .results import OutcomeStatus, RunOutcome
from .runner import PreflightError, Runner
from .schema import load_yaml_mapping, plain_config


class ReplayDriftError(RunServiceError):
    """This environment differs from the recorded run, and drift was not allowed."""

    def __init__(self, message: str, drift: Sequence[Mapping[str, Any]]):
        super().__init__(message)
        self.drift = [dict(entry) for entry in drift]


# ---------------------------------------------------------------------------
# Results
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PreparedRun:
    """A scenario as it will be recorded and the execution plan compiled from it.

    ``notes`` are what the command should tell the user about how the
    scenario was prepared (a moved base port).
    ``generated`` is how a generated scenario was drawn (the run report's
    ``reproducer.generated``); None for a scenario read from a file.
    """

    scenario: dict[str, Any]
    plan: ExecutionPlan
    notes: tuple[str, ...] = ()
    generated: Optional[Mapping[str, Any]] = None


@dataclass(frozen=True)
class RunRecord:
    """One executed plan: its outcome, its run report, and the folder holding both."""

    outcome: RunOutcome
    report: dict[str, Any]
    report_dir: str

    @property
    def report_path(self) -> str:
        return os.path.join(self.report_dir, REPORT_FILE)


@dataclass(frozen=True)
class ReplayPlan:
    """A recorded run (or a bare plan) checked against this environment, ready to replay.

    ``plan`` already uses the ``valkey_binary`` override. ``provenance`` lists
    the drift found before the run and what could not be verified.
    """

    plan: ExecutionPlan
    scenario: Optional[dict[str, Any]]
    provenance: dict[str, Any]
    recorded: Optional[dict[str, Any]]

    @property
    def verified(self) -> bool:
        return self.recorded is not None

    @property
    def drift(self) -> list[dict[str, Any]]:
        return self.provenance["drift"]

    @property
    def not_verified(self) -> list[str]:
        return self.provenance["not_verified"]


@dataclass(frozen=True)
class ReplayResult:
    """A replayed run; ``late_drift`` is binary drift only the replay's own deployment could reveal."""

    record: RunRecord
    late_drift: list[dict[str, Any]]


@dataclass(frozen=True)
class SessionResult:
    """A differential report, and the folder holding it and every variant's report."""

    report: dict[str, Any]
    report_dir: str


@dataclass(frozen=True)
class Minimization:
    """A recorded run to minimize: the scenario to shrink and the failure signature to keep."""

    scenario: dict[str, Any]
    plan: ExecutionPlan
    reference: dict[str, Any]
    required_evidence: list[str]

    @property
    def needed(self) -> bool:
        """Only a failing or erroring run has a failure to preserve."""

        return self.reference["status"] in (OutcomeStatus.FAIL.value, OutcomeStatus.ERROR.value)


@dataclass(frozen=True)
class MinimizeResult:
    """The smallest scenario that kept the failure, the reductions taken, and every attempt."""

    scenario: dict[str, Any]
    accepted: list[str]
    attempts: list[dict[str, Any]]
    signature: dict[str, Any]
    # None when no candidate compiled, so none ran.
    report_dir: Optional[str]


# ---------------------------------------------------------------------------
# The service
# ---------------------------------------------------------------------------


def _new_seed() -> int:
    """A seed for a run that names none; the report records it."""

    return random.randint(0, 2**32 - 1)


class _Session:
    """A multi-run command's folder, created when the first run (or the summary) needs it."""

    def __init__(self, service: "RunService", kind: str, name: str, timeout: Optional[float]):
        self._service = service
        self._parent = os.path.join(service.report_root, kind)
        self._name = name
        self._timeout = timeout
        self.path: Optional[str] = None

    def folder(self) -> str:
        if self.path is None:
            self.path = new_report_folder(self._parent, self._name)
        return self.path

    def run(
        self,
        name: str,
        plan: ExecutionPlan,
        scenario: Mapping[str, Any],
        generated: Optional[Mapping[str, Any]] = None,
    ) -> RunRecord:
        """Execute one run of the session in its report folder ``name``, which must be unique within it."""

        report_dir = make_report_folder(os.path.join(self.folder(), folder_name(name)))
        return self._service.execute(
            plan, scenario=scenario, report_dir=report_dir, timeout=self._timeout, generated=generated
        )


class RunService:
    """Compile, execute, report, and replay scenarios through one catalog.

    The catalog is ``catalog``, or one built on first use
    (:func:`~valkey_fuzzer.catalog.build_catalog`) from ``experiment_directories``
    and ``deployment_providers``. Report folders go under ``report_root``: the
    absolute ``artifact_dir``, or ``<tmp>/valkey-fuzzer`` without one (see the
    module docstring), and each run's components write their artifacts into
    its report folder.
    """

    def __init__(
        self,
        *,
        experiment_directories: Sequence[ExperimentDirectory] = (),
        deployment_providers: Optional[Sequence[DeploymentProviderRegistration]] = None,
        catalog: Optional[Catalog] = None,
        artifact_dir: str = "",
    ):
        self._catalog_options = {
            "experiment_directories": tuple(experiment_directories),
            "deployment_providers": deployment_providers,
        }
        self._artifact_dir = normalize_artifact_root(artifact_dir)
        self._catalog = catalog
        self._compiler: Optional[ScenarioCompiler] = None
        self._runner: Optional[Runner] = None

    @property
    def catalog(self) -> Catalog:
        if self._catalog is None:
            self._catalog = build_catalog(**self._catalog_options)
        return self._catalog

    @property
    def runner(self) -> Runner:
        """Runs a plan; unlike :meth:`run_plan`, a preflight error raises."""

        if self._runner is None:
            self._runner = Runner(self.catalog, artifact_dir=self._artifact_dir)
        return self._runner

    def compile(self, scenario: Mapping[str, Any]) -> ExecutionPlan:
        if self._compiler is None:
            self._compiler = ScenarioCompiler(self.catalog)
        return self._compiler.compile(scenario)

    def validate_plan(self, plan: ExecutionPlan) -> None:
        """Check a compiled plan as a run would, without acquiring or provisioning anything."""

        self.runner.validate(plan)

    @property
    def report_root(self) -> str:
        return self._artifact_dir

    # -- compiling -----------------------------------------------------------

    def prepare_run(self, path: str, *, valkey_binary: Optional[str] = None) -> PreparedRun:
        """Read and compile one scenario file; the recorded scenario includes a ``valkey_binary`` override."""

        scenario = with_valkey_binary(
            self.catalog, plain_config(load_yaml_mapping(read_scenario(path), source_name=str(path))), valkey_binary
        )
        return PreparedRun(scenario=scenario, plan=self.compile(scenario))

    def generate(
        self,
        seed: int,
        *,
        experiment: Optional[str] = None,
        excludes: Sequence[Exclusion] = (),
        base: Optional[Mapping[str, Any]] = None,
        profile: Optional[str] = None,
        valkey_binary: Optional[str] = None,
        keep_own_binary: bool = False,
    ) -> Generated:
        """A valid scenario drawn from ``seed`` for ``experiment`` (one of the opted-in ones when None).

        ``profile`` names the installed search profile that samples the experiment's legal space;
        ``valkey_binary`` is the binary the drawn deployment runs, which hooks see (with
        ``keep_own_binary``, only when ``base`` names none).
        """

        catalog = self.catalog
        return generate(
            catalog,
            seed,
            compile_fn=self._checked_plan,
            experiment=experiment,
            excludes=excludes,
            base=base,
            profile=None if profile is None else find_profile(catalog, profile),
            valkey_binary=valkey_binary,
            keep_own_binary=keep_own_binary,
        )

    def _checked_plan(self, scenario: Mapping[str, Any]) -> ExecutionPlan:
        """The compiled plan, after the static preflight ``validate`` runs (server config, selectors, ports)."""

        plan = self.compile(scenario)
        self.validate_plan(plan)
        return plan

    def prepare_random(
        self,
        *,
        seed: Optional[int] = None,
        experiment: Optional[str] = None,
        excludes: Sequence[Exclusion] = (),
        base: Optional[Mapping[str, Any]] = None,
        valkey_binary: Optional[str] = None,
        profile: Optional[str] = None,
        free_ports: bool = True,
        keep_own_binary: bool = False,
    ) -> PreparedRun:
        """Generate and compile one scenario (``run --random`` / ``run --seed``); a random seed when None.

        ``base`` is a partial scenario that overrides the draws; a
        ``valkey_binary`` override is applied before the hooks run, so they
        see the binary; with ``keep_own_binary`` (a batch's binary for
        scenarios that name none) only when ``base`` names none. ``free_ports`` moves a scenario whose planned ports
        are busy to free ones (the deployment provider's ``relocate_ports``
        hook); the batch runner turns it off, because its workers own their
        port blocks.
        """

        drawn = self.generate(
            _new_seed() if seed is None else seed,
            experiment=experiment,
            excludes=excludes,
            base=base,
            profile=profile,
            valkey_binary=valkey_binary,
            keep_own_binary=keep_own_binary,
        )
        relocate = deployment_registration(drawn.plan, self.catalog).relocate_ports
        moved, note = relocate(drawn.plan) if free_ports and relocate is not None else ({}, None)
        scenario = merge(drawn.scenario, {"deployment": moved}) if moved else drawn.scenario
        plan = self.compile(scenario) if moved else drawn.plan
        return PreparedRun(scenario=scenario, plan=plan, notes=(note,) if note else (), generated=drawn.provenance)

    @staticmethod
    def read_base(path: str) -> dict[str, Any]:
        """A partial scenario file (``run --base``)."""

        document = load_document(path)
        if not isinstance(document, Mapping):
            raise InputError(f"{path} must hold a partial scenario mapping")
        return plain_config(document)

    def validate(self, path: str, *, valkey_binary: Optional[str] = None) -> ExecutionPlan:
        """Compile a scenario and check it without provisioning."""

        document = load_yaml_mapping(read_scenario(path), source_name=str(path))
        plan = self.compile(with_valkey_binary(self.catalog, document, valkey_binary))
        self.validate_plan(plan)
        return plan

    # -- executing and reporting ---------------------------------------------

    def run_plan(
        self, plan: ExecutionPlan, *, timeout: Optional[float] = None, run_dir: Optional[str] = None
    ) -> RunOutcome:
        """Execute ``plan`` within ``timeout`` seconds, its components writing into ``run_dir``.

        An error the runner raises becomes an ERROR outcome, so differentials
        and minimization continue: phase ``preflight`` when the plan fails the
        static checks (for example a deployment selector a reduced scenario no
        longer has), ``runner`` when the runner itself fails after them.
        """

        deadline = time.monotonic() + timeout if timeout is not None else None
        try:
            return self.runner.run(plan, deadline=deadline, run_dir=run_dir)
        except PreflightError as exc:
            return RunOutcome(
                plan=plan,
                status=OutcomeStatus.ERROR,
                summary=f"run failed before execution: {exc}",
                diagnostics={"error": error_record(exc.__cause__, "preflight")},
            )
        except Exception as exc:
            return RunOutcome(
                plan=plan,
                status=OutcomeStatus.ERROR,
                summary=f"the runner failed: {exc}",
                diagnostics={"error": error_record(exc, "runner")},
            )

    def execute(
        self,
        plan: ExecutionPlan,
        *,
        scenario: Optional[Mapping[str, Any]] = None,
        timeout: Optional[float] = None,
        report_dir: Optional[str] = None,
        generated: Optional[Mapping[str, Any]] = None,
    ) -> RunRecord:
        """Run one plan and leave ``plan.json``, ``report.json``, and its artifacts in its report folder.

        ``scenario`` is recorded as run, so the report replays and minimizes on
        its own, and is also written as ``scenario.yaml``, which ``run`` runs
        again (without the generator, for a generated scenario: ``generated``
        says how it was drawn). Without ``report_dir``, a new folder is made under ``runs``.
        """

        if report_dir is None:
            report_dir = new_report_folder(os.path.join(self.report_root, "runs"), plan.scenario_id)
        write_plan(report_dir, plan)
        if scenario is not None:
            export_scenario(scenario, os.path.join(report_dir, SCENARIO_FILE), generated=generated)
        outcome = self.run_plan(plan, timeout=timeout, run_dir=report_dir)
        return self.write_run_report(report_dir, outcome, scenario=scenario, generated=generated)

    def write_run_report(
        self,
        report_dir: str,
        outcome: RunOutcome,
        *,
        scenario: Optional[Mapping[str, Any]] = None,
        provenance: Optional[Mapping[str, Any]] = None,
        generated: Optional[Mapping[str, Any]] = None,
    ) -> RunRecord:
        """Write ``report.json`` for ``outcome``; every command's run reports are written here."""

        report = self.run_report(outcome, scenario=scenario, provenance=provenance, generated=generated)
        write_document(os.path.join(report_dir, REPORT_FILE), report)
        return RunRecord(outcome=outcome, report=report, report_dir=report_dir)

    def run_report(
        self,
        outcome: RunOutcome,
        *,
        scenario: Optional[Mapping[str, Any]] = None,
        provenance: Optional[Mapping[str, Any]] = None,
        generated: Optional[Mapping[str, Any]] = None,
    ) -> dict[str, Any]:
        """:func:`~valkey_fuzzer.reports.run_report` of ``outcome`` through this service's catalog."""

        return run_report(self.catalog, outcome, scenario=scenario, provenance=provenance, generated=generated)

    # -- replay --------------------------------------------------------------

    def prepare_replay(
        self, path: str, *, valkey_binary: Optional[str] = None, allow_drift: bool = False
    ) -> ReplayPlan:
        """Load a run report (or a bare plan) and check it against this environment.

        Raises :class:`ReplayDriftError` when the fuzzer, experiment,
        components (their import path or code),
        deployment provider, or Valkey binary differ from the recorded run, or
        when the report's scenario does not compile to the plan it records,
        unless ``allow_drift``. A ``valkey_binary`` override
        is intentional, so it is recorded rather than treated as drift, and
        the replay's scenario uses it, as ``run --valkey-binary`` records it.
        """

        plan, scenario, recorded = recorded_run(path)
        provenance: dict[str, Any] = {
            "source": path,
            "verified": recorded is not None,
            "drift": [],
            "drift_allowed": allow_drift,
            "not_verified": [],
            "valkey_binary_override": valkey_binary or None,
        }
        if recorded is None:
            provenance["not_verified"].append("everything: the file holds no reproducer")
        else:
            provenance["drift"], provenance["not_verified"] = self._report_drift(
                plan, scenario, recorded, valkey_binary, allow_drift=allow_drift
            )
        if valkey_binary:
            data = to_plain_data(plan)
            data["deployment"] = merge(
                data["deployment"], binary_fields(deployment_registration(plan, self.catalog), valkey_binary)
            )
            plan = ExecutionPlan.from_data(data)
            # The replay report then minimizes and replays with the binary it ran.
            if scenario is not None:
                scenario = with_valkey_binary(self.catalog, scenario, valkey_binary)
        return ReplayPlan(plan=plan, scenario=scenario, provenance=provenance, recorded=recorded)

    def _report_drift(
        self,
        plan: ExecutionPlan,
        scenario: Optional[Mapping[str, Any]],
        recorded: Mapping[str, Any],
        valkey_binary: Optional[str],
        *,
        allow_drift: bool,
    ) -> tuple[list[dict[str, Any]], list[str]]:
        """:func:`~valkey_fuzzer.provenance.report_drift`; raises :class:`ReplayDriftError` on drift, unless
        ``allow_drift``."""

        drift, unchecked = report_drift(self.catalog, self.compile, plan, scenario, recorded, valkey_binary)
        if drift and not allow_drift:
            raise ReplayDriftError("this environment differs from the recorded run", drift)
        return drift, unchecked

    def replay(self, prepared: ReplayPlan, *, timeout: Optional[float] = None) -> ReplayResult:
        """Run a prepared replay and record its provenance, including drift found only by running it."""

        plan = prepared.plan
        override = prepared.provenance["valkey_binary_override"]
        provenance = copy.deepcopy(prepared.provenance)
        report_dir = new_report_folder(os.path.join(self.report_root, "replays"), plan.scenario_id)
        write_plan(report_dir, plan)
        outcome = self.run_plan(plan, timeout=timeout, run_dir=report_dir)
        late: list[dict[str, Any]] = []
        if prepared.recorded is not None:
            after = replayed_drift(self.catalog, plan, prepared.recorded, override, outcome)
            late = [entry for entry in after if entry not in provenance["drift"]]
            provenance["drift"].extend(late)
        record = self.write_run_report(report_dir, outcome, scenario=prepared.scenario, provenance=provenance)
        return ReplayResult(record=record, late_drift=late)

    # -- differentials and minimization --------------------------------------

    def differential(
        self,
        path: str,
        *,
        seed: Optional[int] = None,
        valkey_binary: Optional[str] = None,
        timeout: Optional[float] = None,
        on_variant: Optional[Callable[[str, RunOutcome], None]] = None,
    ) -> SessionResult:
        """Run a differential document's variants and compare their evidence."""

        differential = Differential.from_document(load_document(path))
        if seed is not None:
            differential.base["seed"] = seed
        # Variants patch the base, so a variant that sets its own binary keeps it.
        differential.base = with_valkey_binary(self.catalog, differential.base, valkey_binary)
        # Every variant compiles before the first runs; a bad one leaves no folder.
        session = _Session(self, "differential", differential.differential_id, timeout)

        def execute_variant(name: str, scenario: dict, plan: ExecutionPlan) -> tuple[RunOutcome, str]:
            record = session.run(name, plan, scenario)
            return record.outcome, record.report_dir

        report = run_differential(
            differential,
            compile_fn=self.compile,
            execute_fn=execute_variant,
            components=self.catalog.components,
            on_variant=on_variant,
        )
        write_document(os.path.join(session.folder(), "differential.json"), report)
        return SessionResult(report=report, report_dir=session.folder())

    def prepare_minimization(
        self, path: str, *, valkey_binary: Optional[str] = None, allow_drift: bool = False
    ) -> Minimization:
        """Load a failing run's report: its scenario and the failure signature a reduction must keep.

        Raises :class:`ReplayDriftError` when this environment differs from
        the recorded run, as replay does, unless ``allow_drift``: a reduction
        is kept when it fails the same way, which on other code proves nothing.
        """

        report = as_run_report(load_document(path), path)
        scenario = report.get("scenario") if report is not None else None
        if not isinstance(scenario, Mapping) or "experiment" not in scenario:
            raise InputError("report does not contain the original scenario; minimize needs a 'run' report")
        reference_outcome = RunOutcome.from_data(report["outcome"])
        plan = reference_outcome.plan
        self._report_drift(plan, scenario, report, valkey_binary, allow_drift=allow_drift)
        required = [requirement.evidence_id for requirement in plan.required_evidence]
        return Minimization(
            scenario=with_valkey_binary(self.catalog, scenario, valkey_binary),
            plan=plan,
            reference=failure_signature(reference_outcome, required),
            required_evidence=required,
        )

    def minimize(
        self,
        prepared: Minimization,
        *,
        timeout: Optional[float] = None,
        max_attempts: int = 30,
        log: Optional[Callable[[str], None]] = None,
    ) -> MinimizeResult:
        """Greedily shrink the scenario while the run keeps failing the same way."""

        plan = prepared.plan
        spec = self.catalog.experiments.resolve(plan.experiment_id)
        provider = self.catalog.deployment_providers.of_plan(plan)
        session = _Session(self, "minimize", plan.scenario_id, timeout)
        attempts = itertools.count(1)
        result = minimize_scenario(
            prepared.scenario,
            prepared.reference,
            compile_fn=self.compile,
            execute_fn=lambda scenario, plan: session.run(f"attempt-{next(attempts)}", plan, scenario).outcome,
            experiment_inputs=to_plain_data(spec.inputs),
            required_evidence=prepared.required_evidence,
            deployment_requirements=to_plain_data(spec.deployment_requirements),
            deployment_reductions=None if provider is None else provider.reductions,
            max_attempts=max_attempts,
            log=log,
        )
        return MinimizeResult(
            scenario=plain_config(result["scenario"]),
            accepted=list(result["accepted"]),
            attempts=list(result["attempts"]),
            signature=dict(result["signature"]),
            report_dir=session.path,
        )
