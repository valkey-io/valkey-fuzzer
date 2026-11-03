"""Helpers for the Valkey components' tests: a run service with them, a shipped experiment, and what a unit test of
one component needs: a plan, its registration, its resolved spec, and a fake run context."""

from valkey_fuzzer.compiler import with_planned_instances
from valkey_fuzzer.components import component_registrations, experiments
from valkey_fuzzer.deployment.local.provider import local_deployment_provider_registrations
from valkey_fuzzer.plan import ComponentSpec, DeploymentProviderIdentity, ExecutionPlan
from valkey_fuzzer.protocols import ComponentRole
from valkey_fuzzer.results import evidence_of
from valkey_fuzzer.schema import plain_config
from valkey_fuzzer.service import RunService
from valkey_fuzzer.testing import FakeContext, resolved_spec

LOCAL_PROVIDER = DeploymentProviderIdentity("valkey-fuzzer.deployment.local")


def valkey_service(**options) -> RunService:
    """A run service with the Valkey components, its catalog already built."""

    service = RunService(**options)
    service.catalog  # noqa: B018
    return service


def valkey_experiment(experiment_id):
    """The shipped Valkey experiment ``experiment_id``."""

    return next(item for item in experiments() if item.experiment_id == experiment_id)


def unit_plan(components=(), deployment_mode="standalone", document=None, resources=(), startup_overlays=()):
    """A plan holding ``components`` and an evidence oracle, to check component configs against.

    Its deployment has the local provider's defaults filled in, and it records the instances that provider
    plans, as a compiled plan does.
    """

    registration = local_deployment_provider_registrations()[deployment_mode]
    oracle = ComponentSpec(role=ComponentRole.ORACLE, use="valkey.oracle.evidence", id="assert")
    plan = ExecutionPlan(
        scenario_id="unit",
        experiment_id="tests.experiment",
        seed=1,
        deployment_mode=deployment_mode,
        components=(*components, oracle),
        deployment_provider=LOCAL_PROVIDER,
        mode="smoke",
        resources=resources,
        startup_overlays=startup_overlays,
        deployment=registration.normalizer(document or {"mode": deployment_mode}),
    )
    return with_planned_instances(plan, registration)


def registration_for(component_id):
    return next(item for item in component_registrations() if item.component_id == component_id)


def resolved(spec):
    """``spec`` with its config's defaults, as the compiler records it."""

    return resolved_spec(registration_for(spec.use), spec)


def fake_context(handle, *components, **kwargs):
    """A FakeContext whose ``valkey.deployment`` service is ``handle``; ``components`` may draw from its rng."""

    return FakeContext(*components, services={"valkey.deployment": handle}, **kwargs)


def evidence_values(evidence, evidence_id):
    """The plain values of the ``evidence_id`` items of ``evidence``, in order."""

    return [plain_config(item.value) for item in evidence_of(evidence, evidence_id)]


def chaos_schedule(result):
    """A chaos action's ``valkey.chaos.schedule``."""

    [schedule] = evidence_values(result.evidence, "valkey.chaos.schedule")
    return schedule


def chaos_operation(result, index):
    """Operation ``index`` (such as ``"1.0"``) of a chaos action's schedule."""

    return next(record for record in chaos_schedule(result)["operations"] if record["index"] == index)


def validation_result(*failed_checks, critical=False, inconclusive=False):
    """A deployment state validation's result: a pass, or a failure of ``failed_checks``, each with a message."""

    return {
        "overall_success": not failed_checks,
        "inconclusive": inconclusive,
        "critical": critical,
        "failed_checks": list(failed_checks),
        "error_messages": [f"{check} broke" for check in failed_checks],
        "checks": {},
    }
