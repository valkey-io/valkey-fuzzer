"""Builders and fakes that the framework tests share."""

from valkey_fuzzer.plan import DeploymentProviderIdentity

FAKE_DEPLOYMENT_PROVIDER = "tests.deployment.fake"


def deployment_identity() -> DeploymentProviderIdentity:
    """The identity of the provider :func:`deployment_registrations` registers."""

    return DeploymentProviderIdentity(FAKE_DEPLOYMENT_PROVIDER)
