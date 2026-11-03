"""The deployment test helpers start real servers only when a valkey-server is there to start."""

import pytest

from tests import deployment_support
from tests.deployment_support import PROCESSES, formed_cluster


def test_formed_cluster_skips_without_a_valkey_server_instead_of_building_one(monkeypatch):
    monkeypatch.setattr(deployment_support, "BINARY", None)
    # The fallback clones and builds Valkey into a directory every user of the host shares.
    monkeypatch.setattr(f"{PROCESSES}.build_valkey", lambda *_: pytest.fail("formed_cluster built Valkey"))

    with pytest.raises(pytest.skip.Exception, match="valkey-server is not available"), formed_cluster(3, 1, 7700):
        pytest.fail("formed a cluster without a valkey-server")
