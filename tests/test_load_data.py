"""The cluster slot preload writes the same distinct keys into every slot for the same seed."""

import random
from types import SimpleNamespace
from unittest.mock import Mock, patch

from valkey.crc import key_slot

from valkey_fuzzer.deployment.local.models import DeploymentMode
from valkey_fuzzer.deployment.local.service import load_all_slots


class _Cluster:
    """A cluster client whose pipelines acknowledge every SET but those of the keys in ``refused``."""

    def __init__(self, refused=()):
        self.written, self.refused = [], set(refused)
        self.close = Mock()

    def pipeline(self):
        queued = []
        cluster = self

        class Pipeline:
            def set(self, key, value):
                queued.append((key, value))

            def execute(self, raise_on_error=True):
                cluster.written.extend(queued)
                return [ConnectionError("reset") if key in cluster.refused else True for key, _ in queued]

        return Pipeline()


def test_the_same_seed_preloads_the_same_distinct_keys_into_every_slot():
    cluster = SimpleNamespace(mode=DeploymentMode.CLUSTER, startup_nodes=[{"host": "127.0.0.1", "port": 7000}])
    clients = [_Cluster(), _Cluster(), _Cluster(refused={"key:{0}:1"})]
    with patch("valkey_fuzzer.deployment.local.service.ValkeyCluster", side_effect=clients):
        assert load_all_slots(cluster, keys_per_slot=2, rng=random.Random(1234)) is True
        assert load_all_slots(cluster, keys_per_slot=2, rng=random.Random(1234)) is True
        assert load_all_slots(cluster, keys_per_slot=2, rng=random.Random(1234)) is False
    first, second = (client.written for client in clients[:2])
    assert first == second and all(client.close.called for client in clients)
    # Two keys per slot are two keys: their suffixes never collide within a slot.
    keys = [key for key, _ in first]
    assert len(set(keys)) == len(keys) == 2 * 16384
    assert sorted(key_slot(key.encode()) for key in keys) == sorted(list(range(16384)) * 2)
    # Outside cluster mode there are no slots to fill.
    assert load_all_slots(SimpleNamespace(mode=DeploymentMode.STANDALONE), keys_per_slot=1, rng=random.Random(0))
