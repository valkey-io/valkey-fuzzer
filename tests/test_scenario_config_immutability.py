from copy import deepcopy

from src.models import StateValidationConfig


def test_deepcopy_preserves_validation_config():
    # Create original config
    original_config = StateValidationConfig()
    original_config.check_data_consistency = True
    original_config.check_replication = True
    original_config.stabilization_wait = 5.0

    # Create a deep copy
    copied_config = deepcopy(original_config)

    # Verify initial state matches
    assert copied_config.check_data_consistency is True
    assert copied_config.check_replication is True
    assert copied_config.stabilization_wait == 5.0

    # Mutate the copy
    copied_config.check_data_consistency = False
    copied_config.stabilization_wait = 10.0

    # Verify original is unchanged
    assert original_config.check_data_consistency is True, (
        "Original config should not be affected by mutations to the copy"
    )
    assert original_config.stabilization_wait == 5.0, "Original config should not be affected by mutations to the copy"

    # Verify copy has the mutations
    assert copied_config.check_data_consistency is False
    assert copied_config.stabilization_wait == 10.0


def test_validation_config_nested_objects_are_copied():
    original_config = StateValidationConfig()
    original_config.replication_config.max_acceptable_lag = 5.0
    original_config.cluster_status_config.require_quorum = True

    # Create a deep copy
    copied_config = deepcopy(original_config)

    # Mutate nested objects in the copy
    copied_config.replication_config.max_acceptable_lag = 10.0
    copied_config.cluster_status_config.require_quorum = False

    # Verify original nested objects are unchanged
    assert original_config.replication_config.max_acceptable_lag == 5.0, "Original nested config should not be affected"
    assert original_config.cluster_status_config.require_quorum is True, "Original nested config should not be affected"


def test_validation_config_lists_are_copied():
    original_config = StateValidationConfig()
    original_config.cluster_status_config.acceptable_states = ["ok", "degraded"]

    # Create a deep copy
    copied_config = deepcopy(original_config)

    # Mutate list in the copy
    copied_config.cluster_status_config.acceptable_states.append("fail")

    # Verify original list is unchanged
    assert original_config.cluster_status_config.acceptable_states == ["ok", "degraded"], (
        "Original list should not be affected by mutations to the copy"
    )
    assert copied_config.cluster_status_config.acceptable_states == ["ok", "degraded", "fail"]
