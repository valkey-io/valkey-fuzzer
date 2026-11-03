"""State checks for a local deployment, by concern.

:mod:`.check` is the entry point the deployment service hands to components:
each validation attempt reads the deployment once (:mod:`.snapshot`) and runs
the checks in :mod:`.replication`, :mod:`.cluster_state` (cluster state and
slot coverage), :mod:`.views` (expected primaries and replicas, and cluster
nodes' views), :mod:`.data`, and :mod:`.logs` on that snapshot.
:mod:`.config` holds what each check is configured with (and its YAML form,
``STATE_VALIDATION_SCHEMA``), and :mod:`.results` what each found.
"""
