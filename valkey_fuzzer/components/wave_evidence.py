"""What both chaos actions declare and publish: their events, the schedule, and the loss windows."""

from collections.abc import Mapping, Sequence
from typing import Any, Optional

from valkey_fuzzer.declarations import Declarations
from valkey_fuzzer.results import make_evidence
from valkey_fuzzer.schema import ConfigField, ConfigSchema

from .ledger import LOSS_WINDOW
from .schemas import ANY, FLAG, INTEGER, LIST, MAPPING, NUMBER, TEXT, declared, mapping_of, nullable, optional
from .wave_operations import OPERATIONS

# One kill: the ``<id>.instance-killed`` payload, and an item of the schedule's ``kills`` and ``all_kills``.
KILL_FIELDS = {
    "instance_id": TEXT,
    "role": nullable(str),
    "shard": nullable(int),
    "signal": TEXT,
    "operation": TEXT,
    "wave": INTEGER,
    "at": NUMBER,
    "success": FLAG,
    # Set once the kill landed: None when the process did not exit in time.
    "exited_at": ConfigField(float, required=False, allow_none=True),
    "unexpected_exit": optional(bool),
}
# One operation's record: these fields always, and what happened to it (see docs/components.md).
_OPERATION = mapping_of(
    {
        "index": TEXT,
        "wave": INTEGER,
        "op": ConfigField(str, choices=tuple(OPERATIONS)),
        "shard": nullable(int),
        "config": MAPPING,
        "target": ANY,
        "success": FLAG,
    },
    additional=ANY,
)


def wave_declares(
    wave_fields: Mapping[str, ConfigField],
    schedule_fields: Mapping[str, ConfigField],
    *,
    events: Optional[Mapping[str, ConfigSchema]] = None,
    loss_windows: bool = False,
    changes: tuple[str, ...] = (),
) -> Declarations:
    """A chaos action's declarations: its events, the evidence of :func:`wave_evidence`, and its changes.

    ``wave_fields`` and ``schedule_fields`` are the mode's own fields of each
    wave summary (``wave_summary``) and of the schedule; with
    ``loss_windows`` it emits ``valkey.data.loss-window`` too.
    """

    wave = mapping_of(
        {
            "wave": INTEGER,
            "operations": LIST,
            "ran": LIST,
            "heal_problems": LIST,
            "validation": nullable(dict),
            **{name: LIST for name in ("planned", "applied", "healed", "still_active", "skipped")},
            **wave_fields,
        }
    )
    kills = ConfigField(list, items=mapping_of(KILL_FIELDS))
    schedule = mapping_of(
        {
            "operations": ConfigField(list, items=_OPERATION),
            "kills": kills,
            "all_kills": kills,
            "waves": ConfigField(list, items=wave),
            "halted_after_wave": nullable(int),
            "not_run": INTEGER,
            "still_active": LIST,
            "cleanup_problems": LIST,
            "loss_windows": LIST,
            "test_keys": INTEGER,
            **schedule_fields,
        }
    )
    evidence = {
        "valkey.chaos.schedule": declared(schedule),
        "valkey.instances.expected-down": declared(mapping_of({"instances": ConfigField(list, items=TEXT)})),
        "valkey.chaos.test-keys": declared(mapping_of({"keys": mapping_of(additional=TEXT)})),
    }
    if loss_windows:
        window = mapping_of({"start": NUMBER, "end": NUMBER, "instance_id": TEXT, "reason": TEXT})
        evidence[LOSS_WINDOW] = declared(window, many=True)
    return Declarations(
        events={
            "{id}.instance-killed": ConfigSchema(KILL_FIELDS),
            "{id}.wave-validated": ConfigSchema({"wave": INTEGER, "success": FLAG}),
            **(events or {}),
        },
        evidence=evidence,
        changes=changes,
    )


def wave_evidence(
    spec: Any,
    schedule: Mapping[str, Any],
    expected_down: Sequence[str],
    test_keys: Mapping[str, Any],
    loss_windows: Sequence[Mapping[str, Any]] = (),
) -> tuple:
    """The evidence both actions publish: the schedule, the instances down on purpose, the test keys, and
    one ``valkey.data.loss-window`` per window (``valkey.oracle.ledger`` reads those)."""

    return (
        make_evidence(spec, "valkey.chaos.schedule", "action.completed", schedule),
        make_evidence(spec, "valkey.instances.expected-down", "action.completed", {"instances": list(expected_down)}),
        make_evidence(spec, "valkey.chaos.test-keys", "action.completed", {"keys": test_keys}),
        *(
            make_evidence(
                spec,
                LOSS_WINDOW,
                "action.completed",
                {key: window[key] for key in ("start", "end", "instance_id", "reason")},
            )
            for window in loss_windows
        ),
    )
