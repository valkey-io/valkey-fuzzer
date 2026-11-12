"""``spec translate``: a TLC counterexample to ordinary fuzzer scenarios, through the mapping.

The mapping's ``scenario`` section says which experiment runs the
counterexample, how model constants become deployment fields and inputs, and
which model actions are faults, each injected by a named fault component of
the experiment. For each fault step of the trace the translator finds the
mapped evidence the fault follows (``after``): the last occurrence before the
fault, by the same process when ``target: same-process``, and sets the
experiment input ``occurrence_input`` to its occurrence number among all of
that evidence's steps (without ``occurrence_input`` the fault fires on the
first occurrence, so the trigger must be the first). Model steps between the
trigger and the fault are reported: the real fault fires when the trigger
event is seen, so those steps must happen in that gap or not at all, which is
what the nearby timing variants explore (occurrence shifts and the inputs in
``variants.inputs``).

The experiment is the bundle's own (``experiments/``, see
:func:`bundle_service`) or an installed one. Every scenario is compiled by the
fuzzer's compiler before it is written, and each mapped fault must be a fault
component of the compiled plan.
"""

import itertools
import os
import re
from collections.abc import Mapping, Sequence
from typing import Any, Callable, Optional

from ..protocols import ComponentRole
from ..schema import plain_config
from ..service import RunService
from .bundle import Bundle, Evidence, tla_text
from .tla import TlaSet, TraceState, changed_keys, parse_config, parse_value, render

Compile = Callable[[Mapping[str, Any]], Any]


class TranslateError(ValueError):
    """The counterexample cannot be expressed as a scenario of the mapped experiment."""


class NoFaultStepError(TranslateError):
    """The counterexample has none of the mapped fault actions: there is nothing to inject."""


def bundle_service(bundle: Bundle, experiment_dirs: Sequence[str] = ()) -> RunService:
    """A run service with the Valkey experiments, ``experiment_dirs``, and the bundle's own ``experiments/``."""

    directories = list(experiment_dirs)
    own = bundle.experiment_directory
    if own is not None and all(os.path.realpath(item) != os.path.realpath(own) for item in directories):
        directories.append(own)
    service = RunService(experiment_directories=directories)
    service.catalog  # noqa: B018 - loads the experiments now, so one that does not load fails here
    return service


def _changed(before: Mapping[str, Any], after: Mapping[str, Any], var: str) -> bool:
    return before.get(var) != after.get(var)


def _process(before: Mapping[str, Any], after: Mapping[str, Any], variables: Sequence[str]) -> Optional[str]:
    """The argument of the first of ``variables`` (functions) that changed in this step."""

    for var in variables:
        keys = changed_keys(before.get(var), after.get(var))
        if len(keys) == 1:
            return render(keys[0])
    return None


def evidence_steps(bundle: Bundle, trace: Sequence[TraceState], evidence: Evidence) -> list[dict[str, Any]]:
    """The trace steps that would show ``evidence`` in a real run, with the process each acts on."""

    steps = []
    for before, after in zip(trace, trace[1:]):
        if after.action != evidence.action:
            continue
        step = evidence.step
        if "changes" in step and not _changed(before.values, after.values, step["changes"]):
            continue
        if "unchanged" in step and _changed(before.values, after.values, step["unchanged"]):
            continue
        steps.append(
            {
                "state": after.index,
                "action": after.action,
                "process": _process(before.values, after.values, step.get("process", [])),
            }
        )
    return steps


def _resolve(spec: Any, constants: Mapping[str, str], where: str) -> Any:
    """A scenario value: a literal, ``{count: Const}``, or ``{constant: Const, map: {...}}``."""

    if not isinstance(spec, Mapping):
        return spec
    name = spec.get("count", spec.get("constant"))
    if name not in constants:
        raise TranslateError(f"{where} needs constant {name}, which the config does not set")
    text = constants[name]
    if "count" in spec:
        value = parse_value(text)
        if not isinstance(value, TlaSet):
            raise TranslateError(f"{where}: {name} = {text} is not a set")
        return len(value)
    choices = {tla_text(key): item for key, item in (spec.get("map") or {}).items()}
    if choices:
        if text not in choices:
            raise TranslateError(f"{where}: no mapping for {name} = {text}")
        return choices[text]
    return parse_value(text)


def translate_trace(
    bundle: Bundle,
    trace: Sequence[TraceState],
    config_text: str,
    *,
    name: str,
    violated: Optional[str],
    config: Optional[str] = None,
    variants: int = 4,
    seed: int = 1,
    compile_scenario: Optional[Compile] = None,
) -> dict[str, Any]:
    """Scenarios for one counterexample: the direct translation first, then up to ``variants`` neighbors.

    ``config`` names the TLC config the trace came from (bundle-relative when
    it is in the bundle); it is recorded, not read.
    """

    mapping = bundle.mapping
    scenario_spec = mapping.scenario
    if not scenario_spec:
        raise TranslateError(f"{mapping.path} has no scenario section")
    if len(trace) < 2:
        raise TranslateError("the counterexample has no steps")
    constants = parse_config(config_text).constants
    deployment = {
        key: _resolve(value, constants, f"scenario.deployment.{key}")
        for key, value in scenario_spec["deployment"].items()
    }
    inputs = {
        key: _resolve(value, constants, f"scenario.inputs.{key}")
        for key, value in (scenario_spec.get("inputs") or {}).items()
    }

    alignment = []
    occurrence_inputs = []
    absent = []
    for fault in scenario_spec.get("faults") or []:
        trigger = mapping.event(fault["after"])
        triggers = evidence_steps(bundle, trace, trigger)
        fault_steps = [
            {"state": after.index, "process": _process(before.values, after.values, fault.get("process", []))}
            for before, after in zip(trace, trace[1:])
            if after.action == fault["action"]
        ]
        if len(fault_steps) > 1:
            raise TranslateError(
                f"the counterexample has {len(fault_steps)} {fault['action']} steps; the mapping expresses one"
            )
        if not fault_steps:
            absent.append(fault)
            continue
        step = fault_steps[0]
        earlier = [item for item in triggers if item["state"] < step["state"]]
        if fault.get("target") == "same-process":
            if step["process"] is None:
                raise TranslateError(f"cannot tell which process {fault['action']} at state {step['state']} acts on")
            earlier = [item for item in earlier if item["process"] == step["process"]]
        if not earlier:
            raise TranslateError(f"{fault['action']} at state {step['state']} follows no {fault['after']} step")
        chosen = earlier[-1]
        occurrence = triggers.index(chosen) + 1
        if "occurrence_input" in fault:
            inputs[fault["occurrence_input"]] = occurrence
            occurrence_inputs.append(fault["occurrence_input"])
        elif occurrence != 1:
            raise TranslateError(
                f"{fault['action']} at state {step['state']} follows {fault['after']} #{occurrence}, but the "
                "mapping sets no occurrence_input, so the fault fires on the first"
            )
        alignment.append(
            {
                "fault": fault["action"],
                "component": fault["component"],
                "fault_state": step["state"],
                "process": step["process"],
                "trigger": fault["after"],
                "trigger_event": trigger.event,
                "trigger_state": chosen["state"],
                "occurrence": occurrence,
                "occurrence_input": fault.get("occurrence_input"),
                # The real fault fires when the trigger is seen: these steps must fit in that gap.
                "between": [trace[index - 1].action for index in range(chosen["state"] + 1, step["state"])],
            }
        )
    if not alignment and scenario_spec.get("faults"):
        raise NoFaultStepError("the counterexample has none of the mapped fault actions")
    if absent:
        # The scenario cannot leave a fault component out, and one left as it is fires with its defaults.
        raise TranslateError(
            "; ".join(
                f"the counterexample has no {fault['action']} step, but the experiment's fault component "
                f"{fault['component']} would still fire, so the scenario would not follow the counterexample"
                for fault in absent
            )
        )

    def scenario(index: int, values: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "scenario_id": _scenario_id(bundle.protocol, name, index),
            "seed": seed + index,
            "experiment": {"id": scenario_spec["experiment"], "inputs": dict(values)},
            "deployment": dict(deployment),
        }

    errors: list[str] = []
    seen: list[Any] = []

    def accept(candidate: dict[str, Any]) -> bool:
        """Compile ``candidate``; keep it unless it runs the same inputs as one already kept."""

        resolved: Any = candidate["experiment"]["inputs"]
        if compile_scenario is not None:
            try:
                plan = compile_scenario(candidate)
            except Exception as exc:
                errors.append(f"{candidate['scenario_id']}: {exc}")
                return False
            faults = {spec.id for spec in plan.components if spec.role is ComponentRole.FAULT}
            missing = sorted({item["component"] for item in alignment} - faults)
            if missing:
                errors.append(
                    f"{candidate['scenario_id']}: {plan.experiment_id} has no fault component {', '.join(missing)}"
                )
                return False
            resolved = plain_config(plan.inputs)
        if resolved in seen:
            return False
        seen.append(resolved)
        return True

    scenarios = [scenario(0, inputs)]
    accept(scenarios[0])
    variant_spec = scenario_spec.get("variants") or {}
    shifts = [0, *variant_spec.get("occurrence_shift", [])]
    timing = variant_spec.get("inputs") or {}
    combos = [dict(zip(timing, values)) for values in itertools.product(*timing.values())] or [{}]
    for shift, combo in itertools.product(shifts, combos):
        if len(scenarios) > variants:
            break
        changed = {**inputs, **combo}
        for key in occurrence_inputs:
            changed[key] = inputs[key] + shift
        if any(changed[key] < 1 for key in occurrence_inputs):
            continue
        candidate = scenario(len(scenarios), changed)
        if accept(candidate):
            scenarios.append(candidate)
    if errors:
        raise TranslateError("translated scenarios do not compile: " + "; ".join(errors))
    return {
        "kind": "spec-translation",
        "bundle": bundle.protocol,
        "mapping_sha256": mapping.sha256,
        "counterexample": {
            "name": name,
            "config": config,
            "violated": violated,
            "length": len(trace),
            "actions": [state.action for state in trace],
            "constants": constants,
        },
        "alignment": alignment,
        "scenarios": scenarios,
    }


def _scenario_id(protocol: str, name: str, index: int) -> str:
    base = re.sub(r"[^A-Za-z0-9_.:-]", "-", f"spec-{protocol}-{name}")
    return base if index == 0 else f"{base}-v{index}"


__all__ = ["NoFaultStepError", "TranslateError", "bundle_service", "evidence_steps", "translate_trace"]
