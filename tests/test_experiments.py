"""The scenario compiler and the experiment catalog: bad documents and scenarios fail with a structured error,
good ones compile into a complete plan, and catalog loads register everything or nothing."""

import json
import math
from dataclasses import replace
from typing import Any

import pytest

from tests.support import component_registration
from valkey_fuzzer.catalog import (
    Catalog,
    DuplicateExperimentError,
    load_experiment_yaml,
    parse_experiment_document,
)
from valkey_fuzzer.compiler import ScenarioCompiler, UnresolvedReferenceError, check_plan
from valkey_fuzzer.declarations import Declarations, EvidenceDeclaration
from valkey_fuzzer.plan import ComponentSpec, EvidenceRequirement, ExecutionPlan, ExperimentSpec, RunMode
from valkey_fuzzer.protocols import ComponentRole
from valkey_fuzzer.registrations import DeploymentProviderRegistration
from valkey_fuzzer.schema import (
    ConfigField,
    ConfigSchema,
    ExperimentDocumentError,
    ScenarioCompileError,
    load_yaml_mapping,
)

ACTION = "example.action.fake"


def _document(**fields: Any) -> dict[str, Any]:
    return {
        "id": "example.feature",
        "mode": "smoke",
        "deployment": {"modes": ["standalone"]},
        "components": [{"use": ACTION, "id": "activate", "config": {}}],
        "required_events": ["activate.completed"],
        **fields,
    }


def _scenario(**deployment: Any) -> dict[str, Any]:
    return {
        "scenario_id": "example-scenario",
        "seed": 123,
        "experiment": {"id": "example.feature", "inputs": {}},
        "deployment": {
            "mode": "standalone",
            "base_port": 7400,
            "opaque_future_field": {"values": [1, 2]},
            **deployment,
        },
    }


def _catalog(*experiments, registrations=(), providers=("tests.deployment.fake",), normalizer=None) -> Catalog:
    catalog = Catalog()
    catalog.experiments.register_many(experiments)
    catalog.components.register_many(registrations)
    catalog.deployment_providers.register_many(
        DeploymentProviderRegistration("standalone", provider_id, object, normalizer=normalizer)
        for provider_id in providers
    )
    return catalog


def _inputs(**properties: Any) -> dict[str, Any]:
    return {"type": "object", "properties": properties}


def _requiring_replicas(document: dict[str, Any]) -> None:
    document["deployment"]["replicas_per_primary"] = {"minimum": 1}


def _raise(error: Exception):
    def call(*_: Any) -> Any:
        raise error

    return call


_REPEAT = ConfigSchema({"repeat": ConfigField(int, default=3, minimum=1), "value": ConfigField(str, default="x")})


def _bad(document=None, scenario=None, *, registrations=None, experiment=None, **catalog):
    return {
        "document": document,
        "scenario": scenario,
        "registrations": registrations,
        "experiment": experiment,
        **catalog,
    }


# A Python-authored experiment that states a role must agree with the registration.
_ROLE_MISMATCH = ExperimentSpec(
    "example.feature",
    ("standalone",),
    (ComponentSpec("example.observer.fake", "activate", role=ComponentRole.ACTION),),
    RunMode.SMOKE,
)


@pytest.mark.parametrize(
    ("case", "error", "match"),
    [
        pytest.param(_bad(scenario=lambda s: s.update(unexpected=True)), ScenarioCompileError, "unknown fields", id="unknown-field"),
        pytest.param(_bad(scenario=lambda s: s.update(seed=True)), ScenarioCompileError, "seed must be an integer", id="bool-seed"),
        pytest.param(_bad(scenario=lambda s: s["deployment"].pop("mode")), ScenarioCompileError, "missing required field 'mode'", id="no-mode"),
        pytest.param(_bad(scenario=lambda s: s["deployment"].update(mode="cluster")), ScenarioCompileError, "not supported", id="unsupported-mode"),
        pytest.param(_bad(registrations=()), ScenarioCompileError, r"uses 'example\.action\.fake', which is not registered", id="unregistered-component"),
        pytest.param(_bad(experiment=_ROLE_MISMATCH, registrations=(component_registration("example.observer.fake"),)), ScenarioCompileError, "registered as 'observer'", id="role-mismatch"),
        pytest.param(_bad(providers=("tests.deployment.fake", "tests.deployment.other")), ScenarioCompileError, r"multiple registered providers \['tests\.deployment\.fake', 'tests\.deployment\.other'\].*deployment\.provider", id="several-providers-none-named"),
        pytest.param(_bad(scenario=lambda s: s["deployment"].update(provider="tests.deployment.missing")), ScenarioCompileError, r"'tests\.deployment\.missing' is not registered for deployment mode 'standalone'", id="unknown-provider"),
        pytest.param(_bad(lambda d: d["components"][0].update(config={"value": {"$ref": "inputs.missing"}})), UnresolvedReferenceError, "missing input path", id="missing-input"),
        pytest.param(_bad(lambda d: d["components"][0].update(config={"value": {"$ref": "inputs.value", "fallback": 1}})), UnresolvedReferenceError, "sibling fields", id="reference-with-siblings"),
        pytest.param(_bad(lambda d: d["components"][0].update(config={"value": {"$ref": "environment.secret"}})), UnresolvedReferenceError, "must name inputs, artifacts, or resources", id="reference-outside-inputs"),
        # Inputs bind only environment outputs: anything else is a typo, or data for the literal marker.
        pytest.param(_bad(scenario=lambda s: s["experiment"].update(inputs={"binary": {"$ref": "artifact.valkey.path"}})), UnresolvedReferenceError, r"experiment\.inputs\.binary.*may reference only artifacts or resources", id="input-reference-typo"),
        pytest.param(_bad(scenario=lambda s: s["experiment"].update(inputs={"x": [{"$ref": "inputs.y"}]})), UnresolvedReferenceError, r"experiment\.inputs\.x\[0\]", id="input-references-an-input"),
            # JSON types: True is not the integer 1.
        pytest.param(_bad(lambda d: d.update(inputs=_inputs(x={"enum": [1]})), lambda s: s["experiment"].update(inputs={"x": True})), ScenarioCompileError, r"experiment\.inputs\.x must be one of \[1\], got True", id="input-enum-type"),
        pytest.param(_bad(lambda d: d.update(mode="experiment", required_evidence=[{"evidence_id": "example.path", "source": "activate"}])), ScenarioCompileError, "requires at least one required oracle", id="experiment-without-oracle"),
        pytest.param(_bad(registrations=(component_registration(ACTION, check=_raise(ValueError("invalid component config"))),)), ScenarioCompileError, "rejected 'activate': invalid component config", id="component-check"),
        pytest.param(_bad(lambda d: d["components"][0].update(config={"repeat": 0}), registrations=(component_registration(ACTION, config=_REPEAT),)), ScenarioCompileError, r"components\.activate\.config\.repeat must be at least 1", id="component-config"),
        pytest.param(_bad(_requiring_replicas, normalizer=lambda document: {"replicas_per_primary": 0, **document}), ScenarioCompileError, r"does not support this deployment: deployment\.replicas_per_primary must be at least 1, got 0", id="deployment-requirement"),
        pytest.param(_bad(_requiring_replicas), ScenarioCompileError, "missing required field 'replicas_per_primary'", id="requirement-without-field"),
        pytest.param(_bad(normalizer=_raise(ValueError("primaries is only valid in cluster mode"))), ScenarioCompileError, "deployment: primaries", id="normalizer-raises"),
        pytest.param(_bad(normalizer=lambda document: {"mode": "cluster"}), ScenarioCompileError, "normalized the deployment into an invalid document", id="normalizer-changes-mode"),
    ],
)  # fmt: skip
def test_a_bad_scenario_fails_to_compile_with_a_structured_error(case, error, match):
    document = _document()
    scenario = _scenario()
    (case["document"] or (lambda _: None))(document)
    (case["scenario"] or (lambda _: None))(scenario)
    registrations = case["registrations"]
    catalog = _catalog(
        case["experiment"] or parse_experiment_document(document),
        registrations=(component_registration(ACTION),) if registrations is None else registrations,
        **{key: case[key] for key in ("providers", "normalizer") if key in case},
    )

    with pytest.raises(error, match=match):
        ScenarioCompiler(catalog).compile(scenario)


@pytest.mark.parametrize(
    ("component", "path"),
    [
        ({"use": ACTION, "id": " bad"}, "components[1]"),
        ({"use": ACTION, "id": "b", "depends_on": "a"}, "components[1]"),
    ],
)
def test_a_bad_field_of_a_listed_model_names_the_item_not_the_fields_label(component, path):
    document = _document(components=[{"use": ACTION, "id": "a"}, component])

    with pytest.raises(ExperimentDocumentError) as raised:
        parse_experiment_document(document)
    assert raised.value.path == path


def test_a_reference_outside_inputs_names_the_ref_field_as_its_path():
    document = _document()
    document["components"][0].update(config={"value": {"$ref": "environment.secret"}})
    catalog = _catalog(parse_experiment_document(document), registrations=(component_registration(ACTION),))

    with pytest.raises(UnresolvedReferenceError, match="must name inputs, artifacts, or resources") as raised:
        ScenarioCompiler(catalog).compile(_scenario())
    assert raised.value.path == "components.activate.config.value.$ref"


@pytest.mark.parametrize(
    ("load", "match"),
    [
        (lambda: load_experiment_yaml("id: a.b\nid: a.c\nmode: smoke\ndeployment: {modes: [standalone]}\ncomponents: []"), "duplicate key"),
        (lambda: load_yaml_mapping("!!python/object/apply:os.system ['echo unsafe']"), "could not determine a constructor"),
        (lambda: parse_experiment_document(_document(unexpected=1)), r"unknown fields: \['unexpected'\]"),
        (lambda: parse_experiment_document(_document(startup_overlays=set())), "startup_overlays must be an ordered sequence"),
        (lambda: parse_experiment_document(_document(components=[{"use": ACTION}])), "components entries must be mappings with 'id'"),
        (lambda: parse_experiment_document(_document(deployment={"modes": ["standalone"], "type": "x"})), r"deployment.type must be a rule such as \{minimum: 1\}"),
        (lambda: parse_experiment_document(_document(deployment={"modes": ["standalone"], "replicas_per_primary": {"minimum": 2, "maximum": 1}})), "minimum is greater than maximum"),
        (lambda: parse_experiment_document(_document(startup_overlays=[{"overlay_id": "o", "selector": {"node_ids": ["valkey-0"]}, "server_config": {}}])), r"startup_overlays\[0\].selector"),
        (lambda: parse_experiment_document(_document(extends=["example.loop"]), fragments={"example.loop": {"extends": ["example.loop"]}}), "fragment cycle"),
        (lambda: parse_experiment_document(_document(extends=["example.missing"]), fragments={}), r"unknown fragment 'example\.missing'"),
        # Two of a fragment's components with one ID, which the experiment overrides: neither may be dropped unseen.
        (lambda: parse_experiment_document(_document(extends=["example.twice"], components=[{"use": ACTION, "id": "a"}]),
                                           fragments={"example.twice": {"components": [{"use": ACTION, "id": "a"}, {"use": ACTION, "id": "a"}]}}),
         "more than one entry with id 'a'"),
        # The inputs schema is checked with the experiment, before any scenario uses it.
        (lambda: parse_experiment_document(_document(inputs=_inputs(x={"type": "imaginary"}))), "unsupported schema type"),
        (lambda: parse_experiment_document(_document(inputs=_inputs(x={"minimum": 10, "maximum": 1}))), "minimum is greater than maximum"),
        (lambda: parse_experiment_document(_document(inputs=_inputs(x={"type": "string", "reference_kinds": ["artifacts"]}))), "reference_kinds needs a field that accepts environment references"),
        (lambda: parse_experiment_document(_document(inputs=_inputs(x={"type": "environment_reference", "reference_kinds": ["machines"]}))), "reference_kinds must be 'artifacts' or 'resources'"),
        (lambda: parse_experiment_document(_document(inputs=_inputs(x={"type": "integer", "default": "one"}))), "default must be an integer"),
        (lambda: parse_experiment_document(_document(inputs=_inputs(**{"a.b": {"type": "string"}}))), "'a.b'"),
        (lambda: parse_experiment_document(_document(required_evidence=[{"source": "activate"}])), r"required_evidence\[0\] is missing required field 'evidence_id'"),
    ],
    ids=["duplicate-key", "unsafe-yaml", "unknown-field", "unordered", "component-id", "deployment-rule", "requirement-range", "selector", "fragment-cycle", "unknown-fragment",
         "fragment-duplicate", "input-type", "input-range", "input-reference-field", "input-reference-kinds", "input-default", "input-name", "requirement-field"],
)  # fmt: skip
def test_a_bad_experiment_document_is_rejected(load, match):
    with pytest.raises(ExperimentDocumentError, match=match):
        load()


@pytest.mark.parametrize(
    ("text", "value"),
    [
        # YAML 1.2 core scalars: only true and false are booleans, so a reject rule or a conform constant's
        # equals written as yes or no matches the string, and a number never comes out of a typo.
        ("value: yes", "yes"), ("value: [no, on, Off]", ["no", "on", "Off"]), ("value: TRUE", True),
        ("value: 1:30", "1:30"), ("value: 1_000", "1_000"), ("value: 2026-10-29", "2026-10-29"),
        ("value: 5e-05", 5e-05), ("value: -1.5", -1.5), ("value: .inf", math.inf), ("value: 0o17", 15),
        ("value: 0x1F", 31), ("value: 0", 0), ("value: -12", -12), ("value: ~", None),
        # Read as octal (7936) by YAML 1.1 and as decimal by YAML 1.2: refused rather than guessed.
        ("value: 017400", "write 17400"), ("value: [-07]", "write 17400"),
    ],
)  # fmt: skip
def test_documents_read_yaml_1_2_scalars_and_refuse_a_leading_zero(text, value):
    if value == "write 17400":
        with pytest.raises(ExperimentDocumentError, match="leading zero"):
            load_yaml_mapping(text)
    else:
        assert load_yaml_mapping(text) == {"value": value}


def test_a_valid_scenario_compiles_into_a_complete_plan():
    inputs = {
        "count": {"type": "integer", "minimum": 1},
        "payload": {"type": "object"},
        "provider": {"type": "string"},
        "compression": {"type": "string", "default": "lz4"},
    }
    expect = [{"evidence_id": "example.used", "required": True}, {"evidence_id": "example.x"}]
    document = {
        "extends": ["example.base"],
        "id": "example.feature",
        "mode": "experiment",
        "deployment": {"modes": ["standalone"], "replicas_per_primary": {"minimum": 1}},
        "inputs": {"type": "object", "properties": inputs, "required": ["count", "payload"]},
        "components": [
            {"use": "example.workload.fake", "id": "seed", "config": {"value": {"$ref": "inputs.payload.zone"}}},
            {"use": "example.observer.fake", "id": "watch", "depends_on": ["seed"], "config": {"expect": expect}},
        ],
        "startup_overlays": [{"overlay_id": "p", "selector": "replicas", "server_config": {"repl-compression": {"$ref": "inputs.compression"}}}],
        "required_evidence": [{"evidence_id": "example.path", "source": "watch", "min_count": 2}],
    }  # fmt: skip
    fragment = {
        "components": [{"use": "example.workload.fake", "id": "seed", "config": {"repeat": 5}}, {"use": "example.oracle.fake", "id": "check"}],
        "required_events": ["seed.completed"],
    }  # fmt: skip
    evidence = {
        "{config.expect[].evidence_id}": EvidenceDeclaration(ConfigField(dict)),
        "example.path": EvidenceDeclaration(ConfigField(dict), many=True),
    }
    registrations = (
        component_registration("example.workload.fake", config=_REPEAT),
        component_registration(
            "example.observer.fake",
            declares=Declarations(evidence=evidence),
            required_evidence=lambda spec: [
                item["evidence_id"] for item in spec.config["expect"] if item.get("required")
            ],
        ),
        component_registration("example.oracle.fake"),
    )
    catalog = _catalog(
        parse_experiment_document(document, fragments={"example.base": fragment}),
        registrations=registrations,
        providers=("tests.deployment.fake", "tests.deployment.other"),
        normalizer=lambda deployment: {"replicas_per_primary": 1, **deployment},
    )
    # A value that looks like a reference is data, never a template.
    supplied = {
        "count": 10,
        "provider": "tests.deployment.other",
        "payload": {"zone": "a", "ref": {"$valkey_fuzzer_literal": {"$ref": "inputs.count"}}},
    }
    scenario = _scenario(provider={"$ref": "inputs.provider"})
    scenario["experiment"]["inputs"] = supplied

    plan = ScenarioCompiler(catalog).compile(scenario)
    supplied["payload"]["zone"] = "changed"
    scenario["deployment"]["opaque_future_field"]["values"].append(3)

    assert plan.mode is RunMode.EXPERIMENT
    assert plan.inputs == {
        "count": 10,
        "provider": "tests.deployment.other",
        "payload": {"zone": "a", "ref": {"$ref": "inputs.count"}},
        "compression": "lz4",
    }
    # The experiment's components come first, merged with the fragment's; the fragment's others follow.
    assert [(c.id, c.role) for c in plan.components] == [
        ("seed", ComponentRole.WORKLOAD),
        ("watch", ComponentRole.OBSERVER),
        ("check", ComponentRole.ORACLE),
    ]
    assert dict(plan.components[0].config) == {"value": "a", "repeat": 5}
    assert plan.components[1].depends_on == ("seed",)
    assert plan.startup_overlays[0].server_config == {"repl-compression": "lz4"}
    assert plan.required_events == ("seed.completed",)
    assert set(plan.required_evidence) == {
        EvidenceRequirement("example.path", source="watch", min_count=2),
        EvidenceRequirement("example.used", source="watch"),
    }
    assert plan.deployment_provider.provider_id == "tests.deployment.other"
    assert "provider" not in plan.deployment
    assert plan.deployment["replicas_per_primary"] == 1, "the normalizer's defaults are recorded"
    assert plan.deployment["opaque_future_field"]["values"] == (1, 2)
    assert ExecutionPlan.from_data(json.loads(plan.to_json())) == plan
    assert plan.rng("seed").getrandbits(64) == plan.rng("seed").getrandbits(64)


def test_evidence_a_config_default_marks_required_is_required_of_compiled_and_hand_built_plans():
    # The observer requires its evidence unless its config says otherwise, so the default must count.
    observer = component_registration(
        "example.observer.fake",
        config=ConfigSchema({"required": ConfigField(bool, default=True)}),
        declares=Declarations(evidence={"example.seen": EvidenceDeclaration(ConfigField(dict))}),
        required_evidence=lambda spec: ["example.seen"] if spec.config.get("required") else [],
    )
    components = (ComponentSpec("example.observer.fake", "watch"), ComponentSpec("example.oracle.fake", "check"))
    path = EvidenceRequirement("example.seen", source="check")
    experiment = ExperimentSpec("example.feature", ("standalone",), components, required_evidence=(path,))
    oracle = component_registration(
        "example.oracle.fake", declares=Declarations(evidence={"example.seen": EvidenceDeclaration(ConfigField(dict))})
    )
    catalog = _catalog(experiment, registrations=(observer, oracle))
    derived = EvidenceRequirement("example.seen", source="watch")

    plan = ScenarioCompiler(catalog).compile(_scenario())
    assert plan.required_evidence == (path, derived)
    # A plan built without the compiler gets it when it is checked, as the runner checks every plan.
    hand_built = replace(
        plan, components=tuple(replace(c, config={}) for c in plan.components), required_evidence=(path,)
    )
    assert check_plan(hand_built, catalog).required_evidence == (path, derived)


def test_catalog_loads_register_everything_or_nothing(tmp_path):
    (tmp_path / "a.yaml").write_text(
        "extends: [example.base]\nid: example.extended\nmode: smoke\ndeployment: {modes: [standalone]}\ncomponents: []\n"
    )
    (tmp_path / "b.yaml").write_text("id: [")
    catalog = _catalog(registrations=(component_registration(ACTION),))
    catalog.register(fragments={"example.base": {}})
    with pytest.raises(ExperimentDocumentError, match=r"b\.yaml"):
        catalog.load_experiment_directories([tmp_path])
    assert len(catalog.experiments) == 0

    (tmp_path / "b.yaml").unlink()
    (loaded,) = catalog.load_experiment_directories([tmp_path])
    assert catalog.experiments.resolve("example.extended") is loaded

    # A registration whose experiment collides registers none of its components.
    with pytest.raises(DuplicateExperimentError):
        catalog.register(
            components=(component_registration("new.action.fake"),),
            experiments=(parse_experiment_document(_document(id="example.extended")),),
        )
    assert "new.action.fake" not in catalog.components
    assert catalog.experiments.registered_ids == ("example.extended",)
