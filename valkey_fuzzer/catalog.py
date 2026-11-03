"""The catalog: experiments, component, generator, and provider registries, and fragments.

:class:`Catalog` owns everything one run service registers. Data-only
experiment documents are parsed here (fragments merged in through
``extends``); :func:`build_catalog` registers the experiments of any experiment
directories.
"""

import re
from collections.abc import Iterable, Mapping, Sequence
from functools import partial
from os import PathLike
from pathlib import Path
from types import MappingProxyType
from typing import Any, Optional, TextIO, Union

from ._data import freeze_mapping
from .deployment.local.provider import local_deployment_provider_registrations
from .documents import merge
from .environment import EnvironmentProviderRegistration, EnvironmentProviderRegistry
from .fields import document_sequence, model_from_data, models_from_data, require_qualified_id, strict_fields
from .plan import ComponentSpec, EvidenceRequirement, ExecutionPlan, ExperimentSpec, InstanceSelector, StartupOverlay
from .registrations import ComponentRegistration, DeploymentProviderRegistration, GeneratorRegistration
from .registry import BaseRegistry, DuplicateRegistrationError, FrozenRegistryError, RegistrationError
from .schema import (
    ENVIRONMENT_NAMESPACES,
    ExperimentDocumentError,
    ExperimentError,
    document_mapping,
    load_yaml,
)

ExperimentDirectory = Union[str, "PathLike[str]"]


class DuplicateExperimentError(ExperimentError):
    def __init__(self, experiment_id: str):
        self.experiment_id = experiment_id
        super().__init__(f"experiment {experiment_id!r} is already registered")


class UnknownExperimentError(ExperimentError):
    def __init__(self, experiment_id: str):
        self.experiment_id = experiment_id
        super().__init__(f"experiment {experiment_id!r} is not registered")


# ---------------------------------------------------------------------------
# Experiment documents
# ---------------------------------------------------------------------------

_EXPERIMENT_FIELDS = (
    "extends",
    "id",
    "mode",
    "deployment",
    "components",
    "inputs",
    "startup_overlays",
    "required_events",
    "required_evidence",
    "metadata",
)


def _parse_deployment(value: Any, location: str) -> tuple[tuple[Any, ...], dict[str, Any]]:
    """``{modes: [...], <field>: <rule>, ...}``: the supported modes, then rules for deployment fields."""

    mapping = dict(strict_fields(value, location, allowed=None, required=("modes",), error=ExperimentDocumentError))
    modes = document_sequence(mapping.pop("modes"), f"{location}.modes", ExperimentDocumentError)
    return modes, mapping


def _merge_by_id(base: Sequence[Any], overlay: Sequence[Any], key: str, location: str) -> list[Any]:
    """Merge two lists of mappings by ``key``.

    The overlay's entries come first, in its order; an entry whose ID is also
    in ``base`` deep-merges onto it. Remaining ``base`` entries follow, so the
    experiment author's ordering wins and fragments fill in after it.
    """

    ids = [item.get(key) for item in base if isinstance(item, Mapping)]
    duplicate = next(
        (item_id for index, item_id in enumerate(ids) if item_id is not None and item_id in ids[:index]), None
    )
    if duplicate is not None:
        # An overlay entry with that ID would merge onto one of them and drop the other unseen.
        raise ExperimentDocumentError(f"{location} has more than one entry with {key} {duplicate!r}")
    by_id = {item.get(key): item for item in base if isinstance(item, Mapping)}
    merged = []
    for item in overlay:
        if not isinstance(item, Mapping) or key not in item:
            raise ExperimentDocumentError(f"{location} entries must be mappings with {key!r}")
        merged.append(merge(by_id.pop(item[key]), item) if item[key] in by_id else dict(item))
    return merged + [dict(item) for item in base if isinstance(item, Mapping) and item.get(key) in by_id]


def _union(base: Sequence[Any], overlay: Sequence[Any]) -> list[Any]:
    merged = list(base)
    for item in overlay:
        if item not in merged:
            merged.append(item)
    return merged


def merge_experiment_documents(base: Mapping[str, Any], overlay: Mapping[str, Any]) -> dict[str, Any]:
    """Merge an experiment (or fragment) ``overlay`` onto ``base``.

    ``components`` and ``startup_overlays`` merge by ID (a same-ID entry
    deep-merges, so an experiment can override one config field of a
    fragment's component, and the overlay's order comes first); ``inputs``
    and ``metadata`` deep-merge; ``required_events``, ``required_evidence``,
    and ``inputs.required`` are unions; every other field is replaced.
    """

    merged = dict(base)
    for key, value in overlay.items():
        if key == "components":
            merged[key] = _merge_by_id(
                base.get(key, ()), document_sequence(value, key, ExperimentDocumentError), "id", key
            )
        elif key == "startup_overlays":
            merged[key] = _merge_by_id(
                base.get(key, ()), document_sequence(value, key, ExperimentDocumentError), "overlay_id", key
            )
        elif key in ("required_events", "required_evidence"):
            merged[key] = _union(base.get(key, ()), document_sequence(value, key, ExperimentDocumentError))
        elif key in ("inputs", "metadata"):
            merged[key] = merge(document_mapping(base.get(key, {}), key), document_mapping(value, key))
            if key == "inputs" and "required" in base.get(key, {}) and "required" in value:
                merged[key]["required"] = _union(
                    base[key]["required"], document_sequence(value["required"], key, ExperimentDocumentError)
                )
        else:
            merged[key] = value
    return merged


def apply_fragments(
    value: Mapping[str, Any],
    fragments: Mapping[str, Mapping[str, Any]],
    location: str = "experiment",
    _seen: tuple[str, ...] = (),
) -> dict[str, Any]:
    """Resolve ``extends: [fragment, ...]`` by merging each fragment, then the document itself."""

    document = dict(document_mapping(value, location))
    names = document.pop("extends", ())
    names = (
        [names]
        if isinstance(names, str)
        else list(document_sequence(names, f"{location}.extends", ExperimentDocumentError))
    )
    merged: dict[str, Any] = {}
    for name in names:
        if name in _seen:
            raise ExperimentDocumentError(f"{location} has a fragment cycle: {' -> '.join((*_seen, name))}")
        if name not in fragments:
            raise ExperimentDocumentError(f"{location} extends unknown fragment {name!r}")
        fragment = apply_fragments(fragments[name], fragments, f"fragment {name!r}", (*_seen, name))
        unknown = sorted(set(fragment) - set(_EXPERIMENT_FIELDS))
        if unknown:
            raise ExperimentDocumentError(f"fragment {name!r} contains unknown fields: {unknown}")
        merged = merge_experiment_documents(merged, fragment)
    return merge_experiment_documents(merged, document)


def parse_experiment_document(
    value: Mapping[str, Any],
    *,
    fragments: Optional[Mapping[str, Mapping[str, Any]]] = None,
    default_extends: Sequence[str] = (),
) -> ExperimentSpec:
    """Parse one strict, data-only experiment mapping into an immutable spec.

    ``extends`` names fragments from ``fragments`` to merge first (see
    :func:`merge_experiment_documents`); ``default_extends`` are merged before
    them, which gives every shipped Valkey experiment a common base.
    """

    document = dict(document_mapping(value, "experiment"))
    if default_extends:
        own = document.get("extends", ())
        own = [own] if isinstance(own, str) else list(document_sequence(own, "extends", ExperimentDocumentError))
        document["extends"] = [*default_extends, *own]
    merged = apply_fragments(document, fragments or {})
    # Fragments' metadata.generator only fills in the block of an experiment
    # that declares one: declaring it is how an experiment opts in to random scenarios.
    own_metadata = document.get("metadata")
    if not (isinstance(own_metadata, Mapping) and "generator" in own_metadata):
        if isinstance(merged.get("metadata"), Mapping) and "generator" in merged["metadata"]:
            merged["metadata"] = {key: item for key, item in merged["metadata"].items() if key != "generator"}
    mapping = dict(
        strict_fields(
            merged,
            "experiment",
            allowed=_EXPERIMENT_FIELDS,
            required=("id", "deployment", "components"),
            error=ExperimentDocumentError,
        )
    )
    _reject_environment_references(mapping.get("metadata", {}), "metadata")
    modes, rules = _parse_deployment(mapping.pop("deployment"), "deployment")
    mapping.update(experiment_id=mapping.pop("id"), deployment_modes=modes, deployment_requirements=rules)
    many = partial(models_from_data, error=ExperimentDocumentError)
    return model_from_data(
        ExperimentSpec,
        mapping,
        "experiment",
        error=ExperimentDocumentError,
        components=partial(many, ComponentSpec, location="components"),
        startup_overlays=partial(many, StartupOverlay, location="startup_overlays", selector=InstanceSelector.parse),
        required_evidence=partial(many, EvidenceRequirement, location="required_evidence"),
    )


def _reject_environment_references(value: Any, location: str) -> None:
    """Persisted experiment data such as metadata cannot bind run-time environment outputs."""

    if isinstance(value, Mapping):
        reference = value.get("$ref")
        if isinstance(reference, str) and reference.split(".", 1)[0] in ENVIRONMENT_NAMESPACES:
            raise ExperimentDocumentError(f"{location} must not contain environment references")
        for key, item in value.items():
            _reject_environment_references(item, f"{location}.{key}")
    elif isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            _reject_environment_references(item, f"{location}[{index}]")


def load_experiment_yaml(
    source: Union[str, bytes, TextIO],
    *,
    source_name: str = "<yaml>",
    fragments: Optional[Mapping[str, Mapping[str, Any]]] = None,
    default_extends: Sequence[str] = (),
) -> ExperimentSpec:
    """Parse one YAML experiment document (``source`` is content, not a path)."""

    value = load_yaml(source, source_name)
    try:
        return parse_experiment_document(value, fragments=fragments, default_extends=default_extends)
    except ExperimentError as exc:
        raise ExperimentDocumentError(f"{source_name}: {exc}") from exc


class ExperimentRegistry(BaseRegistry[ExperimentSpec]):
    """Registered experiments, keyed by ``experiment_id``."""

    label = "experiment"
    entry_type = ExperimentSpec

    def key(self, entry: ExperimentSpec) -> str:
        return entry.experiment_id

    def validate_many(self, entries: Any) -> tuple[ExperimentSpec, ...]:
        try:
            return super().validate_many(entries)
        except DuplicateRegistrationError as exc:
            raise DuplicateExperimentError(exc.key) from exc

    def resolve(self, experiment_id: str) -> ExperimentSpec:
        experiment = self.get(experiment_id)
        if experiment is None:
            raise UnknownExperimentError(experiment_id)
        return experiment


def load_experiment_directory(
    directory: ExperimentDirectory, *, fragments: Optional[Mapping[str, Mapping[str, Any]]] = None
) -> tuple[ExperimentSpec, ...]:
    """Parse the immediate ``.yaml``/``.yml`` experiments of ``directory`` in name order, registering nothing."""

    path = Path(directory)
    if not path.is_dir():
        raise ExperimentDocumentError(f"experiment directory does not exist: {path}")
    files = sorted(
        (item for item in path.iterdir() if item.is_file() and item.suffix.lower() in (".yaml", ".yml")),
        key=lambda item: (item.name.casefold(), item.name),
    )
    experiments = []
    for document in files:
        try:
            text = document.read_text(encoding="utf-8")
        except OSError as exc:
            raise ExperimentDocumentError(f"{document}: {exc}") from exc
        experiments.append(load_experiment_yaml(text, source_name=str(document), fragments=fragments))
    return tuple(experiments)


# ---------------------------------------------------------------------------
# Component, generator, and deployment-provider registries
# ---------------------------------------------------------------------------

# One segment of a component registration ID: lowercase words joined by hyphens.
_COMPONENT_ID_SEGMENT = re.compile(r"[a-z0-9]+(?:-[a-z0-9]+)*")


def _check_id(label: str, entry_id: str, kind: str) -> None:
    """Reject a registration ID that is not ``<namespace>.<kind>.<name>``."""

    segments = entry_id.split(".")
    if (
        len(segments) != 3
        or segments[1] != kind
        or not all(_COMPONENT_ID_SEGMENT.fullmatch(segment) for segment in segments)
    ):
        raise RegistrationError(
            f"{label} {entry_id!r} must be named '<namespace>.{kind}.<name>' "
            "with lowercase segments and words joined by hyphens"
        )


class ComponentRegistry(BaseRegistry[ComponentRegistration[Any]]):
    """Component registrations of every role, each named ``<namespace>.<role>.<name>`` after its role."""

    label = "component"
    entry_type = ComponentRegistration

    def key(self, entry: ComponentRegistration[Any]) -> str:
        return entry.component_id

    def check(self, entry: ComponentRegistration[Any]) -> None:
        super().check(entry)
        _check_id("component", entry.component_id, entry.role.value)


class GeneratorRegistry(BaseRegistry[GeneratorRegistration]):
    """Generator hooks, named ``<namespace>.generator.<name>``."""

    label = "generator"
    entry_type = GeneratorRegistration

    def key(self, entry: GeneratorRegistration) -> str:
        return entry.generator_id

    def check(self, entry: GeneratorRegistration) -> None:
        super().check(entry)
        _check_id("generator", entry.generator_id, "generator")


class DeploymentProviderRegistry(BaseRegistry[DeploymentProviderRegistration]):
    """Deployment providers, keyed by ``(deployment_mode, provider_id)``.

    Several providers may implement one deployment mode; a scenario selects
    one with ``deployment.provider`` and may omit it when only one is registered.
    """

    label = "deployment provider"
    entry_type = DeploymentProviderRegistration

    def key(self, entry: DeploymentProviderRegistration) -> tuple[str, str]:
        return (entry.deployment_mode, entry.provider_id)

    def provider_ids(self, deployment_mode: str) -> tuple[str, ...]:
        """IDs of the providers registered for ``deployment_mode``, sorted."""

        return tuple(provider_id for registered, provider_id in self.registered_ids if registered == deployment_mode)

    def of_plan(self, plan: ExecutionPlan) -> Optional[DeploymentProviderRegistration]:
        """The provider ``plan`` names, if it is registered (which need not be the one it was compiled with)."""

        return self.get((plan.deployment_mode, plan.deployment_provider.provider_id))


# ---------------------------------------------------------------------------
# The catalog
# ---------------------------------------------------------------------------


class Catalog:
    """Everything one run service registers: components, generator hooks,
    experiments, artifact, resource, and deployment providers, and experiment
    fragments.

    Each :meth:`register` call and each batch of experiment directories is
    one transaction: a conflicting entry or a bad experiment file leaves the
    catalog unchanged. :meth:`freeze` then makes it read-only, so compilation
    and execution always see the same entries.
    """

    def __init__(self) -> None:
        self._components = ComponentRegistry()
        self._generators = GeneratorRegistry()
        self._experiments = ExperimentRegistry()
        self._environment_providers = EnvironmentProviderRegistry()
        self._deployment_providers = DeploymentProviderRegistry()
        self._fragments: dict[str, Mapping[str, Any]] = {}
        # experiment ID -> the experiment directory it was loaded from
        self._directories: dict[str, str] = {}
        self._frozen = False

    @property
    def components(self) -> ComponentRegistry:
        return self._components

    @property
    def generators(self) -> GeneratorRegistry:
        """The generator hooks random scenarios draw with."""

        return self._generators

    @property
    def experiments(self) -> ExperimentRegistry:
        return self._experiments

    @property
    def environment_providers(self) -> EnvironmentProviderRegistry:
        """Artifact and resource providers, keyed by ``(kind, provider_id)``."""

        return self._environment_providers

    @property
    def deployment_providers(self) -> DeploymentProviderRegistry:
        return self._deployment_providers

    @property
    def fragments(self) -> Mapping[str, Mapping[str, Any]]:
        """The registered experiment fragments, by name."""

        return MappingProxyType(dict(self._fragments))

    def experiment_directory(self, experiment_id: str) -> Optional[str]:
        """The experiment directory ``experiment_id`` was loaded from; None for one registered otherwise."""

        return self._directories.get(experiment_id)

    def _check_open(self) -> None:
        if self._frozen:
            raise FrozenRegistryError("the catalog is frozen")

    def register(
        self,
        *,
        components: Sequence[ComponentRegistration[Any]] = (),
        generators: Sequence[GeneratorRegistration] = (),
        experiments: Sequence[ExperimentSpec] = (),
        environment_providers: Sequence[EnvironmentProviderRegistration] = (),
        deployment_providers: Sequence[DeploymentProviderRegistration] = (),
        fragments: Optional[Mapping[str, Mapping[str, Any]]] = None,
    ) -> None:
        """Register every entry, or nothing if any conflicts."""

        self._check_open()
        frozen = {}
        for name, fragment in (fragments or {}).items():
            require_qualified_id(name, "experiment fragment name")
            # Experiments ``extends`` fragments by name, so one must not shadow another.
            if name in self._fragments:
                raise RegistrationError(f"experiment fragment {name!r} is already registered")
            frozen[name] = freeze_mapping(fragment, f"experiment fragment {name!r}")
        batches: list[tuple[BaseRegistry[Any], tuple[Any, ...]]] = [
            (self._components, tuple(components)),
            (self._generators, tuple(generators)),
            (self._experiments, tuple(experiments)),
            (self._environment_providers, tuple(environment_providers)),
            (self._deployment_providers, tuple(deployment_providers)),
        ]
        batches = [(registry, entries) for registry, entries in batches if entries]
        # Validate everything before changing anything.
        for registry, entries in batches:
            registry.validate_many(entries)
        for registry, entries in batches:
            registry.register_many(entries)
        self._fragments.update(frozen)

    def load_experiment_directories(self, directories: Iterable[ExperimentDirectory]) -> tuple[ExperimentSpec, ...]:
        """Parse the experiments in ``directories`` with the registered fragments, then register them together.

        A file that does not parse, or two experiments with one ID, leave the catalog unchanged.
        """

        self._check_open()
        experiments: list[ExperimentSpec] = []
        directories_of: dict[str, str] = {}
        for directory in directories:
            loaded = load_experiment_directory(directory, fragments=self._fragments)
            directories_of.update((spec.experiment_id, str(directory)) for spec in loaded)
            experiments.extend(loaded)
        self._experiments.register_many(experiments)
        self._directories.update(directories_of)
        return tuple(experiments)

    def freeze(self) -> None:
        """Make every registry read-only."""

        for registry in (
            self._components,
            self._generators,
            self._experiments,
            self._environment_providers,
            self._deployment_providers,
        ):
            registry.freeze()
        self._frozen = True


def build_catalog(
    experiment_directories: Sequence[ExperimentDirectory] = (),
    *,
    deployment_providers: Optional[Iterable[DeploymentProviderRegistration]] = None,
) -> Catalog:
    """A frozen catalog: the deployment providers, then ``experiment_directories``.

    ``deployment_providers`` default to the local deployment's.
    """

    catalog = Catalog()
    if deployment_providers is None:
        deployment_providers = local_deployment_provider_registrations().values()
    catalog.deployment_providers.register_many(deployment_providers)
    catalog.load_experiment_directories(experiment_directories)
    catalog.freeze()
    return catalog
