"""Capability probes that run before the deployment starts.

``valkey.capability.build-probe`` starts each distinct binary the run will use
as a private, TCP-less server on a unix socket with the requested startup
configuration, then checks versions, configuration values, INFO fields, and
commands.  Each of these names a feature the experiment needs, so a build
that lacks one is ``SKIP`` (unsupported). A probe that cannot run at all, a
binary that is not the build the experiment needs (it knows a config in
``lacks_configs``), and fewer distinct builds than ``min_distinct_builds`` are
setup errors: ``ERROR``.

With ``deployment_config`` it also starts each deployment binary with the
configuration the deployment gives the instances that run it, and only asks
whether the build accepts it. ``deployment_config`` names the settings the
build may lack (such as ``rdbcompression``, whose codec a build may not have):
a rejected configuration is ``SKIP`` only when the build starts without those
settings, and ``ERROR`` otherwise, so a typo in any other setting never skips.
It never judges whether that configuration is what the experiment wants: a
scenario that turns a feature off runs, and lacks the experiment's path
evidence.

``valkey.capability.host-probe`` makes a run ``SKIP_UNSUPPORTED`` on a host
that cannot run it: not Linux, not root (or root, for permission faults), no
tmpfs mounts, no usable iptables, or no per-instance loopback addresses.
"""

import functools
import os
import platform
import shutil
import socket
from collections.abc import Mapping
from typing import Any, Callable

import valkey

from valkey_fuzzer.declarations import Declarations
from valkey_fuzzer.deployment.local.processes import SOURCE_BUILD_DIR
from valkey_fuzzer.hoststate import run_iptables
from valkey_fuzzer.plan import ComponentSpec, ExecutionPlan
from valkey_fuzzer.results import ComponentStatus, make_evidence, make_result
from valkey_fuzzer.schema import ConfigError, ConfigField, plain_config

from . import providers
from .registration import component_registration
from .schemas import ANY, LIST, TEXT, declared, mapping_of
from .server import PrivateServer, server_arguments, server_identity, version_tuple

_UNSUPPORTED_PATTERNS = (
    "Bad directive or wrong number of arguments",
    "FATAL CONFIG FILE ERROR",
    "Unknown option",
    "can only be selected when",
    "Invalid argument",
    "argument must be",
    "Unrecognized",
)

_PROBE_FIELDS = {
    "startup_config": ConfigField(dict, default=dict),
    # {config: [accepted values], or null for any value}: the build must know each config.
    "config_get": mapping_of(additional=ConfigField(list, allow_none=True), default=dict),
    # {config: [values]}: CONFIG SET must accept each value at runtime.
    "config_set": mapping_of(additional=ConfigField(list), default=dict),
    # {section: [field names]}: INFO must report each.
    "info_fields": mapping_of(additional=ConfigField(list, items=TEXT), default=dict),
    "commands": ConfigField(list, default=list, items=TEXT),
    "min_version": ConfigField(str, allow_none=True, default=None),
    "min_distinct_builds": ConfigField(int, default=1, minimum=1, maximum=64),
    # Binaries the run starts later (e.g. a rolling-upgrade target), probed
    # alongside the deployment's own; usually artifact references.
    "binaries": ConfigField(list, default=list, reference=True, items=ConfigField(str, reference=True)),
    "timeout_s": ConfigField(float, default=15.0, minimum=1, maximum=120),
    # all: the deployment's binaries and `binaries`; listed: only `binaries`.
    "scope": ConfigField(str, default="all", choices=("all", "listed")),
    # Configs the build must NOT know (CONFIG GET returns nothing), e.g. to
    # prove a binary really is older than a feature; one it knows is ERROR.
    "lacks_configs": ConfigField(list, default=list, items=TEXT),
    # The settings the build may lack, such as rdbcompression: also start each
    # deployment binary with the server config the deployment gives the
    # instances that run it (deployment, instance, and startup overlay
    # layers), once per distinct config. A config the build rejects is SKIP
    # when the build starts without these settings (zstd without BUILD_ZSTD),
    # and ERROR otherwise. Only whether the server starts counts, never the
    # values it runs with. Empty: the deployment config is not probed.
    "deployment_config": ConfigField(list, default=list, items=TEXT),
}


def _validate_probe(config: Mapping[str, Any], spec: ComponentSpec, plan: ExecutionPlan) -> None:
    for key, values in config["config_set"].items():
        if not values:
            raise ConfigError(f"config_set.{key} must list at least one value")
    if config["min_version"] is not None and not version_tuple(config["min_version"]):
        raise ConfigError("min_version must look like '8.1.0'")
    if not all(config["lacks_configs"]):
        raise ConfigError("lacks_configs must hold config names; remove the empty one")
    if set(config["lacks_configs"]) & set(config["config_get"]):
        raise ConfigError("a config cannot be both required (config_get) and absent (lacks_configs)")
    if config["scope"] == "listed":
        if not config["binaries"]:
            raise ConfigError("scope: listed probes only `binaries`, so it needs at least one")
        if config["deployment_config"]:
            raise ConfigError("deployment_config applies to the deployment's binaries, which scope: listed skips")


def _planned_roles(plan: ExecutionPlan) -> dict[str, str]:
    """``{instance_id: role}`` of the instances the plan's deployment provider plans, in its order."""

    return {instance_id: str(instance.get("role")) for instance_id, instance in plan.instances.items()}


def plan_binaries(plan: ExecutionPlan) -> dict[str, str]:
    """Return {label: binary} for every binary the plan's deployment starts.

    ``instance_binaries`` overrides (by instance ID or role) add one entry each; the
    deployment default (``valkey_binary``, else the orchestrator's default) is
    included when at least one planned instance uses it. Artifact references are
    bound before capability probes run.
    """

    document = plain_config(plan.deployment)
    overrides = document.get("instance_binaries") or {}
    roles = _planned_roles(plan)
    uses_default = not roles or any(
        instance not in overrides and role not in overrides for instance, role in roles.items()
    )
    binaries: dict[str, str] = {}
    if uses_default:
        binaries["default"] = document.get("valkey_binary") or ""
    for selector, binary in sorted(overrides.items()):
        binaries[f"instance_binaries.{selector}"] = binary
    return binaries


def _binary_label(overrides: Mapping[str, Any], instance_id: str, role: str) -> str:
    """The :func:`plan_binaries` label of the binary an instance starts with: the deployment's override for its
    ID, else for its role, else the default."""

    selector = next((selector for selector in (instance_id, role) if overrides.get(selector)), None)
    return "default" if selector is None else f"instance_binaries.{selector}"


def planned_server_configs(plan: ExecutionPlan) -> dict[str, list[tuple[list[str], dict[str, Any]]]]:
    """``{binary label: [(instance IDs, server config), ...]}``, one entry per distinct config.

    Each config is the instance's effective one, as the plan records it: the
    deployment provider's defaults under the deployment's ``server_config``,
    its ``instance_configs``, and the startup overlays. Labels are those of
    :func:`plan_binaries`.
    """

    overrides = plain_config(plan.deployment).get("instance_binaries") or {}
    grouped: dict[str, list[tuple[list[str], dict[str, Any]]]] = {}
    for instance_id, instance in plan.instances.items():
        config = plain_config(instance["server_config"])
        entries = grouped.setdefault(_binary_label(overrides, instance_id, str(instance.get("role"))), [])
        same = next((entry for entry in entries if entry[1] == config), None)
        if same is None:
            entries.append(([instance_id], config))
        else:
            same[0].append(instance_id)
    return grouped


# Where the local deployment builds valkey-server when none is configured or on PATH.
_SOURCE_BUILD = os.path.join(SOURCE_BUILD_DIR, "valkey", "src", "valkey-server")


def resolve_binary(binary: str) -> str:
    """The executable to probe for a deployment's ``binary``; an empty one is the deployment's own default.

    Raises ``OSError`` naming the binary when it is missing or not executable.
    """

    if binary:
        return providers.resolve_binary(binary)
    try:
        return providers.resolve_binary("valkey-server")
    except FileNotFoundError:
        if os.path.isfile(_SOURCE_BUILD):
            return _SOURCE_BUILD
        raise FileNotFoundError(
            "valkey-server is not on PATH; set deployment.valkey_binary or put valkey-server on PATH"
        ) from None


Probed = tuple[ComponentStatus, str, dict[str, Any]]


def _run_private_server(
    context: Any,
    spec: ComponentSpec,
    binary: str,
    startup_config: Mapping[str, Any],
    timeout_s: float,
    check: Callable[[Any, dict[str, Any]], Probed],
) -> Probed:
    """Start ``binary`` as a private server with ``startup_config`` and return ``check(client, facts)``: (status,
    summary, facts). A binary that is missing, or a server that cannot start or exits, is that instead.

    The private server is owned until it is stopped.
    """

    try:
        resolved = resolve_binary(binary)
    except OSError as exc:
        return ComponentStatus.ERROR, str(exc), {}
    server = PrivateServer(resolved, server_arguments(startup_config), timeout_s=timeout_s, prefix="vfp-")
    owned = context.obligations.open(spec, "process.private-server", server.workdir, server.stop)
    facts: dict[str, Any] = {"binary": resolved}
    try:
        try:
            exit_reason = server.start()
        except (OSError, TimeoutError) as exc:
            return ComponentStatus.ERROR, f"probe server failed: {exc}", facts
        if exit_reason is not None:
            facts["exit_output"] = exit_reason
            if any(pattern.lower() in exit_reason.lower() for pattern in _UNSUPPORTED_PATTERNS):
                return ComponentStatus.SKIP, f"build rejects startup configuration: {exit_reason[-300:]}", facts
            return ComponentStatus.ERROR, f"probe server exited: {exit_reason[-300:]}", facts
        return check(server.client, facts)
    finally:
        owned.discharge()


def probe_binary(context: Any, spec: ComponentSpec, binary: str, config: Mapping[str, Any]) -> Probed:
    """Probe one binary for component ``spec`` against the capabilities ``config`` declares."""

    return _run_private_server(
        context,
        spec,
        binary,
        config["startup_config"],
        config["timeout_s"],
        functools.partial(_check_capabilities, config),
    )


def starts_with(
    context: Any, spec: ComponentSpec, binary: str, server_config: Mapping[str, Any], timeout_s: float
) -> Probed:
    """Whether ``binary`` starts with ``server_config``; only the start counts, never the values it runs with."""

    return _run_private_server(
        context,
        spec,
        binary,
        server_config,
        timeout_s,
        lambda client, facts: (ComponentStatus.PASS, "the build starts with it", facts),
    )


def starts_with_named(
    context: Any, spec: ComponentSpec, binary: str, server_config: Mapping[str, Any], named: Any, timeout_s: float
) -> Probed:
    """Whether ``binary`` starts with ``server_config``: a rejection is SKIP only when it starts without the
    ``named`` settings, the ones the experiment says the build may lack, and ERROR otherwise."""

    status, summary, facts = starts_with(context, spec, binary, server_config, timeout_s)
    if status is not ComponentStatus.SKIP:
        return status, summary, facts
    lacking = sorted(set(server_config) & set(named))
    without = {name: value for name, value in server_config.items() if name not in named}
    if lacking and starts_with(context, spec, binary, without, timeout_s)[0] is ComponentStatus.PASS:
        return ComponentStatus.SKIP, f"{summary} (it starts without {', '.join(lacking)}, which it may lack)", facts
    return ComponentStatus.ERROR, f"{summary} (not a setting the experiment says the build may lack)", facts


def _check_capabilities(config: Mapping[str, Any], client: Any, facts: dict[str, Any]) -> Probed:
    """The capability checks of ``config`` against a started probe server's ``client``."""

    info_server = client.info("server")
    identity = server_identity(info_server)
    version = identity["version"]
    facts["version"] = version
    facts["git_sha1"] = identity["git_sha"]
    facts["server_name"] = str(info_server.get("server_name") or "")
    if config["min_version"] is not None and version_tuple(version) < version_tuple(config["min_version"]):
        return ComponentStatus.SKIP, f"version {version} is older than {config['min_version']}", facts

    config_values: dict[str, Any] = {}
    for name, accepted in config["config_get"].items():
        reply = client.config_get(name)
        if name not in reply:
            return ComponentStatus.SKIP, f"build does not support config {name!r}", facts
        config_values[name] = reply[name]
        if accepted is not None and str(reply[name]) not in {str(item) for item in accepted}:
            return (
                ComponentStatus.SKIP,
                f"config {name!r} is {reply[name]!r}, not one of {list(accepted)}",
                {**facts, "config": config_values},
            )
    facts["config"] = config_values
    for name, values in config["config_set"].items():
        for value in values:
            try:
                client.config_set(name, server_arguments({name: value})[1])
            except valkey.exceptions.ResponseError as exc:
                return ComponentStatus.SKIP, f"build rejects CONFIG SET {name} {value}: {exc}", facts
    for name in config["lacks_configs"]:
        if name in client.config_get(name):
            # Not the build the experiment needs, such as an "old" build that is not old: a setup error.
            return ComponentStatus.ERROR, f"build supports config {name!r}, which it must lack", facts

    info_values: dict[str, Any] = {}
    for section, field_names in config["info_fields"].items():
        section_info = client.info(section)
        for field_name in field_names:
            if field_name not in section_info:
                return (
                    ComponentStatus.SKIP,
                    f"INFO {section} does not report {field_name!r}",
                    {**facts, "info": info_values},
                )
            info_values[f"{section}.{field_name}"] = section_info[field_name]
    facts["info"] = info_values

    for command in config["commands"]:
        try:
            reply = client.execute_command("COMMAND", "INFO", command)
        except TypeError:
            # valkey-py cannot parse the nil entry an unknown command returns.
            reply = None
        if not isinstance(reply, Mapping) or command.lower() not in {name.lower() for name in reply}:
            return ComponentStatus.SKIP, f"build does not implement command {command!r}", facts
    return ComponentStatus.PASS, "capability probe passed", facts


class BuildProbe:
    """Probe every binary the plan will start against declared capabilities."""

    def probe(self, context: Any, spec: ComponentSpec):
        config = spec.config
        results = {}
        worst = ComponentStatus.PASS
        summaries = []
        order = {ComponentStatus.PASS: 0, ComponentStatus.SKIP: 1, ComponentStatus.FAIL: 2, ComponentStatus.ERROR: 3}
        binaries = plan_binaries(context.plan) if config["scope"] == "all" else {}
        for index, binary in enumerate(config["binaries"]):
            binaries[f"binaries[{index}]"] = binary
        planned = planned_server_configs(context.plan) if config["deployment_config"] else {}

        def record(probe_label: str, probed: Probed) -> None:
            nonlocal worst
            status, summary, facts = probed
            results[probe_label] = {"status": status.value, "summary": summary, **facts}
            if order[status] > order[worst]:
                worst = status
            if status is not ComponentStatus.PASS:
                summaries.append(f"{probe_label}: {summary}")

        builds = set()
        for label, binary in binaries.items():
            # The capability checks with the probe's own startup config, then one start per distinct deployment
            # config.
            record(label, probe_binary(context, spec, binary, config))
            builds.add((results[label].get("version"), results[label].get("git_sha1")))
            for instances, server_config in planned.get(label, ()):
                name = f"{label} deployment config" + ("" if len(planned[label]) == 1 else f" ({', '.join(instances)})")
                startup_config = {**config["startup_config"], **server_config}
                named = {*config["deployment_config"], *config["startup_config"]}
                record(name, starts_with_named(context, spec, binary, startup_config, named, config["timeout_s"]))
        if worst is ComponentStatus.PASS:
            if len(builds) < config["min_distinct_builds"]:
                # A second build is an input the operator gives; without it the run cannot be set up.
                worst = ComponentStatus.ERROR
                summaries.append(
                    f"scenario needs {config['min_distinct_builds']} distinct builds, the deployment provides {len(builds)}"
                )
        evidence = ()
        if worst is ComponentStatus.PASS:
            evidence = (make_evidence(spec, "valkey.capability.supported", "capability.completed", results),)
        return make_result(
            spec,
            worst,
            "; ".join(summaries) or "every binary supports the requested capabilities",
            evidence=evidence,
            diagnostics={"binaries": results},
        )


def iptables_supported() -> tuple[bool, str]:
    if not providers.on_linux():
        return False, "network partitions require Linux iptables"
    if not providers.as_root():
        return False, "network partitions require root"
    binary = shutil.which("iptables")
    if binary is None:
        return False, "iptables is not installed"
    probe = run_iptables("-L", "INPUT", "-n")
    if probe.returncode != 0:
        return False, f"iptables is not usable: {probe.stderr.strip()[:200]}"
    return True, ""


def per_instance_loopback_supported() -> tuple[bool, str]:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind(("127.0.0.2", 0))
    except OSError as exc:
        return False, f"cannot bind 127.0.0.2 ({exc}); per-instance loopback addressing is unavailable"
    finally:
        sock.close()
    return True, ""


# ---------------------------------------------------------------------------
# valkey.capability.host-probe
# ---------------------------------------------------------------------------

_HOST_REQUIREMENTS = ("linux", "root", "not_root", "tmpfs", "iptables", "per_instance_loopback")

_HOST_FIELDS = {"requires": ConfigField(list, items=ConfigField(str, choices=_HOST_REQUIREMENTS))}


def _validate_host(config: dict, spec: ComponentSpec, plan: ExecutionPlan) -> None:
    if not config["requires"]:
        raise ConfigError(f"requires must list some of {list(_HOST_REQUIREMENTS)}")


class HostCapabilityProbe:
    """``SKIP`` when the host cannot run a scenario (e.g. no tmpfs mounts)."""

    def probe(self, context: Any, spec: ComponentSpec):
        config = spec.config
        unmet = []
        for requirement in config["requires"]:
            if requirement == "linux" and not providers.on_linux():
                unmet.append("requires Linux")
            elif requirement == "root" and not providers.as_root():
                unmet.append("requires root")
            elif requirement == "not_root" and providers.as_root():
                unmet.append("permission faults are ineffective as root")
            elif requirement in ("tmpfs", "iptables", "per_instance_loopback"):
                check = {
                    "tmpfs": providers.tmpfs_supported,
                    "iptables": iptables_supported,
                    "per_instance_loopback": per_instance_loopback_supported,
                }[requirement]
                supported, reason = check()
                if not supported:
                    unmet.append(reason)
        if unmet:
            return make_result(spec, ComponentStatus.SKIP, "; ".join(unmet))
        return make_result(
            spec,
            ComponentStatus.PASS,
            "host satisfies " + ", ".join(config["requires"]),
            evidence=(
                make_evidence(
                    spec,
                    "valkey.host.supported",
                    "capability.completed",
                    {"platform": platform.platform(), "requires": config["requires"]},
                ),
            ),
        )


# {probe label: its status, summary, and the facts the probe read}
_PROBE_RESULT = mapping_of({"status": TEXT, "summary": TEXT, "binary": TEXT}, additional=ANY)
_PROBE_DECLARES = Declarations(
    evidence={"valkey.capability.supported": declared(mapping_of(additional=_PROBE_RESULT))},
    changes=("process.private-server",),
)
_HOST_DECLARES = Declarations(
    evidence={"valkey.host.supported": declared(mapping_of({"platform": TEXT, "requires": LIST}))}
)

REGISTRATIONS = (
    component_registration(
        "valkey.capability.build-probe", BuildProbe, _PROBE_FIELDS, check=_validate_probe, declares=_PROBE_DECLARES
    ),
    component_registration(
        "valkey.capability.host-probe", HostCapabilityProbe, _HOST_FIELDS, check=_validate_host, declares=_HOST_DECLARES
    ),
)
