"""The deployment's instances: selecting them, reading their replication, and sending them commands.

Components only talk to the generic run context
(:class:`~valkey_fuzzer.protocols.RunContext`).  Deployment access goes through the
``valkey.deployment`` service published by the local deployment provider.
"""

import re
import time
from collections.abc import Iterable, Mapping, Sequence
from typing import Any, Optional

import valkey

from valkey_fuzzer._data import jsonable
from valkey_fuzzer.deployment.local.queries import parse_replication
from valkey_fuzzer.plan import InstanceSelector
from valkey_fuzzer.schema import ConfigError

from .polling import poll_until, remaining

DEPLOYMENT_SERVICE = "valkey.deployment"


# The local deployment names its instances valkey-0, valkey-1, ...
INSTANCE_ID = re.compile(r"valkey-\d+")


def is_cluster(handle: Any) -> bool:
    """Whether the deployment behind ``handle`` runs in cluster mode."""

    return handle.mode.value == "cluster"


def deployment(context: Any) -> Any:
    try:
        return context.services["deployment"][DEPLOYMENT_SERVICE]
    except KeyError as exc:
        raise RuntimeError(f"run context does not provide the {DEPLOYMENT_SERVICE!r} deployment service") from exc


def validate_instance_selector(value: Any, name: str = "instances") -> None:
    """Accept what :class:`InstanceSelector` parses, naming only local instance IDs (``valkey-N``)."""

    try:
        selector = InstanceSelector.parse(value)
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"config field {name!r}: {exc}") from None
    invalid = [item for item in selector.instance_ids if not INSTANCE_ID.fullmatch(item)]
    if invalid:
        raise ConfigError(
            f"config field {name!r} must be all, primaries, primary, replicas, replica, or instance IDs such as "
            f"valkey-0; got {invalid}"
        )


def check_shard(shard: int, primaries: int, where: str = "shard") -> None:
    """``shard`` names one of the ``primaries`` cluster shards: Valkey cannot fail over one it does not have."""

    if shard >= primaries:
        raise ConfigError(f"{where} {shard} is not below primaries {primaries}; pick a shard from 0 to {primaries - 1}")


def select_instances(handle: Any, value: Any) -> list[str]:
    """Resolve an instance selector against the live deployment: live roles, planned order."""

    selector = InstanceSelector.parse(value)
    roles: dict[str, str] = {}
    if "primary" in selector.roles:
        roles.update(dict.fromkeys(handle.primaries(), "primary"))
    if "replica" in selector.roles:
        roles.update(dict.fromkeys(handle.replicas(), "replica"))
    return list(selector.resolve([instance.instance_id for instance in handle.instances], roles))


def select_instance(handle: Any, value: Any) -> str:
    """The first instance :func:`select_instances` resolves ``value`` to; ``LookupError`` when none is live in
    that role, which leaves the component's run untested (ERROR)."""

    selected = select_instances(handle, value)
    if not selected:
        raise LookupError(f"no live instance matches {value!r}")
    return selected[0]


def instance_at(handle: Any, host: Any, port: Any) -> Optional[str]:
    """The planned instance that listens at ``host:port``; None for any other address."""

    return next(
        (
            instance.instance_id
            for instance in handle.instances
            if (str(instance.host), str(instance.port)) == (str(host), str(port))
        ),
        None,
    )


def replication_info(handle: Any, instance_id: str, timeout: float) -> Optional[dict[str, Any]]:
    """``INFO replication`` of ``instance_id``; None when it does not answer within ``timeout``."""

    client = None
    try:
        client = handle.new_client(instance_id, timeout=timeout)
        return client.info("replication")
    except Exception:
        return None
    finally:
        close_quietly(client)


def is_primary(info: Optional[Mapping[str, Any]]) -> bool:
    """Whether ``info``, an instance's INFO replication (None: unreachable), reports a primary."""

    return bool(info) and parse_replication(info).role == "primary"


def follows(info: Optional[Mapping[str, Any]], primary: Any) -> bool:
    """Whether ``info``, an instance's INFO replication, is a replica of instance ``primary`` with its link up.

    Host and port both count: per-instance loopback addresses may share ports.
    """

    if not info:
        return False
    link = parse_replication(info)
    return (
        link.role == "replica"
        and link.link_up
        and (link.primary_host, link.primary_port) == (str(primary.host), int(primary.port))
    )


def replica_links(info: Mapping[str, Any], handle: Any) -> list[tuple[Any, Optional[str]]]:
    """A primary's replica entries (INFO replication's ``slaveN``, as ``parse_replication`` reads them), each with
    the instance at its address."""

    return [(entry, instance_at(handle, entry.ip, entry.port)) for entry in parse_replication(info).replicas]


def wait_following(
    context: Any, handle: Any, ids: Sequence[str], primary: str, timeout_s: float
) -> tuple[bool, dict[str, Optional[bool]]]:
    """Wait until every live instance of ``ids`` but ``primary`` :func:`follows` it: ``(all do, by instance)``,
    None for an instance that did not answer."""

    target = handle.instance(primary)

    def check() -> tuple[bool, dict[str, Optional[bool]]]:
        status = {}
        for item in ids:
            if item != primary and handle.is_alive(item):
                info = replication_info(handle, item, 0.5)
                status[item] = None if info is None else follows(info, target)
        return all(status.values()), status

    return poll_until(check, timeout_s=timeout_s, interval_s=0.1, context=context)


def graceful_failover(
    context: Any, handle: Any, primary: str, target: str, timeout_s: float
) -> tuple[Optional[bool], Optional[str]]:
    """Hand standalone ``primary``'s role to its replica ``target``: ``FAILOVER TO`` with a ``timeout_s`` limit.

    Valkey stops writes until ``target`` caught up, so nothing acknowledged is
    lost. The roles get ``timeout_s`` plus 2 seconds to swap; then ``FAILOVER
    ABORT`` stops a failover still in progress, so ``primary`` takes writes
    again. Returns whether ``target`` took over, and the error ``FAILOVER TO``
    raised, if any (a timeout may follow a failover that started, so the
    roles decide). Whether it took over is None when the harness cannot
    tell: ``FAILOVER TO`` got no reply (Valkey may never have seen it) and
    the roles did not swap, or the last look could not read them.

    ``target`` took over when it leads and ``primary`` does not, or when
    ``primary`` follows it with its failover state cleared, which Valkey does
    only once ``target`` accepted the handover: a ``target`` that died right
    after it (killed by a concurrent fault) was still promoted.
    """

    address = handle.instance(target)

    def swapped() -> tuple[bool, bool]:
        """Whether ``target`` took over, and whether the roles were read well enough to tell."""

        old = replication_info(handle, primary, 1.0)
        if old and not is_primary(old):
            link = parse_replication(old)
            handed_over = link.failover_state == "no-failover"
            if handed_over and (link.primary_host, link.primary_port) == (str(address.host), int(address.port)):
                return True, True
        new = replication_info(handle, target, 1.0)
        if is_primary(new) and not is_primary(old):
            return True, True
        # ``primary`` still leads, or both answered and neither handed over.
        return False, is_primary(old) or (old is not None and new is not None)

    def send(*command: Any) -> None:
        client = handle.new_client(primary, timeout=5.0)
        try:
            client.execute_command(*command)
        finally:
            close_quietly(client)

    error, unanswered = None, False
    try:
        send("FAILOVER", "TO", address.host, address.port, "TIMEOUT", max(1, int(timeout_s * 1000)))
    except valkey.exceptions.ResponseError as exc:
        error = f"{type(exc).__name__}: {exc}"
    except Exception as exc:
        error, unanswered = f"{type(exc).__name__}: {exc}", True
    if poll_until(lambda: (swapped()[0], None), timeout_s=timeout_s + 2.0, context=context)[0]:
        return True, error
    try:
        send("FAILOVER", "ABORT")
    except Exception:
        pass
    done, known = swapped()
    return (done if done or (known and not unanswered) else None), error


# Instances a component stopped on purpose: ``{"instances": [...]}``.
EXPECTED_DOWN = "valkey.instances.expected-down"


def stopped_on_purpose(handle: Any, evidence: Sequence[Any]) -> set[str]:
    """The instances some component stopped on purpose (:data:`EXPECTED_DOWN` evidence) that are not running:
    ``valkey.oracle.liveness`` judges them, and no other oracle can read them."""

    named = {
        instance
        for item in evidence
        if item.evidence_id == EXPECTED_DOWN
        for instance in item.value.get("instances", ())
    }
    return {instance for instance in named if not handle.is_alive(instance)}


def unreachable_instances(handle: Any, stopped: Iterable[str] = ()) -> list[str]:
    """Instances whose live replication role cannot be read, but for those ``stopped`` on purpose."""

    roles = handle.roles()
    skip = set(stopped)
    return [
        instance.instance_id
        for instance in handle.instances
        if instance.instance_id not in roles and instance.instance_id not in skip
    ]


def close_quietly(*clients: Any) -> None:
    """Close every client that is not None; a connection that is already gone is not an error."""

    for client in clients:
        if client is not None:
            try:
                client.close()
            except Exception:
                pass


# How the client reports a connection it could not open ("Error 49 connecting to 127.0.0.1:21120. Can't
# assign requested address.", "Timeout connecting to server"): nothing was sent.
_NOT_CONNECTED = re.compile(r"\bconnecting to\b")


def send_command(client: Any, command: Sequence[Any], *, timeout_s: float, context: Any) -> Any:
    """Send ``command`` once and return the reply.

    Until the connection is open nothing has reached the server, so a connection
    that could not be opened (a host out of local ports under load, a server not
    accepting yet) is retried until ``timeout_s``, bounded by the run deadline;
    then its error is raised. Any other error, a lost connection included, is
    raised at once, and the command is never sent twice.
    """

    deadline = time.monotonic() + remaining(context, timeout_s)
    while True:
        try:
            return client.execute_command(*[str(item) for item in command])
        except (valkey.exceptions.ConnectionError, valkey.exceptions.TimeoutError) as exc:
            if not _NOT_CONNECTED.search(str(exc)) or time.monotonic() >= deadline or not context.sleep(0.1):
                raise


def send_to(handle: Any, instance_id: str, command: Sequence[Any], *, timeout_s: float, context: Any) -> dict[str, Any]:
    """:func:`send_command` on a new client of ``instance_id``: ``{"reply": <as JSON data>}`` or ``{"error": ...}``.

    The server's error reply is the ``error``; a connection that failed or was lost, or a reply that timed out,
    also sets ``unanswered``: the harness did not see what the server did with the command. Any other error
    (the client refused the command) raises.
    """

    with handle.new_client(instance_id, timeout=timeout_s) as client:
        try:
            return {"reply": jsonable(send_command(client, command, timeout_s=timeout_s, context=context))}
        except (valkey.exceptions.ConnectionError, valkey.exceptions.TimeoutError, OSError) as exc:
            return {"error": f"{type(exc).__name__}: {exc}", "unanswered": True}
        except valkey.exceptions.ResponseError as exc:
            return {"error": f"{type(exc).__name__}: {exc}"}
