"""An instance's keyspace as canonical data, and its digest."""

import base64
import hashlib
import json
from collections.abc import Mapping
from typing import Any, Optional

from valkey_fuzzer._data import jsonable

# The types canonical_value reads as data; any other is a module's.
BUILTIN_TYPES = frozenset({"string", "hash", "list", "set", "zset", "stream"})
# A DUMP payload ends with the RDB version (2 bytes) and a CRC64 (8 bytes) over the rest, which tell the build
# that wrote it, not the value.
_DUMP_TRAILER_BYTES = 10


def dump_body(payload: bytes) -> bytes:
    """A DUMP payload without its RDB version and CRC64: the value as its type serializes it."""

    return payload[:-_DUMP_TRAILER_BYTES]


def exact(value: Any) -> Any:
    """A reply as data that keeps every byte: bytes as the string they decode to as UTF-8, else
    ``{"$base64": ...}``; a list element by element. Values a decoding client read stay as they are."""

    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError:
            return {"$base64": base64.b64encode(value).decode("ascii")}
    if isinstance(value, (list, tuple)):
        return [exact(item) for item in value]
    return value


def _ordered(items: Any) -> list:
    return sorted(items, key=lambda item: json.dumps(item, sort_keys=True))


def _key(key: Any) -> str:
    """A key as a snapshot names it: its text, or ``$base64:...`` for a key that is not UTF-8."""

    named = exact(key)
    return named if isinstance(named, str) else f"$base64:{named['$base64']}"


def canonical_value(client: Any, key: Any) -> Optional[list]:
    """Return ``[type, content]`` with order-insensitive types sorted, or None. ``client`` may decode its replies
    or not: one that does not reads binary values (valkey-search's vectors) exactly.

    A type it does not read, a module's, is ``[type, SHA-256 of its DUMP payload less the RDB version and
    CRC64]``: the module's serialization, which compares across processes only when the module writes a value
    the same way every time (``valkey.oracle.dataset-digest`` probes that).
    """

    kind = exact(client.type(key))
    if kind == "none":
        return None
    if kind == "string":
        content: Any = exact(client.get(key))
    elif kind == "hash":
        content = _ordered([exact(field), exact(value)] for field, value in client.hgetall(key).items())
    elif kind == "list":
        content = exact(client.lrange(key, 0, -1))
    elif kind == "set":
        content = _ordered(exact(member) for member in client.smembers(key))
    elif kind == "zset":
        content = [[exact(member), repr(score)] for member, score in client.zrange(key, 0, -1, withscores=True)]
    elif kind == "stream":
        content = [
            [exact(entry_id), _ordered([exact(field), exact(value)] for field, value in fields.items())]
            for entry_id, fields in client.xrange(key)
        ]
    else:
        # DUMP is never decoded, so a decoding client returns the payload as bytes.
        payload = client.dump(key)
        if payload is None:
            return None
        content = hashlib.sha256(dump_body(payload)).hexdigest()
    return [kind, jsonable(content)]


def keyspace_snapshot(client: Any, match: Optional[str] = None, count: int = 1000) -> dict[str, list]:
    """Canonical content of every key on one instance (optionally a SCAN pattern)."""

    snapshot: dict[str, list] = {}
    for key in client.scan_iter(match=match, count=count):
        value = canonical_value(client, key)
        if value is not None:
            snapshot[_key(key)] = value
    return snapshot


def keyspace_digest(snapshot: Mapping[str, Any]) -> str:
    material = json.dumps(sorted(snapshot.items()), separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(material.encode("utf-8", errors="surrogatepass")).hexdigest()
