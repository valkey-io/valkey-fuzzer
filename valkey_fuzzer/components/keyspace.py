"""An instance's keyspace as canonical data, and its digest."""

import hashlib
import json
from collections.abc import Mapping
from typing import Any, Optional

from valkey_fuzzer._data import jsonable


def canonical_value(client: Any, key: str) -> Optional[list]:
    """Return ``[type, content]`` with order-insensitive types sorted, or None."""

    kind = client.type(key)
    if kind == "none":
        return None
    if kind == "string":
        content: Any = client.get(key)
    elif kind == "hash":
        content = sorted(client.hgetall(key).items())
    elif kind == "list":
        content = client.lrange(key, 0, -1)
    elif kind == "set":
        content = sorted(client.smembers(key))
    elif kind == "zset":
        content = [[member, repr(score)] for member, score in client.zrange(key, 0, -1, withscores=True)]
    elif kind == "stream":
        content = [[entry_id, sorted(fields.items())] for entry_id, fields in client.xrange(key)]
    else:
        content = f"<{kind}>"
    return [kind, jsonable(content)]


def keyspace_snapshot(client: Any, match: Optional[str] = None, count: int = 1000) -> dict[str, list]:
    """Canonical content of every key on one instance (optionally a SCAN pattern)."""

    snapshot: dict[str, list] = {}
    for key in client.scan_iter(match=match, count=count):
        value = canonical_value(client, key)
        if value is not None:
            snapshot[key] = value
    return snapshot


def keyspace_digest(snapshot: Mapping[str, Any]) -> str:
    material = json.dumps(sorted(snapshot.items()), separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(material.encode("utf-8", errors="surrogatepass")).hexdigest()
