"""Files the Valkey components read and write: JSONL records, the vars of a node's nodes.conf, and Valkey log lines."""

import json
import os
import re
import time
from collections.abc import Iterable, Mapping
from typing import Any, Optional

from valkey_fuzzer._data import jsonable


def read_jsonl(path: str) -> list[dict[str, Any]]:
    entries = []
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                entries.append(json.loads(line))
    return entries


def write_jsonl(path: str, entries: Iterable[Mapping[str, Any]]) -> None:
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as handle:
        for entry in entries:
            handle.write(json.dumps(jsonable(entry), sort_keys=True))
            handle.write("\n")
    os.replace(tmp, path)


def nodes_conf_path(handle: Any, instance_id: str) -> str:
    return os.path.join(handle.data_dir(instance_id), "nodes.conf")


def read_cluster_vars(path: str) -> Optional[dict[str, int]]:
    """Return the ``vars`` line of a nodes.conf (currentEpoch, lastVoteEpoch), or None."""

    try:
        with open(path, encoding="utf-8", errors="replace") as handle:
            for line in handle:
                if line.startswith("vars "):
                    tokens = line.split()[1:]
                    values = {}
                    for name, value in zip(tokens[::2], tokens[1::2]):
                        try:
                            values[name] = int(value)
                        except ValueError:
                            continue
                    return values
    except OSError:
        return None
    return None


# A Valkey log line in the legacy format: ``pid:role dd Mon yyyy hh:mm:ss.mmm level message``, where the level is
# ``.`` (debug), ``-`` (verbose), ``*`` (notice), or ``#`` (warning).
LOG_LINE = re.compile(
    r"^\d+:[A-Za-z] (?P<time>\d{1,2} \w{3} \d{4} \d{2}:\d{2}:\d{2})\.(?P<ms>\d{3}) (?P<level>[#*.-]) (?P<message>.*)$"
)


def log_line_time(line: str) -> Optional[float]:
    """Wall-clock time of a legacy-format Valkey log line (:data:`LOG_LINE`)."""

    match = LOG_LINE.match(line)
    if match is None:
        return None
    try:
        whole = time.mktime(time.strptime(match["time"], "%d %b %Y %H:%M:%S"))
    except ValueError:
        return None
    return whole + int(match["ms"]) / 1000.0
