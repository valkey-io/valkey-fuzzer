"""Valkey fuzzer package.

The engine (catalog, compiler, plan, runner, verdict, cleanup) is kept apart
from the Valkey components in :mod:`valkey_fuzzer.components`, so each can be
tested on its own; :mod:`valkey_fuzzer.testing` runs components in-process.
"""

__version__ = "0.2.0"

__all__ = ["__version__"]
