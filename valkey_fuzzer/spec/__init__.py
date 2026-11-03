"""The spec bridge: TLA+ models checked, turned into fuzzer scenarios, and compared with real runs.

A bundle ``<protocol>/`` holds the claims, the model, its configs, the
mapping to implementation evidence, negative controls, and optionally the
data-only experiments its counterexamples run in
(:mod:`~valkey_fuzzer.spec.bundle`). ``valkey-fuzzer spec`` runs the three
tools over it:

- :mod:`~valkey_fuzzer.spec.check`: SANY, TLC on the safe configs and the
  negative controls, model coverage, and the assurance report;
- :mod:`~valkey_fuzzer.spec.translate`: a counterexample to scenarios;
- :mod:`~valkey_fuzzer.spec.conform`: observed-state checks and action
  coverage of real runs (not refinement).
"""

from .bundle import Bundle, SpecBundleError, SpecMapping, find_bundle, load_bundle, load_spec_mapping
from .check import check_bundle
from .conform import collect_runs, conform_runs
from .tla import TlaError, TlaTools
from .translate import TranslateError, bundle_service, translate_trace

__all__ = [
    "Bundle",
    "SpecBundleError",
    "SpecMapping",
    "TlaError",
    "TlaTools",
    "TranslateError",
    "bundle_service",
    "check_bundle",
    "collect_runs",
    "conform_runs",
    "find_bundle",
    "load_bundle",
    "load_spec_mapping",
    "translate_trace",
]
