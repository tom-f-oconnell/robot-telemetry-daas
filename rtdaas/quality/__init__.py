"""Data-quality rule engine: YAML check suites run against pandas, SQLite or Spark tables."""

from rtdaas.quality.engine import (
    CheckResult,
    has_blocking_failures,
    load_suite,
    run_suite,
    to_frame,
)

__all__ = ["CheckResult", "has_blocking_failures", "load_suite", "run_suite", "to_frame"]
