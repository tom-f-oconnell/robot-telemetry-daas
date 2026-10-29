"""YAML-driven data-quality checks (spec §4.6).

A suite lists tables; each table lists checks. Built-in check types:
``not_null``, ``row_count``, ``value_range``, ``sample_rate``, ``schema_match``, ``unique``,
``freshness``, ``sql``. Every check yields a :class:`CheckResult` with status
``pass`` / ``fail`` / ``error`` / ``skipped`` and severity ``blocking`` / ``warning``.
The same suite runs unchanged against pandas frames, the SQLite mirror or Iceberg via Spark.
"""

from __future__ import annotations

import logging
import operator
import uuid
from collections.abc import Callable, Iterable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yaml

from rtdaas.quality.adapters import TableAdapter
from rtdaas.quality.schemas import ARM_STRUCT_FIELDS, SPEC_SCHEMAS

log = logging.getLogger(__name__)

SEVERITIES = ("blocking", "warning")
STATUSES = ("pass", "fail", "error", "skipped")
RESULT_COLUMNS: tuple[str, ...] = (
    "run_id",
    "suite",
    "table_name",
    "check_name",
    "severity",
    "status",
    "observed",
    "threshold",
    "partition_value",
    "run_ts",
    "message",
)
_OPS: dict[str, Callable[[float, float], bool]] = {
    "<": operator.lt,
    "<=": operator.le,
    ">": operator.gt,
    ">=": operator.ge,
    "==": operator.eq,
    "=": operator.eq,
    "!=": operator.ne,
}


@dataclass
class CheckResult:
    suite: str
    table_name: str
    check_name: str
    severity: str
    status: str
    observed: float | None
    threshold: str
    partition_value: str | None
    message: str

    @property
    def failed(self) -> bool:
        return self.status == "fail"


def load_suite(path: Path | str) -> dict:
    """Load and lightly validate a suite YAML file."""
    with open(path) as fh:
        suite = yaml.safe_load(fh) or {}
    if "suite" not in suite:
        suite["suite"] = Path(path).stem
    tables = suite.get("tables") or []
    if not isinstance(tables, list):
        raise ValueError(f"{path}: 'tables' must be a list")
    for t in tables:
        if "table" not in t:
            raise ValueError(f"{path}: every table entry needs a 'table' key")
        for c in t.get("checks") or []:
            if "type" not in c:
                raise ValueError(f"{path}: check {c.get('name')!r} on {t['table']} has no 'type'")
            c.setdefault("name", c["type"])
            sev = c.setdefault("severity", "warning")
            if sev not in SEVERITIES:
                raise ValueError(f"{path}: severity {sev!r} must be one of {SEVERITIES}")
    return suite


# --------------------------------------------------------------------------- check implementations
# Each takes (frame or None, check dict, context dict) and returns (status, observed, threshold, message).


def _ts_series(frame: pd.DataFrame, column: str) -> pd.Series:
    s = frame[column]
    if pd.api.types.is_datetime64_any_dtype(s.dtype):
        return pd.to_datetime(s, utc=True)
    return pd.to_datetime(s, utc=True, format="ISO8601")


def _ts_ns(frame: pd.DataFrame, column: str) -> np.ndarray:
    """Timestamps -> int64 nanoseconds since epoch, independent of the parsed resolution
    (pandas 3 infers ``datetime64[us]`` for microsecond ISO strings)."""
    t = _ts_series(frame, column)
    return (
        t.dt.tz_convert("UTC")
        .dt.tz_localize(None)
        .to_numpy()
        .astype("datetime64[ns]")
        .astype(np.int64)
    )


def _check_not_null(
    frame: pd.DataFrame, check: dict, ctx: dict
) -> tuple[str, float | None, str, str]:
    col = check["column"]
    max_frac = float(check.get("max_null_fraction", 0.0))
    if col not in frame.columns:
        return "fail", None, f"<= {max_frac}", f"column {col!r} missing"
    if len(frame) == 0:
        return "pass", 0.0, f"<= {max_frac}", "no rows"
    frac = float(frame[col].isna().mean())
    return (
        ("pass" if frac <= max_frac else "fail"),
        frac,
        f"<= {max_frac}",
        f"null fraction of {col}: {frac:.4f}",
    )


def _check_row_count(
    frame: pd.DataFrame, check: dict, ctx: dict
) -> tuple[str, float | None, str, str]:
    n = len(frame)
    lo = check.get("min")
    hi = check.get("max")
    ok = (lo is None or n >= lo) and (hi is None or n <= hi)
    threshold = (
        " and ".join(
            p
            for p in [f">= {lo}" if lo is not None else "", f"<= {hi}" if hi is not None else ""]
            if p
        )
        or "any"
    )
    return ("pass" if ok else "fail"), float(n), threshold, f"{n} rows"


def _check_value_range(
    frame: pd.DataFrame, check: dict, ctx: dict
) -> tuple[str, float | None, str, str]:
    col = check["column"]
    if col not in frame.columns:
        return "fail", None, "", f"column {col!r} missing"
    lo, hi = check.get("min"), check.get("max")
    max_frac = float(check.get("max_violation_fraction", 0.0))
    vals = pd.to_numeric(frame[col], errors="coerce").dropna()
    if len(vals) == 0:
        return "pass", 0.0, f"[{lo}, {hi}]", "no non-null values"
    bad = np.zeros(len(vals), dtype=bool)
    if lo is not None:
        bad |= vals.to_numpy() < lo
    if hi is not None:
        bad |= vals.to_numpy() > hi
    frac = float(bad.mean())
    status = "pass" if frac <= max_frac else "fail"
    return (
        status,
        frac,
        f"[{lo}, {hi}] violations <= {max_frac}",
        f"{int(bad.sum())}/{len(vals)} values of {col} outside range",
    )


def _check_sample_rate(
    frame: pd.DataFrame, check: dict, ctx: dict
) -> tuple[str, float | None, str, str]:
    ts_col = check.get("ts_column", "ts")
    expected = float(check["expected_hz"])
    tol = float(check.get("tolerance_fraction", 0.1))
    group_by = check.get("group_by") or []
    if ts_col not in frame.columns:
        return "fail", None, f"{expected} Hz ±{tol:.0%}", f"column {ts_col!r} missing"
    if len(frame) < 3:
        return "skipped", None, f"{expected} Hz ±{tol:.0%}", "fewer than 3 samples"
    df = frame[[ts_col, *group_by]].copy()
    df["_ns"] = _ts_ns(df, ts_col)
    worst_dev = 0.0
    worst_hz = expected
    offenders: list[str] = []
    groups = df.groupby(group_by, sort=False) if group_by else [((), df)]
    for key, g in groups:
        ns = np.sort(g["_ns"].to_numpy())
        if ns.size < 3:
            continue
        dt = np.diff(ns) / 1e9
        dt = dt[dt > 0]
        if dt.size == 0:
            continue
        hz = 1.0 / float(np.median(dt))
        dev = abs(hz - expected) / expected
        if dev > worst_dev:
            worst_dev, worst_hz = dev, hz
        if dev > tol:
            offenders.append(f"{key}: {hz:.2f} Hz")
    status = "pass" if worst_dev <= tol else "fail"
    msg = f"median rate {worst_hz:.2f} Hz (expected {expected} Hz)" + (
        f"; offenders: {', '.join(offenders[:5])}" if offenders else ""
    )
    return status, worst_hz, f"{expected} Hz ±{tol:.0%}", msg


def _check_schema_match(
    frame: pd.DataFrame | None, check: dict, ctx: dict
) -> tuple[str, float | None, str, str]:
    table = ctx["table_name"]
    expected_key = check.get("expected", "spec")
    if expected_key == "spec":
        expected = SPEC_SCHEMAS.get(table)
        if expected is None:
            return "error", None, "spec", f"no spec schema for {table}"
    else:
        expected = dict(expected_key)  # inline {column: type}
    actual = set(ctx["adapter"].columns(table).keys())
    missing: list[str] = []
    for col, typ in expected.items():
        if col in actual:
            continue
        if typ.startswith("struct<") and all(f"{col}_{f}" in actual for f in ARM_STRUCT_FIELDS):
            continue  # flattened struct (SQLite mirror)
        missing.append(col)
    expected_flat = set(expected) | {
        f"{c}_{f}"
        for c, t in expected.items()
        if t.startswith("struct<")
        for f in ARM_STRUCT_FIELDS
    }
    extra = sorted(actual - expected_flat)
    if missing:
        return (
            "fail",
            float(len(missing)),
            "all spec columns present",
            f"missing columns: {', '.join(missing)}",
        )
    msg = "all spec columns present" + (f"; extra columns: {', '.join(extra)}" if extra else "")
    return "pass", 0.0, "all spec columns present", msg


def _check_unique(
    frame: pd.DataFrame, check: dict, ctx: dict
) -> tuple[str, float | None, str, str]:
    cols = (
        check["columns"]
        if isinstance(check.get("columns"), list)
        else [check.get("column") or check.get("columns")]
    )
    missing = [c for c in cols if c not in frame.columns]
    if missing:
        return "fail", None, "0 duplicates", f"columns missing: {', '.join(missing)}"
    max_dups = int(check.get("max_duplicates", 0))
    dups = int(frame.duplicated(subset=cols).sum()) if len(frame) else 0
    return (
        ("pass" if dups <= max_dups else "fail"),
        float(dups),
        f"<= {max_dups} duplicates",
        f"{dups} duplicate rows on ({', '.join(cols)})",
    )


def _check_freshness(
    frame: pd.DataFrame, check: dict, ctx: dict
) -> tuple[str, float | None, str, str]:
    ts_col = check.get("ts_column", "ts")
    max_age_h = float(check["max_age_hours"])
    if ts_col not in frame.columns:
        return "fail", None, f"<= {max_age_h} h", f"column {ts_col!r} missing"
    if len(frame) == 0:
        return "fail", None, f"<= {max_age_h} h", "no rows"
    latest = _ts_series(frame, ts_col).max()
    now = ctx.get("now") or datetime.now(UTC)
    age_h = (pd.Timestamp(now) - latest).total_seconds() / 3600.0
    return (
        ("pass" if age_h <= max_age_h else "fail"),
        float(age_h),
        f"<= {max_age_h} h",
        f"latest {ts_col} is {age_h:.2f} h old",
    )


def _check_sql(
    frame: pd.DataFrame | None, check: dict, ctx: dict
) -> tuple[str, float | None, str, str]:
    adapter: TableAdapter = ctx["adapter"]
    query = check["query"].replace("{table}", adapter.resolve_table(ctx["table_name"]))
    partition_value = ctx.get("partition_value")
    query = query.replace("{partition}", partition_value or "")
    op_name = check.get("op", "<=")
    if op_name not in _OPS:
        return "error", None, "", f"unknown op {op_name!r}"
    threshold = float(check["threshold"])
    observed = adapter.sql(query)
    if observed is None:
        return "fail", None, f"{op_name} {threshold}", "query returned no numeric result"
    ok = _OPS[op_name](observed, threshold)
    return (
        ("pass" if ok else "fail"),
        float(observed),
        f"{op_name} {threshold}",
        f"query result {observed}",
    )


_CHECKS: dict[str, Callable[..., tuple[str, float | None, str, str]]] = {
    "not_null": _check_not_null,
    "row_count": _check_row_count,
    "value_range": _check_value_range,
    "sample_rate": _check_sample_rate,
    "schema_match": _check_schema_match,
    "unique": _check_unique,
    "freshness": _check_freshness,
    "sql": _check_sql,
}
_NEEDS_FRAME = {"not_null", "row_count", "value_range", "sample_rate", "unique", "freshness"}


def run_suite(
    suite: dict,
    adapter: TableAdapter,
    partition_value: str | None = None,
    run_id: str | None = None,
    now: datetime | None = None,
) -> list[CheckResult]:
    """Run every check in ``suite`` against ``adapter``. Never raises for data problems."""
    suite_name = str(suite.get("suite", "suite"))
    results: list[CheckResult] = []
    for table_spec in suite.get("tables") or []:
        table = table_spec["table"]
        partition_column = table_spec.get("partition_column")
        checks = table_spec.get("checks") or []
        if not checks:
            continue
        frame: pd.DataFrame | None = None
        frame_error: str | None = None
        if any(c["type"] in _NEEDS_FRAME for c in checks):
            try:
                frame = adapter.frame(table, partition_column, partition_value)
            except KeyError as exc:
                frame_error = str(exc)
            except Exception as exc:  # noqa: BLE001 - adapter failures must not abort the suite
                frame_error = f"{type(exc).__name__}: {exc}"
                log.exception("failed to load %s", table)
        ctx = {
            "adapter": adapter,
            "table_name": table,
            "partition_value": partition_value,
            "now": now,
        }
        for check in checks:
            ctype = check["type"]
            name = check.get("name", ctype)
            severity = check.get("severity", "warning")
            impl = _CHECKS.get(ctype)
            if impl is None:
                results.append(
                    CheckResult(
                        suite_name,
                        table,
                        name,
                        severity,
                        "error",
                        None,
                        "",
                        partition_value,
                        f"unknown check type {ctype!r}",
                    )
                )
                continue
            if ctype in _NEEDS_FRAME and frame is None:
                status = "skipped" if table_spec.get("optional") else "error"
                results.append(
                    CheckResult(
                        suite_name,
                        table,
                        name,
                        severity,
                        status,
                        None,
                        "",
                        partition_value,
                        f"table unavailable: {frame_error}",
                    )
                )
                continue
            try:
                status, observed, threshold, message = impl(frame, check, ctx)
            except KeyError as exc:
                status, observed, threshold, message = (
                    "error",
                    None,
                    "",
                    f"table/column unavailable: {exc}",
                )
            except Exception as exc:  # noqa: BLE001
                log.exception("check %s.%s raised", table, name)
                status, observed, threshold, message = (
                    "error",
                    None,
                    "",
                    f"{type(exc).__name__}: {exc}",
                )
            results.append(
                CheckResult(
                    suite_name,
                    table,
                    name,
                    severity,
                    status,
                    observed,
                    threshold,
                    partition_value,
                    message,
                )
            )
    log.info(
        "suite %s: %d checks, %d failed, %d errors",
        suite_name,
        len(results),
        sum(r.status == "fail" for r in results),
        sum(r.status == "error" for r in results),
    )
    return results


def run_suites(
    suite_paths: Iterable[Path | str],
    adapter: TableAdapter,
    partition_value: str | None = None,
    run_id: str | None = None,
) -> tuple[str, list[CheckResult]]:
    """Convenience: load and run several suite files with a shared run id."""
    run_id = run_id or new_run_id()
    results: list[CheckResult] = []
    for p in suite_paths:
        results.extend(run_suite(load_suite(p), adapter, partition_value, run_id))
    return run_id, results


def new_run_id() -> str:
    return uuid.uuid4().hex[:12]


def has_blocking_failures(results: Iterable[CheckResult]) -> bool:
    return any(r.severity == "blocking" and r.status in ("fail", "error") for r in results)


def to_frame(
    results: list[CheckResult], run_id: str, run_ts: datetime | pd.Timestamp | None = None
) -> pd.DataFrame:
    """``dq.check_results`` rows (spec §3.2) for a run."""
    run_ts = pd.Timestamp(run_ts or datetime.now(UTC))
    if run_ts.tzinfo is None:
        run_ts = run_ts.tz_localize("UTC")
    rows = []
    for r in results:
        d: dict[str, Any] = asdict(r)
        d["run_id"] = run_id
        d["run_ts"] = run_ts
        rows.append(d)
    df = pd.DataFrame(rows, columns=list(RESULT_COLUMNS))
    df["observed"] = pd.to_numeric(df["observed"], errors="coerce").astype("float64")
    df["run_ts"] = pd.to_datetime(df["run_ts"], utc=True)
    return df


def summary(results: Iterable[CheckResult]) -> str:
    """One line per non-passing check, for logs and CLI output."""
    lines = []
    for r in results:
        if r.status != "pass":
            lines.append(
                f"[{r.status.upper():7}] {r.severity:8} {r.table_name}.{r.check_name}: {r.message}"
            )
    return "\n".join(lines)
