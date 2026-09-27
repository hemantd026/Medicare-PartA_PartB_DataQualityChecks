"""Execution engine: runs a list of Checks against a Spark DataFrame."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from .checks import Check
from .report import CheckResult, DQReport


@dataclass
class _FailedCheck(Exception):
    """Internal: raised when a check cannot be evaluated."""


class DQEngine:
    """Runs :class:`~sparkdq.checks.Check` objects against Spark DataFrames.

    Example
    -------
    >>> engine = DQEngine(spark)
    >>> report = engine.run(claims_df, checks=[not_null("CLM_ID"), unique("CLM_ID")],
    ...                       table_name="inpatient")
    >>> print(report.to_markdown())
    """

    def __init__(self, spark: SparkSession, sample_size: int = 5):
        self.spark = spark
        self.sample_size = sample_size

    # ------------------------------------------------------------------
    def run(self, df: DataFrame, checks: List[Check],
            table_name: str = "table",
            ref_tables: Optional[Dict[str, DataFrame]] = None) -> DQReport:
        """Execute ``checks`` against ``df`` and return a :class:`DQReport`.

        ``ref_tables`` maps a table name -> DataFrame for
        ``referential_integrity`` checks (keyed by the check's ``params["ref_table"]``,
        defaulting to ``"parent"``).
        """
        ref_tables = ref_tables or {}
        total_rows = df.count()
        results: List[CheckResult] = []
        for check in checks:
            try:
                results.append(self._evaluate(df, check, total_rows, ref_tables))
            except Exception as exc:  # a broken check is itself a finding
                results.append(CheckResult(
                    check_name=check.name, check_type=check.check_type,
                    column=check.column, severity=check.severity,
                    status="error", total_rows=total_rows,
                    violating_rows=None, violating_pct=None, sample=[],
                    details=f"Check could not be evaluated: {exc}",
                    description=check.description))
        return DQReport(table_name=table_name, results=results,
                        run_at=datetime.now(timezone.utc).isoformat())

    # ------------------------------------------------------------------
    def _sample(self, df: DataFrame, violation) -> List[dict]:
        rows = df.filter(violation).limit(self.sample_size).collect()
        return [r.asDict(recursive=True) for r in rows]

    def _result(self, check: Check, total_rows: int, violating_rows: int,
                violation, df: DataFrame, details: str = "") -> CheckResult:
        status = "pass" if violating_rows == 0 else "fail"
        pct = (violating_rows / total_rows * 100) if total_rows else 0.0
        sample = self._sample(df, violation) if violating_rows else []
        return CheckResult(
            check_name=check.name, check_type=check.check_type,
            column=check.column, severity=check.severity, status=status,
            total_rows=total_rows, violating_rows=violating_rows,
            violating_pct=round(pct, 2), sample=sample,
            details=details or f"{violating_rows} violating row(s)",
            description=check.description)

    # ------------------------------------------------------------------
    def _evaluate(self, df: DataFrame, check: Check, total_rows: int,
                  ref_tables: Dict[str, DataFrame]) -> CheckResult:
        c, p = check.column, check.params
        t = check.check_type

        if t == "row_count":
            viol = max(0, p["min_rows"] - total_rows)
            return self._result(check, total_rows, 1 if viol else 0,
                                F.lit(False), df,
                                f"Found {total_rows} row(s), minimum required: {p['min_rows']}")

        if t == "schema_match":
            expected: Dict[str, str] = p["expected"]
            actual = {f.name: f.dataType.simpleString() for f in df.schema.fields}
            problems = []
            for col, typ in expected.items():
                if col not in actual:
                    problems.append(f"missing column: {col}")
                elif actual[col] != typ:
                    problems.append(f"{col}: expected {typ}, found {actual[col]}")
            status = "pass" if not problems else "fail"
            return CheckResult(
                check_name=check.name, check_type=t, column=None,
                severity=check.severity, status=status, total_rows=total_rows,
                violating_rows=len(problems) if problems else 0,
                violating_pct=None, sample=[],
                details="Schema matches" if not problems else "; ".join(problems),
                description=check.description)

        if t == "not_null":
            violation = F.col(c).isNull()
            n = df.filter(violation).count()
            return self._result(check, total_rows, n, violation, df,
                                f"{n} NULL value(s) in {c}")

        if t == "unique":
            # Window-based duplicate detection: fully distributed, no
            # duplicate keys are ever collected to the driver. The helper
            # column is stripped from reported samples.
            dup_n = F.count(F.lit(1)).over(Window.partitionBy(c))
            counted = df.withColumn("__dq_dup_n", dup_n)
            violation = F.col(c).isNotNull() & (F.col("__dq_dup_n") > 1)
            n = counted.filter(violation).count()
            res = self._result(check, total_rows, n, violation, counted,
                               f"{n} row(s) with duplicated {c}")
            res.sample = [{k: v for k, v in row.items() if k != "__dq_dup_n"}
                          for row in res.sample]
            return res

        if t == "accepted_values":
            violation = F.col(c).isNotNull() & ~F.col(c).isin(p["values"])
            n = df.filter(violation).count()
            return self._result(check, total_rows, n, violation, df,
                                f"{n} row(s) with {c} outside {p['values']}")

        if t == "numeric_range":
            # try_cast: non-numeric strings become NULL (ignored, like NULLs)
            # instead of aborting the job under ANSI mode.
            num = F.expr(f"try_cast(`{c}` as double)")
            cond = F.lit(True)
            lo, hi = p.get("min_value"), p.get("max_value")
            if lo is not None:
                cond = cond & (num >= lo)
            if hi is not None:
                cond = cond & (num <= hi)
            violation = F.col(c).isNotNull() & num.isNotNull() & ~cond
            n = df.filter(violation).count()
            return self._result(check, total_rows, n, violation, df,
                                f"{n} row(s) with {c} outside [{lo}, {hi}]")

        if t == "regex_match":
            # NB: in Spark 4.x the regexp argument is a ColumnOrName, so a plain
            # Python string would be resolved as a column -> wrap it in lit().
            violation = F.col(c).isNotNull() & ~F.regexp_like(F.col(c), F.lit(p["pattern"]))
            n = df.filter(violation).count()
            return self._result(check, total_rows, n, violation, df,
                                f"{n} row(s) where {c} does not match /{p['pattern']}/")

        if t == "date_order":
            s, e = p["start_column"], p["end_column"]
            violation = (F.col(s).isNotNull() & F.col(e).isNotNull()
                         & (F.col(s) > F.col(e)))
            n = df.filter(violation).count()
            return self._result(check, total_rows, n, violation, df,
                                f"{n} row(s) where {s} > {e}")

        if t == "referential_integrity":
            parent = ref_tables.get(p.get("ref_table", "parent"))
            if parent is None:
                raise _FailedCheck("No parent DataFrame supplied in ref_tables")
            parent_col = p["parent_column"]
            # left_anti join: orphans stay distributed, parent keys are never
            # collected to the driver.
            parent_keys = parent.select(parent_col).distinct()
            orphans = df.join(parent_keys, df[c] == parent_keys[parent_col],
                              "left_anti")
            n = orphans.count()
            status = "pass" if n == 0 else "fail"
            pct = (n / total_rows * 100) if total_rows else 0.0
            sample = ([r.asDict(recursive=True)
                       for r in orphans.limit(self.sample_size).collect()]
                      if n else [])
            return CheckResult(
                check_name=check.name, check_type=check.check_type,
                column=check.column, severity=check.severity, status=status,
                total_rows=total_rows, violating_rows=n,
                violating_pct=round(pct, 2), sample=sample,
                details=f"{n} orphan row(s): {c} not found in parent.{parent_col}",
                description=check.description)

        if t == "custom_sql":
            violation = F.expr(f"NOT ({p['condition']})")
            # also surface rows where the condition evaluates to NULL
            violation = violation | F.expr(f"({p['condition']}) IS NULL")
            n = df.filter(violation).count()
            return self._result(check, total_rows, n, violation, df,
                                f"{n} row(s) violating: {p['condition']}")

        raise _FailedCheck(f"Unknown check_type: {t}")
