"""Check definitions for sparkdq.

A :class:`Check` is a small, declarative description of one data-quality
rule.  The :class:`~sparkdq.engine.DQEngine` knows how to execute each
``check_type`` against a Spark DataFrame.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class Check:
    """A single declarative data-quality rule."""

    name: str
    check_type: str
    column: Optional[str] = None
    params: Dict[str, Any] = field(default_factory=dict)
    severity: str = "error"  # "error" | "warning"
    description: str = ""

    def __post_init__(self) -> None:
        if self.severity not in ("error", "warning"):
            raise ValueError("severity must be 'error' or 'warning'")


# ---------------------------------------------------------------------------
# Builder helpers
# ---------------------------------------------------------------------------

def row_count(min_rows: int = 1, name: str = "row_count",
              severity: str = "error", description: str = "") -> Check:
    """Fail if the table has fewer than ``min_rows`` rows."""
    return Check(name=name, check_type="row_count",
                 params={"min_rows": min_rows},
                 severity=severity,
                 description=description or f"Table must contain at least {min_rows} row(s)")


def not_null(column: str, name: Optional[str] = None,
             severity: str = "error", description: str = "") -> Check:
    """Fail if ``column`` contains NULL values."""
    return Check(name=name or f"{column}_not_null", check_type="not_null",
                 column=column, severity=severity,
                 description=description or f"{column} must not contain NULLs")


def unique(column: str, name: Optional[str] = None,
           severity: str = "error", description: str = "") -> Check:
    """Fail if ``column`` contains duplicate values."""
    return Check(name=name or f"{column}_unique", check_type="unique",
                 column=column, severity=severity,
                 description=description or f"{column} must contain unique values")


def accepted_values(column: str, values: List[Any], name: Optional[str] = None,
                    severity: str = "error", description: str = "") -> Check:
    """Fail if ``column`` contains values outside ``values`` (NULLs are ignored)."""
    return Check(name=name or f"{column}_accepted_values",
                 check_type="accepted_values", column=column,
                 params={"values": list(values)}, severity=severity,
                 description=description or f"{column} must be one of {sorted(map(str, values))}")


def numeric_range(column: str, min_value: Optional[float] = None,
                  max_value: Optional[float] = None, name: Optional[str] = None,
                  severity: str = "error", description: str = "") -> Check:
    """Fail if ``column`` has values outside [``min_value``, ``max_value``]."""
    return Check(name=name or f"{column}_in_range", check_type="numeric_range",
                 column=column,
                 params={"min_value": min_value, "max_value": max_value},
                 severity=severity,
                 description=description or f"{column} must be within [{min_value}, {max_value}]")


def regex_match(column: str, pattern: str, name: Optional[str] = None,
                severity: str = "error", description: str = "") -> Check:
    """Fail if ``column`` has values not matching the regex ``pattern`` (NULLs ignored)."""
    return Check(name=name or f"{column}_regex", check_type="regex_match",
                 column=column, params={"pattern": pattern}, severity=severity,
                 description=description or f"{column} must match /{pattern}/")


def date_order(start_column: str, end_column: str, name: Optional[str] = None,
               severity: str = "error", description: str = "") -> Check:
    """Fail if ``start_column`` > ``end_column`` on any row (NULLs ignored)."""
    return Check(name=name or f"{start_column}_before_{end_column}",
                 check_type="date_order",
                 params={"start_column": start_column, "end_column": end_column},
                 severity=severity,
                 description=description or f"{start_column} must be on or before {end_column}")


def referential_integrity(column: str, parent_column: str,
                          name: Optional[str] = None,
                          severity: str = "error", description: str = "") -> Check:
    """Fail if ``column`` has values missing from the parent table's ``parent_column``.

    The parent DataFrame itself is supplied at run time via
    ``DQEngine.run(..., ref_tables={...})``.
    """
    return Check(name=name or f"{column}_ref_integrity",
                 check_type="referential_integrity", column=column,
                 params={"parent_column": parent_column}, severity=severity,
                 description=description or f"Every {column} must exist in the parent table")


def schema_match(expected: Dict[str, str], name: str = "schema_match",
                 severity: str = "error", description: str = "") -> Check:
    """Fail if the DataFrame's columns/types differ from ``expected``.

    ``expected`` maps column name -> Spark simple type name
    (e.g. ``{"CLM_ID": "string", "CLM_PMT_AMT": "double"}``).
    """
    return Check(name=name, check_type="schema_match",
                 params={"expected": dict(expected)}, severity=severity,
                 description=description or "DataFrame schema must match the expected schema")


def custom_sql(name: str, condition: str, description: str = "",
               severity: str = "error") -> Check:
    """Fail on rows where the SQL boolean ``condition`` is not true.

    Example: ``custom_sql("non_negative_payment", "CLM_PMT_AMT >= 0")``.
    """
    return Check(name=name, check_type="custom_sql",
                 params={"condition": condition}, severity=severity,
                 description=description or f"Rows must satisfy: {condition}")
