"""sparkdq: a small, declarative data-quality engine for PySpark."""

from .checks import (
    Check, accepted_values, custom_sql, date_order, not_null, numeric_range,
    referential_integrity, regex_match, row_count, schema_match, unique,
)
from .engine import DQEngine
from .report import CheckResult, DQReport

__all__ = [
    "Check", "DQEngine", "DQReport", "CheckResult",
    "accepted_values", "custom_sql", "date_order", "not_null",
    "numeric_range", "referential_integrity", "regex_match",
    "row_count", "schema_match", "unique",
]

__version__ = "0.1.0"
