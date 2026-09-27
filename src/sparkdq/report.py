"""Report objects: per-check results plus markdown/JSON rendering."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import List, Optional


@dataclass
class CheckResult:
    check_name: str
    check_type: str
    column: Optional[str]
    severity: str          # "error" | "warning"
    status: str            # "pass" | "fail" | "error"
    total_rows: int
    violating_rows: Optional[int]
    violating_pct: Optional[float]
    sample: List[dict]
    details: str
    description: str = ""


@dataclass
class DQReport:
    table_name: str
    results: List[CheckResult] = field(default_factory=list)
    run_at: str = ""

    # ------------------------------------------------------------------
    @property
    def passed(self) -> int:
        return sum(1 for r in self.results if r.status == "pass")

    @property
    def failed(self) -> int:
        return sum(1 for r in self.results if r.status == "fail" and r.severity == "error")

    @property
    def warnings(self) -> int:
        return sum(1 for r in self.results if r.status == "fail" and r.severity == "warning")

    @property
    def overall_status(self) -> str:
        if any(r.status == "error" for r in self.results):
            return "ERROR"
        return "FAIL" if self.failed else "PASS"

    # ------------------------------------------------------------------
    def to_dict(self) -> dict:
        return {"table": self.table_name, "run_at": self.run_at,
                "overall_status": self.overall_status,
                "summary": {"passed": self.passed, "failed": self.failed,
                            "warnings": self.warnings,
                            "total": len(self.results)},
                "results": [asdict(r) for r in self.results]}

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, default=str)

    def save_json(self, path: str) -> None:
        with open(path, "w") as f:
            f.write(self.to_json())

    # ------------------------------------------------------------------
    def to_markdown(self) -> str:
        icon = {"pass": "✅", "fail": "❌", "error": "⚠️"}
        lines = [
            f"# Data Quality Report — `{self.table_name}`",
            "",
            f"**Overall: {self.overall_status}** · "
            f"{self.passed} passed · {self.failed} failed · "
            f"{self.warnings} warning(s) · run at {self.run_at}",
            "",
            "| Check | Column | Severity | Status | Violations | Details |",
            "|---|---|---|---|---|---|",
        ]
        for r in self.results:
            viol = "-" if r.violating_rows is None else (
                f"{r.violating_rows} ({r.violating_pct}%)"
                if r.violating_pct is not None else str(r.violating_rows))
            lines.append(
                f"| {r.check_name} | {r.column or '-'} | {r.severity} | "
                f"{icon.get(r.status, '?')} {r.status} | {viol} | {r.details} |")
        failing = [r for r in self.results if r.status != "pass" and r.sample]
        if failing:
            lines += ["", "## Sample violating rows"]
            for r in failing:
                lines.append(f"### {r.check_name}")
                lines.append("```json")
                lines.append(json.dumps(r.sample, indent=2, default=str))
                lines.append("```")
        return "\n".join(lines) + "\n"

    def save_markdown(self, path: str) -> None:
        with open(path, "w") as f:
            f.write(self.to_markdown())
