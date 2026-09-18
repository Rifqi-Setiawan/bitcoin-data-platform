"""Incident report generation in structured JSON and publication-ready Markdown formats."""

from __future__ import annotations

import os
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path

from bitcoin_data_platform.diagnostics.models import (
    IncidentRecord,
    IncidentReport,
    RemediationPlan,
    RemediationResult,
    TelemetryBundle,
)


class IncidentReportGenerator:
    """Generates structured JSON and human-readable Markdown incident post-mortem reports."""

    def __init__(
        self,
        telemetry: TelemetryBundle,
        incidents: Sequence[IncidentRecord],
        remediation_plans: Sequence[RemediationPlan],
        remediation_results: Sequence[RemediationResult],
        *,
        target_run_id: str | None = None,
        now_utc: datetime | None = None,
    ) -> None:
        self.telemetry = telemetry
        self.incidents = list(incidents)
        self.remediation_plans = list(remediation_plans)
        self.remediation_results = list(remediation_results)
        self.target_run_id = target_run_id
        self.now_utc = (
            now_utc.astimezone(UTC)
            if now_utc and now_utc.tzinfo
            else (now_utc.replace(tzinfo=UTC) if now_utc else datetime.now(UTC))
        )

    def generate(self) -> IncidentReport:
        """Compile comprehensive IncidentReport object."""
        telemetry_summary = {
            "watermark_utc": self.telemetry.watermark_utc,
            "watermark_age_hours": self.telemetry.watermark_age_hours,
            "is_locked": self.telemetry.is_locked,
            "running_lock": self.telemetry.running_lock,
            "disk": self.telemetry.disk,
            "gaps": self.telemetry.gaps,
            "last_run": self.telemetry.last_run,
            "quality_checks_count": len(self.telemetry.quality_checks),
            "lakehouse_tables_count": len(self.telemetry.lakehouse_tables),
        }

        return IncidentReport(
            incidents=self.incidents,
            remediation_plans=self.remediation_plans,
            remediation_results=self.remediation_results,
            telemetry_summary=telemetry_summary,
            executed_queries=list(self.telemetry.executed_queries),
            target_run_id=self.target_run_id,
            generated_at_utc=self.now_utc,
        )

    @staticmethod
    def write_report(
        report: IncidentReport,
        output_path: Path | str,
        fmt: str = "json",
    ) -> Path:
        """Atomically persist incident report to filesystem in JSON or Markdown format."""
        out = Path(output_path).resolve()
        norm_fmt = fmt.lower()

        ext = ".json" if norm_fmt == "json" else ".md"

        # If path is an existing directory or ends with a slash, create default filename
        if out.is_dir() or str(output_path).endswith(("/", "\\")):
            out.mkdir(parents=True, exist_ok=True)
            timestamp_slug = report.generated_at_utc.strftime("%Y%m%d_%H%M%S")
            target_file = out / f"incident_report_{timestamp_slug}_{report.report_id[:8]}{ext}"
        else:
            out.parent.mkdir(parents=True, exist_ok=True)
            target_file = out

        content = report.to_json() if norm_fmt == "json" else report.to_markdown()

        # Atomic file write using .tmp and os.replace
        tmp_file = target_file.with_suffix(f"{target_file.suffix}.tmp")
        with open(tmp_file, "w", encoding="utf-8") as f:
            f.write(content)
            if not content.endswith("\n"):
                f.write("\n")

        os.replace(tmp_file, target_file)
        return target_file
