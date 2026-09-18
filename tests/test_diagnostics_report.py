"""Unit tests for incident report generation (JSON, Markdown, and persistence)."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from bitcoin_data_platform.diagnostics.models import (
    IncidentRecord,
    RemediationPlan,
    RemediationResult,
    TelemetryBundle,
)
from bitcoin_data_platform.diagnostics.report import IncidentReportGenerator


def test_report_generation_healthy_json() -> None:
    """1. Report generator produces valid JSON for healthy system."""
    telemetry = TelemetryBundle(
        watermark_utc="2026-01-15T12:00:00Z",
        watermark_age_hours=0.5,
        is_locked=False,
        disk={"percent_used": 35.0, "free_gb": 50.0, "total_gb": 80.0},
        gaps=[],
        executed_queries=["SELECT 1;"],
    )
    generator = IncidentReportGenerator(
        telemetry=telemetry,
        incidents=[],
        remediation_plans=[],
        remediation_results=[],
        now_utc=datetime(2026, 1, 15, 12, 30, tzinfo=UTC),
    )
    report = generator.generate()

    assert report.incidents == []
    json_str = report.to_json()
    data = json.loads(json_str)

    assert data["overall_status"] == "HEALTHY"
    assert data["incident_count"] == 0
    assert data["telemetry_summary"]["watermark_utc"] == "2026-01-15T12:00:00Z"
    assert len(data["executed_queries"]) == 1


def test_report_generation_degraded_markdown() -> None:
    """2. Report generator renders Markdown with RCA, evidence, and query audit."""
    telemetry = TelemetryBundle(
        watermark_utc="2026-01-15T08:00:00Z",
        watermark_age_hours=4.0,
        is_locked=True,
        disk={"percent_used": 85.0, "free_gb": 15.0, "total_gb": 100.0},
        gaps=[
            {
                "start_utc": "2026-01-15T09:00:00Z",
                "end_utc": "2026-01-15T11:00:00Z",
                "missing_hours": 2,
            }
        ],
        executed_queries=["SELECT * FROM run_metadata WHERE status = 'RUNNING';"],
    )
    incident = IncidentRecord(
        incident_code="INC-04",
        title="Stale Concurrency Run Lock",
        severity="CRITICAL",
        root_cause="Run lock held for 4.0 hours exceeding stale threshold.",
        recommended_action="ACTION_CLEAR_STALE_LOCK",
        affected_scope="concurrency:pipeline_lock",
        runbook_ref="docs/runbooks/OPERATIONAL_RUNBOOK.md#concurrency-locks-exit-code-6",
        evidence={"run_id": "stale-1", "duration_hours": 4.0},
    )
    plan = RemediationPlan(
        incident_id=incident.incident_id,
        action_type="ACTION_CLEAR_STALE_LOCK",
        description="Clear stale lock for run stale-1",
        dry_run=True,
    )
    result = RemediationResult(
        action_type="ACTION_CLEAR_STALE_LOCK",
        success=True,
        message="[DRY-RUN] Would clear stale lock",
    )

    generator = IncidentReportGenerator(
        telemetry=telemetry,
        incidents=[incident],
        remediation_plans=[plan],
        remediation_results=[result],
        target_run_id="stale-1",
        now_utc=datetime(2026, 1, 15, 12, 0, tzinfo=UTC),
    )
    report = generator.generate()

    md = report.to_markdown()

    assert "# Operational Diagnostics & Incident Report" in md
    assert "- **System Health Status**: **DEGRADED**" in md
    assert "- **Target Run ID**: `stale-1`" in md
    assert "INC-04" in md
    assert "Stale Concurrency Run Lock" in md
    assert "## 2. Root Cause Analysis (RCA)" in md
    assert "docs/runbooks/OPERATIONAL_RUNBOOK.md#concurrency-locks-exit-code-6" in md
    assert "## 3. Telemetry Snapshot" in md
    assert "## 4. Remediation Plans & Healing Actions" in md
    assert "[DRY-RUN] ACTION_CLEAR_STALE_LOCK" in md
    assert "## 5. Executed Query Audit Trail" in md
    assert "SELECT * FROM run_metadata" in md


def test_report_persistence_file_and_directory(tmp_path: Path) -> None:
    """3. IncidentReportGenerator.write_report writes atomically to file and dir."""
    telemetry = TelemetryBundle()
    generator = IncidentReportGenerator(
        telemetry=telemetry,
        incidents=[],
        remediation_plans=[],
        remediation_results=[],
        now_utc=datetime(2026, 1, 15, 12, 0, tzinfo=UTC),
    )
    report = generator.generate()

    # 1. Write to explicit JSON file path
    target_json = tmp_path / "reports" / "my_report.json"
    written_json = IncidentReportGenerator.write_report(report, target_json, fmt="json")
    assert written_json.exists()
    assert written_json == target_json
    content_json = json.loads(written_json.read_text(encoding="utf-8"))
    assert content_json["overall_status"] == "HEALTHY"

    # 2. Write to explicit Markdown file path
    target_md = tmp_path / "reports" / "my_report.md"
    written_md = IncidentReportGenerator.write_report(report, target_md, fmt="markdown")
    assert written_md.exists()
    assert "Operational Diagnostics & Incident Report" in written_md.read_text(encoding="utf-8")

    # 3. Write to directory (creates default timestamped file)
    report_dir = tmp_path / "reports_dir"
    report_dir.mkdir(parents=True, exist_ok=True)
    written_auto = IncidentReportGenerator.write_report(report, report_dir, fmt="json")
    assert written_auto.exists()
    assert written_auto.parent == report_dir
    assert written_auto.name.startswith("incident_report_")
    assert written_auto.suffix == ".json"


def test_report_empty_incidents_markdown_output() -> None:
    """4. Empty incident list produces clean Healthy markdown."""
    telemetry = TelemetryBundle(executed_queries=[])
    generator = IncidentReportGenerator(
        telemetry=telemetry,
        incidents=[],
        remediation_plans=[],
        remediation_results=[],
        now_utc=datetime(2026, 1, 15, 12, 0, tzinfo=UTC),
    )
    report = generator.generate()
    md = report.to_markdown()

    assert "**HEALTHY**" in md
    assert "No active incidents detected" in md
    assert "No SQL queries were executed during this investigation." in md


def test_report_to_dict_keys() -> None:
    """5. Report to_dict contains all required audit and lineage keys."""
    telemetry = TelemetryBundle()
    generator = IncidentReportGenerator(
        telemetry=telemetry,
        incidents=[],
        remediation_plans=[],
        remediation_results=[],
        now_utc=datetime(2026, 1, 15, 12, 0, tzinfo=UTC),
    )
    report = generator.generate()
    d = report.to_dict()

    expected_keys = {
        "report_id",
        "generated_at_utc",
        "target_run_id",
        "overall_status",
        "incident_count",
        "incidents",
        "remediation_plans",
        "remediation_results",
        "telemetry_summary",
        "executed_queries",
    }
    assert expected_keys.issubset(set(d.keys()))
