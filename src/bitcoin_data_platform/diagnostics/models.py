"""Data models for operational diagnostics, incident triage, and bounded self-healing."""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

SEVERITY_ORDER = {
    "CRITICAL": 0,
    "WARNING": 1,
    "INFO": 2,
}


@dataclass
class IncidentRecord:
    """Record of an active or diagnosed operational incident."""

    incident_code: str  # INC-01 through INC-06
    title: str
    severity: str  # CRITICAL, WARNING, INFO
    root_cause: str
    recommended_action: str
    affected_scope: str
    runbook_ref: str
    incident_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    status: str = "ACTIVE"
    evidence: dict[str, Any] = field(default_factory=dict)
    detected_at_utc: datetime = field(default_factory=lambda: datetime.now(UTC))

    def to_dict(self) -> dict[str, Any]:
        """Serialize incident record to JSON-compatible dictionary."""
        return {
            "incident_id": self.incident_id,
            "incident_code": self.incident_code,
            "title": self.title,
            "severity": self.severity,
            "status": self.status,
            "root_cause": self.root_cause,
            "evidence": self.evidence,
            "recommended_action": self.recommended_action,
            "affected_scope": self.affected_scope,
            "runbook_ref": self.runbook_ref,
            "detected_at_utc": self.detected_at_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }


@dataclass
class TelemetryBundle:
    """Container for out-of-band telemetry collected across storage, catalog, and logs."""

    last_run: dict[str, Any] | None = None
    recent_runs: list[dict[str, Any]] = field(default_factory=list)
    quality_checks: list[dict[str, Any]] = field(default_factory=list)
    watermark_utc: str | None = None
    watermark_age_hours: float | None = None
    gaps: list[dict[str, Any]] = field(default_factory=list)
    is_locked: bool = False
    running_lock: dict[str, Any] | None = None
    lakehouse_tables: dict[str, Any] = field(default_factory=dict)
    disk: dict[str, Any] = field(default_factory=dict)
    alert_state: dict[str, Any] = field(default_factory=dict)
    executed_queries: list[str] = field(default_factory=list)
    collected_at_utc: datetime = field(default_factory=lambda: datetime.now(UTC))

    def to_dict(self) -> dict[str, Any]:
        """Serialize telemetry bundle to JSON-compatible dictionary."""
        return {
            "last_run": self.last_run,
            "recent_runs": self.recent_runs,
            "quality_checks": self.quality_checks,
            "watermark_utc": self.watermark_utc,
            "watermark_age_hours": self.watermark_age_hours,
            "gaps": self.gaps,
            "is_locked": self.is_locked,
            "running_lock": self.running_lock,
            "lakehouse_tables": self.lakehouse_tables,
            "disk": self.disk,
            "alert_state": self.alert_state,
            "executed_queries": list(self.executed_queries),
            "collected_at_utc": self.collected_at_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }


@dataclass
class RemediationPlan:
    """Bounded, deterministic plan for self-healing or operator remediation."""

    incident_id: str
    action_type: str  # ACTION_CLEAR_STALE_LOCK, ACTION_BACKFILL_GAPS, etc.
    description: str
    plan_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    parameters: dict[str, Any] = field(default_factory=dict)
    safety_checks: list[str] = field(default_factory=list)
    dry_run: bool = True

    def to_dict(self) -> dict[str, Any]:
        """Serialize remediation plan to JSON-compatible dictionary."""
        return {
            "plan_id": self.plan_id,
            "incident_id": self.incident_id,
            "action_type": self.action_type,
            "description": self.description,
            "parameters": self.parameters,
            "safety_checks": self.safety_checks,
            "dry_run": self.dry_run,
        }


@dataclass
class RemediationResult:
    """Execution outcome of an automated or simulated remediation action."""

    action_type: str
    success: bool
    message: str
    remediated_at_utc: datetime = field(default_factory=lambda: datetime.now(UTC))
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Serialize remediation result to JSON-compatible dictionary."""
        return {
            "action_type": self.action_type,
            "success": self.success,
            "message": self.message,
            "remediated_at_utc": self.remediated_at_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "details": self.details,
        }


@dataclass
class IncidentReport:
    """Comprehensive incident report documenting root causes, evidence, and audit queries."""

    incidents: list[IncidentRecord] = field(default_factory=list)
    remediation_plans: list[RemediationPlan] = field(default_factory=list)
    remediation_results: list[RemediationResult] = field(default_factory=list)
    telemetry_summary: dict[str, Any] = field(default_factory=dict)
    executed_queries: list[str] = field(default_factory=list)
    target_run_id: str | None = None
    report_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    generated_at_utc: datetime = field(default_factory=lambda: datetime.now(UTC))

    def to_dict(self) -> dict[str, Any]:
        """Serialize full incident report to dictionary."""
        return {
            "report_id": self.report_id,
            "generated_at_utc": self.generated_at_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "target_run_id": self.target_run_id,
            "overall_status": "DEGRADED" if self.incidents else "HEALTHY",
            "incident_count": len(self.incidents),
            "incidents": [inc.to_dict() for inc in self.incidents],
            "remediation_plans": [plan.to_dict() for plan in self.remediation_plans],
            "remediation_results": [res.to_dict() for res in self.remediation_results],
            "telemetry_summary": self.telemetry_summary,
            "executed_queries": self.executed_queries,
        }

    def to_json(self, indent: int = 2) -> str:
        """Serialize incident report to indented JSON string."""
        return json.dumps(self.to_dict(), indent=indent)

    def to_markdown(self) -> str:
        """Render incident report as publication-ready Markdown document."""
        gen_str = self.generated_at_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
        status_str = "DEGRADED" if self.incidents else "HEALTHY"
        lines: list[str] = [
            "# Operational Diagnostics & Incident Report",
            "",
            f"- **Report ID**: `{self.report_id}`",
            f"- **Generated At (UTC)**: `{gen_str}`",
            f"- **Target Run ID**: `{self.target_run_id or 'ALL'}`",
            f"- **System Health Status**: **{status_str}**",
            f"- **Total Active Incidents**: {len(self.incidents)}",
            "",
            "## 1. Active Incidents Overview",
            "",
        ]

        if not self.incidents:
            lines.append(
                "No active incidents detected. All system components are operating normally."
            )
            lines.append("")
        else:
            lines.append("| Code | Title | Severity | Status | Scope | Action | Runbook |")
            lines.append("| :--- | :--- | :--- | :--- | :--- | :--- | :--- |")
            for inc in self.incidents:
                lines.append(
                    f"| `{inc.incident_code}` | {inc.title} | `{inc.severity}` | "
                    f"`{inc.status}` | `{inc.affected_scope}` | {inc.recommended_action} | "
                    f"[{inc.runbook_ref}]({inc.runbook_ref}) |"
                )
            lines.append("")

            lines.append("## 2. Root Cause Analysis (RCA)")
            lines.append("")
            for idx, inc in enumerate(self.incidents, 1):
                lines.append(f"### 2.{idx} [{inc.incident_code}] {inc.title}")
                lines.append(f"- **Severity**: `{inc.severity}`")
                lines.append(f"- **Affected Scope**: `{inc.affected_scope}`")
                lines.append(f"- **Root Cause**: {inc.root_cause}")
                lines.append(f"- **Runbook Reference**: `{inc.runbook_ref}`")
                lines.append("- **Telemetry Evidence**:")
                lines.append("```json")
                lines.append(json.dumps(inc.evidence, indent=2))
                lines.append("```")
                lines.append("")

        lines.append("## 3. Telemetry Snapshot")
        lines.append("")
        wm_val = self.telemetry_summary.get("watermark_utc") or "None"
        lines.append(f"- **Pipeline Watermark**: `{wm_val}`")
        wm_age = self.telemetry_summary.get("watermark_age_hours")
        lag_str = f"`{wm_age} hours`" if wm_age is not None else "`N/A`"
        lines.append(f"- **Watermark Lag**: {lag_str}")
        lock_str = "LOCKED" if self.telemetry_summary.get("is_locked") else "UNLOCKED"
        lines.append(f"- **Pipeline Lock**: `{lock_str}`")
        disk_info = self.telemetry_summary.get("disk", {})
        lines.append(
            f"- **Disk Utilization**: `{disk_info.get('percent_used', 0)}%` "
            f"({disk_info.get('free_gb', 0)} GB free of {disk_info.get('total_gb', 0)} GB)"
        )
        gaps = self.telemetry_summary.get("gaps", [])
        lines.append(f"- **Hourly Gaps Count**: {len(gaps)}")
        lines.append("")

        lines.append("## 4. Remediation Plans & Healing Actions")
        lines.append("")
        if not self.remediation_plans:
            lines.append("No remediation actions planned.")
            lines.append("")
        else:
            for plan in self.remediation_plans:
                mode_str = "[DRY-RUN]" if plan.dry_run else "[AUTO-HEAL]"
                lines.append(
                    f"- **{mode_str} {plan.action_type}** ({plan.incident_id}): {plan.description}"
                )
                if plan.safety_checks:
                    lines.append(f"  - Safety checks: {', '.join(plan.safety_checks)}")
            lines.append("")

        if self.remediation_results:
            lines.append("### Execution Outcomes")
            lines.append("")
            for res in self.remediation_results:
                status_icon = "SUCCESS" if res.success else "FAILED"
                lines.append(f"- `[{status_icon}]` **{res.action_type}**: {res.message}")
            lines.append("")

        lines.append("## 5. Executed Query Audit Trail")
        lines.append("")
        if not self.executed_queries:
            lines.append("No SQL queries were executed during this investigation.")
        else:
            lines.append("Read-only telemetry queries executed during diagnosis:")
            lines.append("```sql")
            for q in self.executed_queries:
                lines.append(f"-- Timestamp: {gen_str}")
                lines.append(q.strip())
                lines.append("")
            lines.append("```")
        lines.append("")

        return "\n".join(lines)
