"""Deterministic, rule-based operational incident triage engine."""

from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import Any

from bitcoin_data_platform.diagnostics.models import (
    SEVERITY_ORDER,
    IncidentRecord,
    RemediationPlan,
    TelemetryBundle,
)
from bitcoin_data_platform.time_range import parse_iso_utc

# Regex patterns for incident classifications
OUTAGE_PATTERNS = re.compile(
    r"(429|too many requests|rate limit|500|502|503|504|5xx|timeout|timed out|sourceunavailable)",
    re.IGNORECASE,
)
QUALITY_PATTERNS = re.compile(
    r"(qualitycheckerror|contractviolation|contract violation|"
    r"quality check violation|quality check failed)",
    re.IGNORECASE,
)
STORAGE_PATTERNS = re.compile(
    r"(no space left on device|disk full|storageerror|parquetstorageerror|storage failure)",
    re.IGNORECASE,
)
LOCK_PATTERNS = re.compile(
    r"(runlockerror|concurrent run detected|concurrent run lock|lock collision)",
    re.IGNORECASE,
)


class TriageEngine:
    """Classifies operational degradation across taxonomy INC-01..INC-06."""

    def __init__(
        self,
        telemetry: TelemetryBundle,
        *,
        target_run_id: str | None = None,
        now_utc: datetime | None = None,
    ) -> None:
        self.telemetry = telemetry
        self.target_run_id = target_run_id
        self.now_utc = (
            now_utc.astimezone(UTC)
            if now_utc and now_utc.tzinfo
            else (now_utc.replace(tzinfo=UTC) if now_utc else datetime.now(UTC))
        )

    def triage(self) -> tuple[list[IncidentRecord], list[RemediationPlan]]:
        """Run rule evaluation and return prioritized incidents and bound remediation plans."""
        incidents: list[IncidentRecord] = []
        plans: list[RemediationPlan] = []

        # 1. Evaluate INC-04: Concurrency Lock Collision / Stale Lock
        inc_04, plan_04 = self._check_inc_04_concurrency_lock()
        if inc_04:
            incidents.append(inc_04)
            if plan_04:
                plans.append(plan_04)

        # 2. Evaluate INC-01: Source Upstream Outage
        inc_01, plan_01 = self._check_inc_01_upstream_outage()
        if inc_01:
            incidents.append(inc_01)
            if plan_01:
                plans.append(plan_01)

        # 3. Evaluate INC-02: Data Quality Block Violation
        inc_02, plan_02 = self._check_inc_02_data_quality()
        if inc_02:
            incidents.append(inc_02)
            if plan_02:
                plans.append(plan_02)

        # 4. Evaluate INC-03: Storage & Promotion Failure
        inc_03, plan_03 = self._check_inc_03_storage_failure()
        if inc_03:
            incidents.append(inc_03)
            if plan_03:
                plans.append(plan_03)

        # 5. Evaluate INC-05: Freshness Lag / Missing Gaps
        inc_05, plan_05 = self._check_inc_05_freshness_gaps()
        if inc_05:
            incidents.append(inc_05)
            if plan_05:
                plans.append(plan_05)

        # 6. Evaluate INC-06: Lakehouse Small-File Bloat
        lakehouse_incidents, lakehouse_plans = self._check_inc_06_lakehouse_bloat()
        incidents.extend(lakehouse_incidents)
        plans.extend(lakehouse_plans)

        # Sort incidents by severity (CRITICAL > WARNING > INFO)
        incidents.sort(key=lambda x: (SEVERITY_ORDER.get(x.severity, 99), x.incident_code))

        # Re-order plans to match incident order
        inc_id_order = {inc.incident_id: idx for idx, inc in enumerate(incidents)}
        plans.sort(key=lambda p: inc_id_order.get(p.incident_id, 99))

        return incidents, plans

    def _get_relevant_runs(self) -> list[dict[str, Any]]:
        """Identify runs to inspect for triage."""
        runs: list[dict[str, Any]] = []
        if self.telemetry.last_run:
            runs.append(self.telemetry.last_run)
        for r in self.telemetry.recent_runs:
            if r not in runs:
                runs.append(r)
        return runs

    def _check_inc_01_upstream_outage(
        self,
    ) -> tuple[IncidentRecord | None, RemediationPlan | None]:
        """INC-01: Source Upstream Outage (Exit code 3, HTTP 429, 5xx, timeout)."""
        runs = self._get_relevant_runs()
        for run in runs:
            if str(run.get("status", "")).upper() in ("FAILED", "FAILURE"):
                err_msg = str(run.get("error_message") or "")
                err_class = str(run.get("error_class") or "")
                full_text = f"{err_msg} {err_class}"

                is_outage = (
                    "exit code: 3" in full_text
                    or "exit code 3" in full_text
                    or OUTAGE_PATTERNS.search(full_text) is not None
                )

                if is_outage:
                    r_id = run.get("run_id")
                    inc = IncidentRecord(
                        incident_code="INC-01",
                        title="Source Upstream Outage / Rate Limit Encountered",
                        severity="CRITICAL",
                        status="ACTIVE",
                        root_cause=(
                            f"Upstream provider outage in run {r_id}: {err_msg or err_class}"
                        ),
                        evidence={
                            "failed_run_id": r_id,
                            "error_message": err_msg,
                            "error_class": err_class,
                            "exit_code": 3,
                        },
                        recommended_action=(
                            "Enforce API cooldown; execute ACTION_BACKFILL_GAPS once recovered."
                        ),
                        affected_scope="sources:upstream_api",
                        runbook_ref="docs/runbooks/OPERATIONAL_RUNBOOK.md#recovery-from-api-outages",
                        detected_at_utc=self.now_utc,
                    )
                    plan = RemediationPlan(
                        incident_id=inc.incident_id,
                        action_type="ACTION_BACKFILL_GAPS",
                        description="Schedule bounded backfill after upstream API cooldown.",
                        parameters={"run_id": r_id, "cooldown_seconds": 300},
                        safety_checks=["verify_upstream_reachability", "verify_no_active_lock"],
                    )
                    return inc, plan

        return None, None

    def _check_inc_02_data_quality(
        self,
    ) -> tuple[IncidentRecord | None, RemediationPlan | None]:
        """INC-02: Data Quality Block Violation (Exit code 4, FAILED quality checks)."""
        # Check failed quality checks with BLOCK severity
        failed_blocks = [
            qc
            for qc in self.telemetry.quality_checks
            if str(qc.get("status", "")).upper() == "FAILED"
            and str(qc.get("severity", "")).upper() == "BLOCK"
        ]

        # Also inspect failed runs for exit code 4 or contract violations
        runs = self._get_relevant_runs()
        contract_violation_run = None
        for run in runs:
            if str(run.get("status", "")).upper() in ("FAILED", "FAILURE"):
                err_msg = str(run.get("error_message") or "")
                err_class = str(run.get("error_class") or "")
                full_text = f"{err_msg} {err_class}"
                if (
                    "exit code: 4" in full_text
                    or "exit code 4" in full_text
                    or QUALITY_PATTERNS.search(full_text) is not None
                ):
                    contract_violation_run = run
                    break

        if failed_blocks or contract_violation_run:
            evidence: dict[str, Any] = {}
            if failed_blocks:
                evidence["failed_block_checks"] = failed_blocks
                rule_names = ", ".join(str(c.get("rule_name")) for c in failed_blocks)
                root_cause = f"Dataset quality check(s) failed with BLOCK severity: {rule_names}"
            else:
                assert contract_violation_run is not None
                evidence["failed_run"] = contract_violation_run
                root_cause = (
                    f"Data contract violation in run {contract_violation_run.get('run_id')}: "
                    f"{contract_violation_run.get('error_message')}"
                )

            inc = IncidentRecord(
                incident_code="INC-02",
                title="Data Quality Block Violation",
                severity="CRITICAL",
                status="ACTIVE",
                root_cause=root_cause,
                evidence=evidence,
                recommended_action="Isolate bad batch; execute ACTION_REBUILD_CURATED.",
                affected_scope="quality:curated_partitions",
                runbook_ref="docs/runbooks/DATA_QUALITY_RUNBOOK.md#incident-response",
                detected_at_utc=self.now_utc,
            )
            plan = RemediationPlan(
                incident_id=inc.incident_id,
                action_type="ACTION_REBUILD_CURATED",
                description="Reconstruct curated Parquet partitions from raw envelope storage.",
                parameters={"isolate_bad_batch": True},
                safety_checks=["verify_raw_envelopes_exist", "preserve_watermark_monotonicity"],
            )
            return inc, plan

        return None, None

    def _check_inc_03_storage_failure(
        self,
    ) -> tuple[IncidentRecord | None, RemediationPlan | None]:
        """INC-03: Storage & Promotion Failure (Exit code 5, disk > 80%)."""
        disk = self.telemetry.disk
        is_disk_critical = bool(
            disk.get("disk_critical", False) or disk.get("percent_used", 0) > 80.0
        )

        storage_failed_run = None
        runs = self._get_relevant_runs()
        for run in runs:
            if str(run.get("status", "")).upper() in ("FAILED", "FAILURE"):
                err_msg = str(run.get("error_message") or "")
                err_class = str(run.get("error_class") or "")
                full_text = f"{err_msg} {err_class}"
                if (
                    "exit code: 5" in full_text
                    or "exit code 5" in full_text
                    or STORAGE_PATTERNS.search(full_text) is not None
                ):
                    storage_failed_run = run
                    break

        if is_disk_critical or storage_failed_run:
            percent_used = disk.get("percent_used", 0)
            free_gb = disk.get("free_gb", 0)
            if is_disk_critical:
                root_cause = (
                    f"Filesystem storage pressure critical: {percent_used}% utilized "
                    f"({free_gb} GB free, exceeding 80% threshold)."
                )
            else:
                assert storage_failed_run is not None
                root_cause = (
                    f"Atomic partition storage or promotion failure in run "
                    f"{storage_failed_run.get('run_id')}: {storage_failed_run.get('error_message')}"
                )

            inc = IncidentRecord(
                incident_code="INC-03",
                title="Storage & Promotion Failure / Disk Pressure",
                severity="CRITICAL",
                status="ACTIVE",
                root_cause=root_cause,
                evidence={
                    "disk_usage": disk,
                    "storage_failed_run": storage_failed_run,
                },
                recommended_action=(
                    "Free storage volume headroom before resuming pipeline promotion."
                ),
                affected_scope="storage:volume",
                runbook_ref="docs/runbooks/OPERATIONAL_RUNBOOK.md#storage-and-disk-pressure",
                detected_at_utc=self.now_utc,
            )
            plan = RemediationPlan(
                incident_id=inc.incident_id,
                action_type="MANUAL",
                description="Manual operator volume expansion or temporary file cleanup required.",
                parameters={"percent_used": percent_used, "free_gb": free_gb},
                safety_checks=["operator_confirmation_required"],
            )
            return inc, plan

        return None, None

    def _check_inc_04_concurrency_lock(
        self,
    ) -> tuple[IncidentRecord | None, RemediationPlan | None]:
        """INC-04: Concurrency Lock Collision / Stale Lock (Exit code 6, RUNNING > 1h)."""
        lock = self.telemetry.running_lock
        runs = self._get_relevant_runs()

        # Check if lock is active
        if lock is not None:
            started_at_str = lock.get("started_at_utc")
            duration_seconds = 0.0
            if started_at_str:
                try:
                    s_dt = parse_iso_utc(str(started_at_str), "started_at_utc")
                    duration_seconds = max(0.0, (self.now_utc - s_dt).total_seconds())
                except Exception:
                    duration_seconds = 0.0

            run_id = str(lock.get("run_id", "unknown"))
            duration_hours = round(duration_seconds / 3600.0, 2)

            if duration_seconds >= 3600.0:
                # Stale lock older than 1 hour
                inc = IncidentRecord(
                    incident_code="INC-04",
                    title="Stale Concurrency Run Lock Detected",
                    severity="CRITICAL",
                    status="ACTIVE",
                    root_cause=(
                        f"Pipeline run lock for run {run_id} has been in RUNNING status for "
                        f"{duration_hours} hours (stale threshold: > 1 hour)."
                    ),
                    evidence={
                        "run_id": run_id,
                        "started_at_utc": started_at_str,
                        "duration_seconds": duration_seconds,
                        "duration_hours": duration_hours,
                    },
                    recommended_action=(
                        "ACTION_CLEAR_STALE_LOCK: Clear stale lock and verify process termination."
                    ),
                    affected_scope="concurrency:pipeline_lock",
                    runbook_ref="docs/runbooks/OPERATIONAL_RUNBOOK.md#concurrency-locks-exit-code-6",
                    detected_at_utc=self.now_utc,
                )
                plan = RemediationPlan(
                    incident_id=inc.incident_id,
                    action_type="ACTION_CLEAR_STALE_LOCK",
                    description=(
                        f"Safely clear stale run lock for {run_id} (held for {duration_hours}h)."
                    ),
                    parameters={"run_id": run_id, "duration_seconds": duration_seconds},
                    safety_checks=["verify_process_inactive", "verify_duration_gt_1h"],
                )
                return inc, plan
            else:
                # Active lock younger than 1 hour
                duration_m = round(duration_seconds / 60, 1)
                inc = IncidentRecord(
                    incident_code="INC-04",
                    title="Active Concurrency Run In Progress",
                    severity="WARNING",
                    status="ACTIVE",
                    root_cause=f"Active run {run_id} is running (started {duration_m}m ago).",
                    evidence={
                        "run_id": run_id,
                        "started_at_utc": started_at_str,
                        "duration_seconds": duration_seconds,
                    },
                    recommended_action="Wait for active run to finish; re-check in 5 minutes.",
                    affected_scope="concurrency:pipeline_lock",
                    runbook_ref="docs/runbooks/OPERATIONAL_RUNBOOK.md#concurrency-locks-exit-code-6",
                    detected_at_utc=self.now_utc,
                )
                return inc, None

        # Check for exit code 6 lock collisions in recent runs
        for run in runs:
            if str(run.get("status", "")).upper() in ("FAILED", "FAILURE"):
                err_msg = str(run.get("error_message") or "")
                err_class = str(run.get("error_class") or "")
                full_text = f"{err_msg} {err_class}"
                if (
                    "exit code: 6" in full_text
                    or "exit code 6" in full_text
                    or LOCK_PATTERNS.search(full_text) is not None
                ):
                    r_id = run.get("run_id")
                    inc = IncidentRecord(
                        incident_code="INC-04",
                        title="Concurrency Lock Collision Encountered",
                        severity="WARNING",
                        status="ACTIVE",
                        root_cause=(
                            f"Run {r_id} was rejected due to an overlapping running execution."
                        ),
                        evidence={"collided_run_id": r_id, "error": err_msg},
                        recommended_action=(
                            "Ensure sequential scheduling to avoid overlapping triggers."
                        ),
                        affected_scope="concurrency:pipeline_lock",
                        runbook_ref="docs/runbooks/OPERATIONAL_RUNBOOK.md#concurrency-locks-exit-code-6",
                        detected_at_utc=self.now_utc,
                    )
                    return inc, None

        return None, None

    def _check_inc_05_freshness_gaps(
        self,
    ) -> tuple[IncidentRecord | None, RemediationPlan | None]:
        """INC-05: Freshness Lag / Missing Gaps (watermark age > 2h, gap count > 0)."""
        wm_age = self.telemetry.watermark_age_hours
        gaps = self.telemetry.gaps

        has_freshness_lag = wm_age is not None and wm_age > 2.0
        has_gaps = len(gaps) > 0

        if has_freshness_lag or has_gaps:
            is_critical = bool(wm_age is not None and wm_age > 24.0)
            severity = "CRITICAL" if is_critical else "WARNING"

            reasons: list[str] = []
            if has_freshness_lag:
                reasons.append(f"watermark lag is {wm_age} hours (> 2h threshold)")
            if has_gaps:
                reasons.append(f"{len(gaps)} missing hourly gap(s) detected in fact data")

            inc = IncidentRecord(
                incident_code="INC-05",
                title="Data Freshness Lag / Missing Hourly Gaps",
                severity=severity,
                status="ACTIVE",
                root_cause=f"Pipeline freshness degraded: {'; '.join(reasons)}.",
                evidence={
                    "watermark_utc": self.telemetry.watermark_utc,
                    "watermark_age_hours": wm_age,
                    "gaps": gaps,
                },
                recommended_action=(
                    "ACTION_BACKFILL_GAPS: Execute bounded gap backfill for missing hours."
                ),
                affected_scope="pipeline:btc_usd_hourly",
                runbook_ref="docs/runbooks/OPERATIONAL_RUNBOOK.md#freshness-lag-and-backfill",
                detected_at_utc=self.now_utc,
            )
            max_h = min(24, len(gaps) or 24)
            plan = RemediationPlan(
                incident_id=inc.incident_id,
                action_type="ACTION_BACKFILL_GAPS",
                description=f"Execute bounded backfill for detected gaps (bounded to {max_h}h).",
                parameters={"gaps": gaps[:24], "max_hours": 24},
                safety_checks=["verify_no_active_lock", "bounded_to_24h"],
            )
            return inc, plan

        return None, None

    def _check_inc_06_lakehouse_bloat(
        self,
    ) -> tuple[list[IncidentRecord], list[RemediationPlan]]:
        """INC-06: Lakehouse Small-File Bloat (> 32 small files < 16MB)."""
        incidents: list[IncidentRecord] = []
        plans: list[RemediationPlan] = []

        for table_name, table_info in self.telemetry.lakehouse_tables.items():
            small_files_by_part = table_info.get("small_files_by_partition", {})
            for part_name, count in small_files_by_part.items():
                if count > 32:
                    inc = IncidentRecord(
                        incident_code="INC-06",
                        title=f"Lakehouse Small-File Bloat ({table_name}/{part_name})",
                        severity="WARNING",
                        status="ACTIVE",
                        root_cause=(
                            f"Partition '{part_name}' in table '{table_name}' contains "
                            f"{count} small files (< 16 MiB), exceeding the 32 file threshold."
                        ),
                        evidence={
                            "table_name": table_name,
                            "partition": part_name,
                            "small_files_count": count,
                            "threshold": 32,
                            "target_size_mb": 128,
                        },
                        recommended_action=(
                            "ACTION_COMPACT_LAKEHOUSE: Bin-pack small files into 128 MiB target."
                        ),
                        affected_scope=f"lakehouse:{table_name}/{part_name}",
                        runbook_ref="docs/decisions/D-011_LAKEHOUSE_EVOLUTION.md#compaction",
                        detected_at_utc=self.now_utc,
                    )
                    plan = RemediationPlan(
                        incident_id=inc.incident_id,
                        action_type="ACTION_COMPACT_LAKEHOUSE",
                        description=(
                            f"Bin-pack {count} files in '{table_name}' partition '{part_name}' "
                            f"into 128 MiB files."
                        ),
                        parameters={
                            "table_name": table_name,
                            "partition": part_name,
                            "target_size_mb": 128,
                        },
                        safety_checks=["verify_table_exists", "verify_small_files_gt_threshold"],
                    )
                    incidents.append(inc)
                    plans.append(plan)

        return incidents, plans
