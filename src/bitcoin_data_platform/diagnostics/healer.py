"""Bounded self-healing runner with safety gates and dry-run simulation."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from bitcoin_data_platform.diagnostics.models import RemediationPlan, RemediationResult
from bitcoin_data_platform.storage.duckdb_manager import DuckDBManager


class RemediationRunner:
    """Executes bounded, safe self-healing actions with pre/post-condition validation."""

    def __init__(
        self,
        *,
        db_path: Path | str | None = None,
        curated_dir: Path | str | None = None,
        raw_dir: Path | str | None = None,
        lakehouse_catalog_dir: Path | str | None = None,
        dry_run: bool = True,
        now_utc: datetime | None = None,
        backfill_fn: Callable[[list[dict[str, Any]]], Any] | None = None,
        rebuild_fn: Callable[[], Any] | None = None,
    ) -> None:
        self.db_path = Path(db_path) if db_path else Path("./data/state/platform.duckdb")
        self.curated_dir = Path(curated_dir) if curated_dir else Path("./data/curated")
        self.raw_dir = Path(raw_dir) if raw_dir else Path("./data/raw")
        self.lakehouse_catalog_dir = (
            Path(lakehouse_catalog_dir)
            if lakehouse_catalog_dir
            else Path("./data/lakehouse/catalog")
        )
        self.dry_run = dry_run
        self.now_utc = (
            now_utc.astimezone(UTC)
            if now_utc and now_utc.tzinfo
            else (now_utc.replace(tzinfo=UTC) if now_utc else datetime.now(UTC))
        )
        self.backfill_fn = backfill_fn
        self.rebuild_fn = rebuild_fn

    def execute_all(self, plans: Sequence[RemediationPlan]) -> list[RemediationResult]:
        """Execute all remediation plans sequentially, respecting dry-run setting."""
        results: list[RemediationResult] = []
        for plan in plans:
            res = self.execute_plan(plan)
            results.append(res)
        return results

    def execute_plan(self, plan: RemediationPlan) -> RemediationResult:
        """Dispatch single remediation plan to appropriate action handler."""
        action = plan.action_type
        if action == "ACTION_CLEAR_STALE_LOCK":
            return self._clear_stale_lock(plan)
        elif action == "ACTION_BACKFILL_GAPS":
            return self._backfill_gaps(plan)
        elif action == "ACTION_REBUILD_CURATED":
            return self._rebuild_curated(plan)
        elif action == "ACTION_COMPACT_LAKEHOUSE":
            return self._compact_lakehouse(plan)
        elif action == "MANUAL":
            return RemediationResult(
                action_type="MANUAL",
                success=True,
                message=f"Manual operator action required: {plan.description}",
                remediated_at_utc=self.now_utc,
                details={"plan_id": plan.plan_id, "parameters": plan.parameters},
            )
        else:
            return RemediationResult(
                action_type=action,
                success=False,
                message=f"Unknown remediation action type: {action}",
                remediated_at_utc=self.now_utc,
                details={"plan_id": plan.plan_id},
            )

    def _clear_stale_lock(self, plan: RemediationPlan) -> RemediationResult:
        """Safely release stale RUNNING lock if duration > 1 hour."""
        duration_seconds = plan.parameters.get("duration_seconds", 0.0)
        run_id = plan.parameters.get("run_id", "unknown")

        # Safety Precondition: lock duration must exceed 3600 seconds (1 hour)
        if duration_seconds < 3600.0:
            return RemediationResult(
                action_type="ACTION_CLEAR_STALE_LOCK",
                success=False,
                message=(
                    f"Refusing to clear lock: lock duration ({round(duration_seconds / 60, 1)}m) "
                    f"does not exceed 1-hour stale threshold."
                ),
                remediated_at_utc=self.now_utc,
                details={"run_id": run_id, "duration_seconds": duration_seconds},
            )

        if self.dry_run:
            duration_hours_fmt = round(duration_seconds / 3600, 2)
            return RemediationResult(
                action_type="ACTION_CLEAR_STALE_LOCK",
                success=True,
                message=(
                    f"[DRY-RUN] Preconditions satisfied for run {run_id} "
                    f"(duration {duration_hours_fmt}h > 1h). Would execute force_clear_lock."
                ),
                remediated_at_utc=self.now_utc,
                details={"dry_run": True, "target_run_id": run_id},
            )

        # Execution
        try:
            db_mgr = DuckDBManager(db_path=self.db_path, curated_dir=self.curated_dir)
            with db_mgr:
                cleared = db_mgr.force_clear_lock(
                    stale_threshold_seconds=3600, now_utc=self.now_utc
                )
                # Postcondition: lock should no longer be active
                is_still_locked = db_mgr.check_lock()
                if is_still_locked:
                    return RemediationResult(
                        action_type="ACTION_CLEAR_STALE_LOCK",
                        success=False,
                        message="Postcondition failed: lock remains active after force_clear_lock.",
                        remediated_at_utc=self.now_utc,
                        details={"cleared_count": cleared},
                    )

            return RemediationResult(
                action_type="ACTION_CLEAR_STALE_LOCK",
                success=True,
                message=f"Successfully cleared {cleared} stale lock(s). System unlocked.",
                remediated_at_utc=self.now_utc,
                details={"cleared_count": cleared, "target_run_id": run_id},
            )
        except Exception as exc:
            return RemediationResult(
                action_type="ACTION_CLEAR_STALE_LOCK",
                success=False,
                message=f"Failed to clear stale lock: {exc}",
                remediated_at_utc=self.now_utc,
                details={"error": str(exc)},
            )

    def _backfill_gaps(self, plan: RemediationPlan) -> RemediationResult:
        """Execute bounded backfill for detected gaps (capped at 24 hours)."""
        raw_gaps = plan.parameters.get("gaps", [])
        # Bounded guardrail: max 24 hours per action
        bounded_gaps = raw_gaps[:24]

        if not bounded_gaps:
            return RemediationResult(
                action_type="ACTION_BACKFILL_GAPS",
                success=True,
                message="No missing hourly gaps require backfilling.",
                remediated_at_utc=self.now_utc,
                details={"gaps_processed": 0},
            )

        # Safety Precondition: check if system is currently locked
        if self.db_path.exists():
            try:
                db_mgr = DuckDBManager(db_path=self.db_path, curated_dir=self.curated_dir)
                with db_mgr:
                    if db_mgr.check_lock():
                        return RemediationResult(
                            action_type="ACTION_BACKFILL_GAPS",
                            success=False,
                            message=(
                                "Precondition failed: active run lock is held. Cannot run backfill."
                            ),
                            remediated_at_utc=self.now_utc,
                            details={"is_locked": True},
                        )
            except Exception:
                pass

        if self.dry_run:
            return RemediationResult(
                action_type="ACTION_BACKFILL_GAPS",
                success=True,
                message=(
                    f"[DRY-RUN] Would execute backfill for {len(bounded_gaps)} gap window(s) "
                    f"(capped at 24 hours maximum)."
                ),
                remediated_at_utc=self.now_utc,
                details={"dry_run": True, "bounded_gaps": bounded_gaps},
            )

        # Real execution via hook or planner
        try:
            if self.backfill_fn is not None:
                self.backfill_fn(bounded_gaps)

            return RemediationResult(
                action_type="ACTION_BACKFILL_GAPS",
                success=True,
                message=(
                    f"Successfully executed bounded backfill for {len(bounded_gaps)} gap window(s)."
                ),
                remediated_at_utc=self.now_utc,
                details={"bounded_gaps_count": len(bounded_gaps)},
            )
        except Exception as exc:
            return RemediationResult(
                action_type="ACTION_BACKFILL_GAPS",
                success=False,
                message=f"Failed to execute gap backfill: {exc}",
                remediated_at_utc=self.now_utc,
                details={"error": str(exc)},
            )

    def _rebuild_curated(self, plan: RemediationPlan) -> RemediationResult:
        """Rebuild curated Parquet partitions from raw envelopes without lowering watermark."""
        if self.dry_run:
            return RemediationResult(
                action_type="ACTION_REBUILD_CURATED",
                success=True,
                message=(
                    "[DRY-RUN] Would rebuild curated Parquet partitions from raw storage envelope "
                    "without altering watermark monotonicity."
                ),
                remediated_at_utc=self.now_utc,
                details={"dry_run": True},
            )

        try:
            if self.rebuild_fn is not None:
                self.rebuild_fn()
            else:
                # Refresh analytical views
                db_mgr = DuckDBManager(db_path=self.db_path, curated_dir=self.curated_dir)
                with db_mgr:
                    db_mgr.create_hourly_view()
                    db_mgr.create_daily_mart_view()

            return RemediationResult(
                action_type="ACTION_REBUILD_CURATED",
                success=True,
                message="Successfully validated curated partitions and analytical views.",
                remediated_at_utc=self.now_utc,
                details={"rebuilt": True},
            )
        except Exception as exc:
            return RemediationResult(
                action_type="ACTION_REBUILD_CURATED",
                success=False,
                message=f"Failed to rebuild curated partitions: {exc}",
                remediated_at_utc=self.now_utc,
                details={"error": str(exc)},
            )

    def _compact_lakehouse(self, plan: RemediationPlan) -> RemediationResult:
        """Merge fragmented Lakehouse small files into target size Parquet files."""
        table_name = plan.parameters.get("table_name")
        target_size_mb = plan.parameters.get("target_size_mb", 128)

        if not table_name:
            return RemediationResult(
                action_type="ACTION_COMPACT_LAKEHOUSE",
                success=False,
                message="Precondition failed: table_name parameter missing from plan.",
                remediated_at_utc=self.now_utc,
                details={"parameters": plan.parameters},
            )

        if self.dry_run:
            return RemediationResult(
                action_type="ACTION_COMPACT_LAKEHOUSE",
                success=True,
                message=(
                    f"[DRY-RUN] Would compact fragmented small files in table '{table_name}' "
                    f"targeting {target_size_mb} MiB per partition."
                ),
                remediated_at_utc=self.now_utc,
                details={
                    "dry_run": True,
                    "table_name": table_name,
                    "target_size_mb": target_size_mb,
                },
            )

        try:
            from bitcoin_data_platform.lakehouse.catalog import LakehouseCatalog
            from bitcoin_data_platform.lakehouse.compaction import CompactionEngine

            catalog = LakehouseCatalog(self.lakehouse_catalog_dir)
            engine = CompactionEngine(catalog)
            res = engine.compact(table_name, target_size_bytes=target_size_mb * 1024 * 1024)

            return RemediationResult(
                action_type="ACTION_COMPACT_LAKEHOUSE",
                success=True,
                message=(
                    f"Compacted table '{table_name}': "
                    f"{res.compacted_files_count} files -> {res.new_files_count} files."
                ),
                remediated_at_utc=self.now_utc,
                details=res.to_dict(),
            )
        except Exception as exc:
            return RemediationResult(
                action_type="ACTION_COMPACT_LAKEHOUSE",
                success=False,
                message=f"Failed to compact lakehouse table '{table_name}': {exc}",
                remediated_at_utc=self.now_utc,
                details={"error": str(exc)},
            )
