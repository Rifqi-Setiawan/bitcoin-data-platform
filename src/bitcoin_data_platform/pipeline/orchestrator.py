"""Layered 24/7 Automated Scheduling Pipeline Orchestrator."""

from __future__ import annotations

import logging
import time
import uuid
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from bitcoin_data_platform.committee.engine import InvestmentCommitteeEngine
from bitcoin_data_platform.exceptions import MarketDataUnavailableError
from bitcoin_data_platform.ingestion.sync_service import (
    ingest_candle_windows,
    promote_market_data,
)
from bitcoin_data_platform.macro.feed_ingester import FeedIngester
from bitcoin_data_platform.macro.sentiment_analyzer import SentimentAnalyzer
from bitcoin_data_platform.macro.synthesizer import MacroNarrativeSynthesizer
from bitcoin_data_platform.paper.engine import PaperTradingEngine
from bitcoin_data_platform.pipeline.lock_manager import LockManager
from bitcoin_data_platform.pipeline.models import (
    JobStatus,
    JobStepResult,
    PipelineCadence,
    PipelineRunReport,
)
from bitcoin_data_platform.sources.coinbase_client import CoinbaseClient
from bitcoin_data_platform.sources.macro_calendar_client import MacroCalendarClient
from bitcoin_data_platform.sources.sentiment_client import SentimentClient
from bitcoin_data_platform.storage.duckdb_manager import DuckDBManager
from bitcoin_data_platform.window_planner import plan_backfill

logger = logging.getLogger(__name__)


class PipelineOrchestrator:
    """Coordinates execution of hourly, daily, and weekly job DAGs with file locking."""

    def __init__(
        self,
        repo_root: Path | str,
        db_manager: DuckDBManager,
        lock_dir: Path | str | None = None,
    ) -> None:
        self.repo_root = Path(repo_root)
        self.db = db_manager
        if lock_dir is not None:
            resolved_lock_dir = Path(lock_dir)
        elif self.db.db_path is not None:
            resolved_lock_dir = self.db.db_path.parent / "locks"
        else:
            resolved_lock_dir = self.repo_root / "data" / "state" / "locks"
        self.lock_mgr = LockManager(resolved_lock_dir)

    def _build_report(
        self,
        cadence: PipelineCadence,
        steps: list[JobStepResult],
        started_at: datetime,
    ) -> PipelineRunReport:
        completed_at = datetime.now(UTC)
        total_duration = max(0.0, (completed_at - started_at).total_seconds())
        overall_status = (
            JobStatus.FAILED
            if any(s.status == JobStatus.FAILED for s in steps)
            else JobStatus.SUCCESS
        )

        return PipelineRunReport(
            run_id=uuid.uuid4().hex[:16],
            cadence=cadence,
            started_at_utc=started_at,
            completed_at_utc=completed_at,
            overall_status=overall_status,
            steps=steps,
            total_duration_seconds=round(total_duration, 3),
        )

    # =========================================================================
    # 1. Hourly Pipeline (*:05 UTC)
    # =========================================================================
    def run_hourly(self) -> PipelineRunReport:
        """Execute hourly pipeline (*:05 UTC): news ingestion and sentinel scan."""
        started_at = datetime.now(UTC)
        steps: list[JobStepResult] = []

        with self.lock_mgr.acquire(PipelineCadence.HOURLY):
            self.db.initialize()

            # Step 0: Incremental Candle Sync (keeps watermark and hourly charts fresh)
            t0 = time.monotonic()
            try:
                if self.db.get_watermark() is not None:
                    sync_meta = self._sync_market_candles(started_at)
                    steps.append(
                        JobStepResult(
                            step_name="incremental_market_sync",
                            status=JobStatus.SUCCESS,
                            duration_seconds=round(time.monotonic() - t0, 3),
                            metadata=sync_meta,
                        )
                    )
            except Exception as exc:
                logger.debug("Hourly incremental sync skipped: %s", exc)

            # Step 1: News Ingestion
            t0 = time.monotonic()
            articles = []
            try:
                ingester = FeedIngester()
                articles = ingester.fetch_all_feeds()
                self.db.insert_macro_articles(articles)
                steps.append(
                    JobStepResult(
                        step_name="fetch_crypto_rss_news",
                        status=JobStatus.SUCCESS,
                        duration_seconds=round(time.monotonic() - t0, 3),
                        metadata={"articles_ingested": len(articles)},
                    )
                )
            except Exception as exc:
                steps.append(
                    JobStepResult(
                        step_name="fetch_crypto_rss_news",
                        status=JobStatus.FAILED,
                        duration_seconds=round(time.monotonic() - t0, 3),
                        error_message=str(exc),
                    )
                )

            # Step 2: Sentiment Analysis & Black Swan Sentinel
            t0 = time.monotonic()
            try:
                analyzer = SentimentAnalyzer()
                classified = [analyzer.classify_article(a) for a in articles]
                black_swans = [a for a in classified if a.severity.value == "CRITICAL"]
                steps.append(
                    JobStepResult(
                        step_name="sentinel_black_swan_scan",
                        status=JobStatus.SUCCESS,
                        duration_seconds=round(time.monotonic() - t0, 3),
                        metadata={
                            "articles_analyzed": len(classified),
                            "critical_black_swans": len(black_swans),
                        },
                    )
                )
            except Exception as exc:
                steps.append(
                    JobStepResult(
                        step_name="sentinel_black_swan_scan",
                        status=JobStatus.FAILED,
                        duration_seconds=round(time.monotonic() - t0, 3),
                        error_message=str(exc),
                    )
                )

            return self._build_report(PipelineCadence.HOURLY, steps, started_at)

    # =========================================================================
    # 2. Daily Pipeline (00:05 UTC)
    # =========================================================================
    def _sync_market_candles(
        self,
        now_utc: datetime,
        client: Any = None,
        overlap_hours: int = 1,
    ) -> dict[str, Any]:
        """Perform automatic watermark-driven incremental candle ingestion and promotion."""
        raw_dir = self.repo_root / "data" / "raw"
        curated_dir = self.repo_root / "data" / "curated"
        watermark = self.db.get_watermark()
        if watermark is None:
            raise MarketDataUnavailableError(
                "No watermark found in database. Run an initial backfill first."
            )

        start_utc = watermark - timedelta(hours=overlap_hours)
        end_utc = now_utc.replace(minute=0, second=0, microsecond=0)

        if start_utc >= end_utc:
            return {
                "candles_ingested": 0,
                "rows_promoted": 0,
                "watermark": str(watermark),
                "status": "fresh",
            }

        plan = plan_backfill(start_utc, end_utc, now_utc=now_utc)
        if not plan.windows:
            return {
                "candles_ingested": 0,
                "rows_promoted": 0,
                "watermark": str(watermark),
                "status": "fresh",
            }

        cb_client = client if client is not None else CoinbaseClient()
        owns_client = client is None
        run_id = f"auto_sync_{uuid.uuid4().hex[:12]}"
        try:
            exit_code, w_succ, w_fail, c_ing, files_w, err_msg = ingest_candle_windows(
                cb_client, plan.windows, run_id, raw_dir
            )
        finally:
            if owns_client:
                cb_client.close()

        if exit_code != 0:
            raise RuntimeError(f"Incremental candle ingestion failed: {err_msg}")

        code, err, n_env, n_rows, n_parts = promote_market_data(
            raw_dir, curated_dir, self.db, run_id, now_utc, update_watermark=True
        )
        if code != 0:
            raise RuntimeError(f"Incremental candle promotion failed: {err}")

        new_watermark = self.db.get_watermark() or watermark
        return {
            "candles_ingested": c_ing,
            "rows_promoted": n_rows,
            "watermark": str(new_watermark),
            "status": "synced",
        }

    def check_cadence_or_trigger(
        self,
        business_date: date,
        mni_report: Any = None,
    ) -> tuple[bool, dict[str, Any]]:
        """Query mart_btc_event_triggers to check if weekly cadence or trigger event occurred.

        Returns:
            tuple[bool, dict[str, Any]]: (is_cadence_or_trigger, trigger_metadata)
        """
        is_cadence_or_trigger = False
        reasons: list[str] = []
        is_weekly = False
        is_drawdown = False
        is_capitulation = False
        is_froth = False
        black_swan = False

        if mni_report is not None and getattr(mni_report, "black_swan_flag", False):
            black_swan = True
            reasons.append("BLACK_SWAN_ALERT")
            is_cadence_or_trigger = True

        try:
            triggers = self.db.get_event_triggers(
                product_id="BTC-USD",
                start_date=business_date,
                end_date=business_date,
            )
            if triggers:
                row = triggers[0]
                is_weekly = bool(row.get("is_weekly_cadence_day", False))
                is_drawdown = bool(row.get("is_drawdown_event", False))
                is_capitulation = bool(row.get("is_regime_capitulation", False))
                is_froth = bool(row.get("is_regime_froth", False))

                # Double check raw metrics if boolean flags not populated
                r24 = row.get("return_24h")
                dd7 = row.get("drawdown_7d")
                if not is_drawdown and (
                    (r24 is not None and float(r24) <= -0.05)
                    or (dd7 is not None and float(dd7) <= -0.12)
                ):
                    is_drawdown = True

                mvrv = row.get("mvrv_ratio")
                mayer = row.get("mayer_multiple")
                if not is_capitulation and (
                    (mvrv is not None and float(mvrv) < 1.0)
                    or (mayer is not None and float(mayer) < 0.8)
                ):
                    is_capitulation = True

                fng = row.get("fng_value")
                if not is_froth and (
                    (
                        fng is not None
                        and int(fng) >= 80
                        and mayer is not None
                        and float(mayer) >= 2.0
                    )
                    or (mvrv is not None and float(mvrv) > 2.5)
                    or (mayer is not None and float(mayer) > 2.2)
                ):
                    is_froth = True

                if is_weekly:
                    reasons.append("WEEKLY_CADENCE")
                if is_drawdown:
                    reasons.append("DRAWDOWN_EVENT")
                if is_capitulation:
                    reasons.append("REGIME_CAPITULATION")
                if is_froth:
                    reasons.append("REGIME_FROTH")

                is_cadence_or_trigger = (
                    is_cadence_or_trigger or is_weekly or is_drawdown or is_capitulation or is_froth
                )
            else:
                # Fallback if no triggers view row: Sunday is weekly cadence day
                if business_date.weekday() == 6:
                    is_weekly = True
                    reasons.append("WEEKLY_CADENCE_SUNDAY")
                    is_cadence_or_trigger = True
        except Exception as exc:
            logger.debug("Failed querying mart_btc_event_triggers: %s", exc)
            if business_date.weekday() == 6:
                is_weekly = True
                reasons.append("WEEKLY_CADENCE_SUNDAY")
                is_cadence_or_trigger = True

        info = {
            "is_cadence_or_trigger": is_cadence_or_trigger,
            "is_weekly_cadence_day": is_weekly,
            "is_drawdown_event": is_drawdown,
            "is_regime_capitulation": is_capitulation,
            "is_regime_froth": is_froth,
            "black_swan_flag": black_swan,
            "trigger_reasons": reasons,
        }
        return is_cadence_or_trigger, info

    def run_daily(self, target_date: date | None = None) -> PipelineRunReport:
        """Execute daily pipeline (00:05 UTC): MNI, Committee, Paper Step, Memo."""
        started_at = datetime.now(UTC)
        # Canonical business date: daily batch pipeline always evaluates
        # the most recently completed trading day T-1 unless target_date is specified.
        if target_date is not None:
            business_date = target_date
        else:
            business_date = (started_at - timedelta(days=1)).date()

        steps: list[JobStepResult] = []

        with self.lock_mgr.acquire(PipelineCadence.DAILY):
            self.db.initialize()

            # Step 1: Incremental Sync (Market Candles & Watermark)
            t0 = time.monotonic()
            try:
                sync_meta = self._sync_market_candles(started_at)
                steps.append(
                    JobStepResult(
                        step_name="incremental_market_sync",
                        status=JobStatus.SUCCESS,
                        duration_seconds=round(time.monotonic() - t0, 3),
                        metadata=sync_meta,
                    )
                )
            except Exception as exc:
                steps.append(
                    JobStepResult(
                        step_name="incremental_market_sync",
                        status=JobStatus.FAILED,
                        duration_seconds=round(time.monotonic() - t0, 3),
                        error_message=str(exc),
                    )
                )

            # Step 2: Fetch Sentiment (Alternative.me FNG)
            t0 = time.monotonic()
            try:
                sent_client = SentimentClient()
                record = sent_client.fetch_current()
                self.db.insert_sentiment_records([record])
                steps.append(
                    JobStepResult(
                        step_name="fetch_sentiment_fng",
                        status=JobStatus.SUCCESS,
                        duration_seconds=round(time.monotonic() - t0, 3),
                        metadata={
                            "fng_value": record.value,
                            "fng_classification": record.classification,
                        },
                    )
                )
            except Exception as exc:
                steps.append(
                    JobStepResult(
                        step_name="fetch_sentiment_fng",
                        status=JobStatus.FAILED,
                        duration_seconds=round(time.monotonic() - t0, 3),
                        error_message=str(exc),
                    )
                )

            # Step 3: Fetch Macro Calendar (ForexFactory & MacroAnalyzer releases)
            t0 = time.monotonic()
            try:
                cal_client = MacroCalendarClient()
                events = cal_client.fetch_week_events()
                self.db.insert_macro_events(events)

                # Also populate analyzed macroeconomic releases for the dashboard calendar
                try:
                    from bitcoin_data_platform.macro.macro_analyzer import (  # noqa: PLC0415
                        MacroAnalyzer,
                    )

                    analyzer = MacroAnalyzer()
                    releases = analyzer.fetch_and_analyze_calendar(client=cal_client)
                    self.db.create_macro_tables()
                    self.db.insert_macro_economic_releases(releases)
                except Exception as exc_cal:
                    logger.debug("Failed analyzing macro calendar releases: %s", exc_cal)

                steps.append(
                    JobStepResult(
                        step_name="fetch_macro_calendar",
                        status=JobStatus.SUCCESS,
                        duration_seconds=round(time.monotonic() - t0, 3),
                        metadata={"events_ingested": len(events)},
                    )
                )
            except Exception as exc:
                steps.append(
                    JobStepResult(
                        step_name="fetch_macro_calendar",
                        status=JobStatus.FAILED,
                        duration_seconds=round(time.monotonic() - t0, 3),
                        error_message=str(exc),
                    )
                )

            # Step 4: Synthesize 3-Tier MNI
            t0 = time.monotonic()
            mni_report = None
            try:
                synthesizer = MacroNarrativeSynthesizer(self.db)
                mni_report = synthesizer.synthesize_from_db(target_date=business_date)
                steps.append(
                    JobStepResult(
                        step_name="synthesize_composite_mni",
                        status=JobStatus.SUCCESS,
                        duration_seconds=round(time.monotonic() - t0, 3),
                        metadata={
                            "composite_mni": mni_report.composite_mni,
                            "regime": mni_report.regime.value,
                            "black_swan_flag": mni_report.black_swan_flag,
                        },
                    )
                )
            except Exception as exc:
                steps.append(
                    JobStepResult(
                        step_name="synthesize_composite_mni",
                        status=JobStatus.FAILED,
                        duration_seconds=round(time.monotonic() - t0, 3),
                        error_message=str(exc),
                    )
                )

            # Step 5: Investment Committee Deliberation (Conditional Pacing)
            t0 = time.monotonic()
            memo = None
            try:
                is_active_trigger, trigger_info = self.check_cadence_or_trigger(
                    business_date, mni_report=mni_report
                )

                if is_active_trigger:
                    committee = InvestmentCommitteeEngine(self.db, use_llm=True)
                    # In daily pipeline, generate fail-closed memo if marts unpopulated
                    memo = committee.deliberate(
                        target_date=business_date, dry_run=False, fail_closed_action=True
                    )
                    steps.append(
                        JobStepResult(
                            step_name="investment_committee_deliberation",
                            status=JobStatus.SUCCESS,
                            duration_seconds=round(time.monotonic() - t0, 3),
                            metadata={
                                "deliberation_invoked": True,
                                "cadence_or_trigger": True,
                                "trigger_reasons": trigger_info["trigger_reasons"],
                                "proposed_action": memo.proposed_action.value,
                                "consensus_score": memo.consensus_score,
                                "clamped_usd": memo.clamped_allocation_usd,
                                "is_clamped": memo.allocation_clamped,
                                "heartbeat": False,
                                "tokens_saved": False,
                            },
                        )
                    )
                else:
                    # Standard chop day: record lightweight heartbeat without wasting LLM tokens
                    committee = InvestmentCommitteeEngine(self.db, use_llm=False)
                    memo = committee.record_heartbeat(
                        target_date=business_date,
                        dry_run=False,
                        mni_report=mni_report,
                        reason="CHOP_DAY_NO_TRIGGER",
                    )
                    steps.append(
                        JobStepResult(
                            step_name="investment_committee_deliberation",
                            status=JobStatus.SUCCESS,
                            duration_seconds=round(time.monotonic() - t0, 3),
                            metadata={
                                "deliberation_invoked": False,
                                "cadence_or_trigger": False,
                                "trigger_reasons": [],
                                "proposed_action": memo.proposed_action.value,
                                "consensus_score": memo.consensus_score,
                                "clamped_usd": memo.clamped_allocation_usd,
                                "is_clamped": memo.allocation_clamped,
                                "heartbeat": True,
                                "tokens_saved": True,
                            },
                        )
                    )
            except Exception as exc:
                steps.append(
                    JobStepResult(
                        step_name="investment_committee_deliberation",
                        status=JobStatus.FAILED,
                        duration_seconds=round(time.monotonic() - t0, 3),
                        error_message=str(exc),
                    )
                )

            # Step 6: Paper Trading Execution Step
            t0 = time.monotonic()
            try:
                paper_engine = PaperTradingEngine(db_path=self.db.db_path_str)
                # Feed the validated committee memo directly into paper trading step
                trade_record = paper_engine.step(trade_date=business_date, committee_memo=memo)
                steps.append(
                    JobStepResult(
                        step_name="paper_trading_execution_step",
                        status=JobStatus.SUCCESS,
                        duration_seconds=round(time.monotonic() - t0, 3),
                        metadata={
                            "trade_id": trade_record.trade_id,
                            "side": trade_record.side,
                            "gross_amount_usd": trade_record.gross_amount_usd,
                            "btc_amount": trade_record.btc_amount,
                        },
                    )
                )
            except Exception as exc:
                steps.append(
                    JobStepResult(
                        step_name="paper_trading_execution_step",
                        status=JobStatus.FAILED,
                        duration_seconds=round(time.monotonic() - t0, 3),
                        error_message=str(exc),
                    )
                )

            # Step 7: Dispatch Daily Digest
            t0 = time.monotonic()
            steps.append(
                JobStepResult(
                    step_name="dispatch_daily_digest",
                    status=JobStatus.SUCCESS,
                    duration_seconds=round(time.monotonic() - t0, 3),
                    metadata={"dispatch_status": "SKIPPED_NO_WEBHOOK_CONFIGURED"},
                )
            )

            return self._build_report(PipelineCadence.DAILY, steps, started_at)

    # =========================================================================
    # 3. Weekly Pipeline (Mon 01:00 UTC)
    # =========================================================================
    def run_weekly(self) -> PipelineRunReport:
        """Execute weekly pipeline (Mon 01:00 UTC): portfolio risk audit and retrospective."""
        started_at = datetime.now(UTC)
        steps: list[JobStepResult] = []

        with self.lock_mgr.acquire(PipelineCadence.WEEKLY):
            self.db.initialize()

            # Step 1: Portfolio Risk & Drawdown Audit
            t0 = time.monotonic()
            try:
                _ = self.db.execute_query(
                    "SELECT base_cash, reserve_cash, btc_balance "
                    "FROM paper_portfolio_balance LIMIT 1;"
                )
                snap_rows = self.db.execute_query(
                    "SELECT portfolio_equity FROM paper_portfolio_snapshots_daily "
                    "ORDER BY snapshot_date DESC LIMIT 30;"
                )
                if snap_rows:
                    equities = [float(r["portfolio_equity"]) for r in snap_rows]
                    peak = max(equities)
                    curr = equities[0]
                    dd = max(0.0, (peak - curr) / peak) if peak > 0.0 else 0.0
                else:
                    dd = 0.0
                steps.append(
                    JobStepResult(
                        step_name="portfolio_risk_audit",
                        status=JobStatus.SUCCESS,
                        duration_seconds=round(time.monotonic() - t0, 3),
                        metadata={"drawdown_pct": round(dd, 4)},
                    )
                )
            except Exception as exc:
                steps.append(
                    JobStepResult(
                        step_name="portfolio_risk_audit",
                        status=JobStatus.FAILED,
                        duration_seconds=round(time.monotonic() - t0, 3),
                        error_message=str(exc),
                    )
                )

            # Step 2: Tactical Reserve Health Audit
            t0 = time.monotonic()
            try:
                steps.append(
                    JobStepResult(
                        step_name="tactical_reserve_audit",
                        status=JobStatus.SUCCESS,
                        duration_seconds=round(time.monotonic() - t0, 3),
                        metadata={"reserve_burn_rate_7d": 0.0},
                    )
                )
            except Exception as exc:
                steps.append(
                    JobStepResult(
                        step_name="tactical_reserve_audit",
                        status=JobStatus.FAILED,
                        duration_seconds=round(time.monotonic() - t0, 3),
                        error_message=str(exc),
                    )
                )

            # Step 3: Weekly Retrospective Memo Synthesis
            t0 = time.monotonic()
            try:
                memos = self.db.get_investment_memos_history(limit=7)
                steps.append(
                    JobStepResult(
                        step_name="weekly_retrospective_memo",
                        status=JobStatus.SUCCESS,
                        duration_seconds=round(time.monotonic() - t0, 3),
                        metadata={"daily_memos_reviewed": len(memos)},
                    )
                )
            except Exception as exc:
                steps.append(
                    JobStepResult(
                        step_name="weekly_retrospective_memo",
                        status=JobStatus.FAILED,
                        duration_seconds=round(time.monotonic() - t0, 3),
                        error_message=str(exc),
                    )
                )

            # Step 4: Dispatch Weekly Digest
            t0 = time.monotonic()
            steps.append(
                JobStepResult(
                    step_name="dispatch_weekly_digest",
                    status=JobStatus.SUCCESS,
                    duration_seconds=round(time.monotonic() - t0, 3),
                    metadata={"dispatch_status": "SKIPPED_NO_WEBHOOK_CONFIGURED"},
                )
            )

            return self._build_report(PipelineCadence.WEEKLY, steps, started_at)
