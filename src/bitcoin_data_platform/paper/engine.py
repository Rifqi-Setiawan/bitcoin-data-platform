"""Forward Paper Trading Simulation Engine for systematic Bitcoin portfolio validation."""

from __future__ import annotations

import logging
import uuid
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import duckdb
import httpx

from bitcoin_data_platform.committee.models import InvestmentMemorandum
from bitcoin_data_platform.exceptions import MarketDataUnavailableError
from bitcoin_data_platform.paper.models import (
    PaperPortfolioBalance,
    PaperSnapshotRecord,
    PaperSummary,
    PaperTradeRecord,
)
from bitcoin_data_platform.paper.pacing_guard import PacingGuard
from bitcoin_data_platform.paper.risk_guard import RiskGuard
from bitcoin_data_platform.signals.generator import generate_narrative
from bitcoin_data_platform.time_range import parse_iso_utc

logger = logging.getLogger(__name__)


class PaperEngineError(Exception):
    """Base exception for Paper Trading Engine operations."""


class PaperTradingEngine:
    """Forward paper trading engine executing systematic DCA orders against live market data."""

    def __init__(
        self,
        db_path: Path | str = "data/state/platform.duckdb",
        portfolio_id: str = "default",
        fee_bps: float = 10.0,  # 10.0 bps = 0.10% Coinbase spot fee
        risk_guard: RiskGuard | None = None,
        kill_switch_path: Path | str | None = None,
        allow_unpopulated: bool = False,
        pacing_guard: PacingGuard | None = None,
        weekly_base_usd: float = 20.0,
    ) -> None:
        self.db_path_str = str(db_path)
        self.db_path = Path(db_path) if self.db_path_str != ":memory:" else None
        self.portfolio_id = portfolio_id
        self.fee_bps = fee_bps
        resolved_kill_switch = kill_switch_path or (
            self.db_path.parent / "PAPER_KILL_SWITCH" if self.db_path is not None else None
        )
        self.risk_guard = risk_guard or RiskGuard(kill_switch_path=resolved_kill_switch)
        self.allow_unpopulated = allow_unpopulated
        self.weekly_base_usd = weekly_base_usd
        self.pacing_guard = pacing_guard or PacingGuard(weekly_base_usd=weekly_base_usd)

    def _get_connection(self, read_only: bool = False) -> duckdb.DuckDBPyConnection:
        """Create a fresh DuckDB connection with strict UTC timezone."""
        if self.db_path is not None:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            con = duckdb.connect(self.db_path_str, read_only=read_only)
            con.execute("SET TimeZone='UTC';")
            return con
        except Exception as exc:
            # Fallback to read_only=False if an active writer connection already exists
            if read_only and "different configuration" in str(exc):
                try:
                    con = duckdb.connect(self.db_path_str, read_only=False)
                    con.execute("SET TimeZone='UTC';")
                    return con
                except Exception as inner_exc:
                    raise PaperEngineError(
                        f"Failed connecting to DuckDB at {self.db_path_str}: {inner_exc}"
                    ) from inner_exc
            raise PaperEngineError(
                f"Failed connecting to DuckDB at {self.db_path_str}: {exc}"
            ) from exc

    def _ensure_schema(self, con: duckdb.DuckDBPyConnection) -> None:
        """Ensure all paper trading relational tables exist."""
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS paper_portfolio_balance (
                portfolio_id VARCHAR PRIMARY KEY,
                initial_cash DOUBLE NOT NULL,
                base_cash DOUBLE NOT NULL,
                reserve_cash DOUBLE NOT NULL,
                btc_balance DOUBLE NOT NULL,
                total_contributed DOUBLE NOT NULL,
                last_updated_utc TIMESTAMPTZ NOT NULL,
                total_trades INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS paper_portfolio_snapshots_daily (
                snapshot_date DATE NOT NULL,
                portfolio_id VARCHAR NOT NULL,
                base_cash DOUBLE NOT NULL,
                reserve_cash DOUBLE NOT NULL,
                total_cash DOUBLE NOT NULL,
                btc_balance DOUBLE NOT NULL,
                btc_price DOUBLE NOT NULL,
                portfolio_equity DOUBLE NOT NULL,
                unrealized_pnl_usd DOUBLE NOT NULL,
                unrealized_pnl_pct DOUBLE NOT NULL,
                benchmark_equity DOUBLE NOT NULL,
                PRIMARY KEY (snapshot_date, portfolio_id)
            );

            CREATE TABLE IF NOT EXISTS paper_trade_ledger (
                trade_id VARCHAR PRIMARY KEY,
                portfolio_id VARCHAR NOT NULL,
                executed_at_utc TIMESTAMPTZ NOT NULL,
                trade_date DATE NOT NULL,
                side VARCHAR NOT NULL,
                signal_regime VARCHAR NOT NULL,
                spot_price DOUBLE NOT NULL,
                gross_amount_usd DOUBLE NOT NULL,
                fee_usd DOUBLE NOT NULL,
                net_amount_usd DOUBLE NOT NULL,
                btc_amount DOUBLE NOT NULL,
                narrative VARCHAR NOT NULL
            );

            CREATE TABLE IF NOT EXISTS paper_pacing_state (
                portfolio_id VARCHAR PRIMARY KEY,
                sniper_armed BOOLEAN NOT NULL DEFAULT TRUE,
                active_drawdown_episode_id VARCHAR,
                last_weekly_date DATE,
                last_trade_date DATE,
                updated_at_utc TIMESTAMPTZ NOT NULL
            );
            """
        )

    def init_portfolio(
        self,
        initial_cash: float = 1000.0,
        overwrite: bool = False,
    ) -> PaperPortfolioBalance:
        """Initialize or retrieve the paper trading portfolio state.

        Partitions virtual capital into 40% Base Cash ($400) and 60% Tactical Reserve ($600).
        """
        if initial_cash <= 0.0:
            raise ValueError(f"initial_cash must be > 0, got {initial_cash}")

        con = self._get_connection(read_only=False)
        try:
            self._ensure_schema(con)

            existing = con.execute(
                """
                SELECT portfolio_id, initial_cash, base_cash, reserve_cash,
                       btc_balance, total_contributed,
                       STRFTIME(last_updated_utc, '%Y-%m-%dT%H:%M:%S.%fZ') AS last_updated_utc,
                       total_trades
                FROM paper_portfolio_balance
                WHERE portfolio_id = ?;
                """,
                [self.portfolio_id],
            ).fetchone()

            if existing and not overwrite:
                return PaperPortfolioBalance(
                    portfolio_id=existing[0],
                    initial_cash=float(existing[1]),
                    base_cash=float(existing[2]),
                    reserve_cash=float(existing[3]),
                    btc_balance=float(existing[4]),
                    total_contributed=float(existing[5]),
                    last_updated_utc=parse_iso_utc(str(existing[6])),
                    total_trades=int(existing[7]),
                )

            base_cash = round(0.40 * initial_cash, 2)
            reserve_cash = round(initial_cash - base_cash, 2)
            now_utc = datetime.now(UTC)

            balance = PaperPortfolioBalance(
                portfolio_id=self.portfolio_id,
                initial_cash=initial_cash,
                base_cash=base_cash,
                reserve_cash=reserve_cash,
                btc_balance=0.0,
                total_contributed=initial_cash,
                last_updated_utc=now_utc,
                total_trades=0,
            )

            con.execute(
                """
                INSERT OR REPLACE INTO paper_portfolio_balance (
                    portfolio_id, initial_cash, base_cash, reserve_cash,
                    btc_balance, total_contributed, last_updated_utc, total_trades
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
                """,
                [
                    balance.portfolio_id,
                    balance.initial_cash,
                    balance.base_cash,
                    balance.reserve_cash,
                    balance.btc_balance,
                    balance.total_contributed,
                    balance.last_updated_utc,
                    balance.total_trades,
                ],
            )

            con.execute(
                """
                INSERT OR REPLACE INTO paper_pacing_state (
                    portfolio_id, sniper_armed, active_drawdown_episode_id,
                    last_weekly_date, last_trade_date, updated_at_utc
                ) VALUES (?, true, NULL, NULL, NULL, ?);
                """,
                [self.portfolio_id, now_utc],
            )
            return balance
        finally:
            con.close()

    def reset_portfolio(self, initial_cash: float = 1000.0) -> PaperPortfolioBalance:
        """Purge existing transactions and snapshots, re-initializing virtual capital."""
        con = self._get_connection(read_only=False)
        try:
            self._ensure_schema(con)
            con.execute(
                "DELETE FROM paper_pacing_state WHERE portfolio_id = ?;",
                [self.portfolio_id],
            )
            con.execute(
                "DELETE FROM paper_trade_ledger WHERE portfolio_id = ?;",
                [self.portfolio_id],
            )
            con.execute(
                "DELETE FROM paper_portfolio_snapshots_daily WHERE portfolio_id = ?;",
                [self.portfolio_id],
            )
            con.execute(
                "DELETE FROM paper_portfolio_balance WHERE portfolio_id = ?;",
                [self.portfolio_id],
            )
        finally:
            con.close()

        return self.init_portfolio(initial_cash=initial_cash, overwrite=True)

    def get_portfolio_balance(self) -> PaperPortfolioBalance:
        """Fetch current portfolio balances, initializing if missing."""
        con = self._get_connection(read_only=False)
        try:
            self._ensure_schema(con)
            row = con.execute(
                """
                SELECT portfolio_id, initial_cash, base_cash, reserve_cash,
                       btc_balance, total_contributed,
                       STRFTIME(last_updated_utc, '%Y-%m-%dT%H:%M:%S.%fZ') AS last_updated_utc,
                       total_trades
                FROM paper_portfolio_balance
                WHERE portfolio_id = ?;
                """,
                [self.portfolio_id],
            ).fetchone()
        finally:
            con.close()

        if row is None:
            return self.init_portfolio()

        return PaperPortfolioBalance(
            portfolio_id=row[0],
            initial_cash=float(row[1]),
            base_cash=float(row[2]),
            reserve_cash=float(row[3]),
            btc_balance=float(row[4]),
            total_contributed=float(row[5]),
            last_updated_utc=parse_iso_utc(str(row[6])),
            total_trades=int(row[7]),
        )

    def _get_sniper_armed(self) -> bool:
        """Fetch current sniper armed state for the portfolio."""
        con = self._get_connection(read_only=True)
        try:
            row = con.execute(
                "SELECT sniper_armed FROM paper_pacing_state WHERE portfolio_id = ?;",
                [self.portfolio_id],
            ).fetchone()
            if row is not None:
                return bool(row[0])
            return True
        except Exception:
            return True
        finally:
            con.close()

    def _set_sniper_armed(
        self,
        sniper_armed: bool,
        trade_date: date,
    ) -> None:
        """Update sniper armed state in paper_pacing_state."""
        con = self._get_connection(read_only=False)
        try:
            self._ensure_schema(con)
            now_utc = datetime.now(UTC)
            con.execute(
                """
                INSERT OR REPLACE INTO paper_pacing_state (
                    portfolio_id, sniper_armed, active_drawdown_episode_id,
                    last_weekly_date, last_trade_date, updated_at_utc
                ) VALUES (?, ?, NULL, NULL, ?, ?);
                """,
                [self.portfolio_id, sniper_armed, trade_date, now_utc],
            )
        except Exception as exc:
            logger.debug("Failed updating paper_pacing_state: %s", exc)
        finally:
            con.close()

    def _get_tactical_debits_48h(
        self,
        trade_date: date,
    ) -> float:
        """Sum gross tactical reserve debits settled in the trailing 48-hour window."""
        con = self._get_connection(read_only=True)
        try:
            row = con.execute(
                """
                SELECT COALESCE(SUM(gross_amount_usd), 0.0)
                FROM paper_trade_ledger
                WHERE portfolio_id = ?
                  AND side = 'BUY'
                  AND signal_regime IN ('SNIPER_DEPLOYMENT', 'AGGRESSIVE_ACCUMULATE')
                  AND CAST(trade_date AS DATE) >= ? - INTERVAL 2 DAYS
                  AND CAST(trade_date AS DATE) <= ?;
                """,
                [self.portfolio_id, trade_date, trade_date],
            ).fetchone()
            return float(row[0]) if row and row[0] is not None else 0.0
        except Exception:
            return 0.0
        finally:
            con.close()

    def _query_triggers_and_signals(
        self,
        trade_date: date,
        force_spot_price: float | None = None,
    ) -> dict[str, Any]:
        """Retrieve spot price, signals, and trigger observations for trade date."""
        con = self._get_connection(read_only=True)
        try:
            # Check if mart_btc_event_triggers exists
            has_triggers = False
            try:
                t_check = con.execute(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_name = 'mart_btc_event_triggers';"
                ).fetchone()
                has_triggers = t_check is not None
            except Exception:
                has_triggers = False

            row = None
            if has_triggers:
                try:
                    row = con.execute(
                        """
                        SELECT
                            COALESCE(t.close, m.market_close_usd) AS spot_price,
                            m.investment_signal,
                            COALESCE(m.has_high_impact_macro_event, false) AS has_macro,
                            COALESCE(t.mayer_multiple, m.mayer_multiple) AS mayer_multiple,
                            COALESCE(t.mvrv_ratio, m.mvrv_ratio) AS mvrv_ratio,
                            COALESCE(t.fng_value, m.fng_value) AS fng_value,
                            t.return_24h,
                            t.drawdown_7d,
                            t.drawdown_30d,
                            t.is_weekly_cadence_day,
                            t.is_drawdown_event,
                            t.is_regime_capitulation,
                            t.is_regime_froth
                        FROM mart_btc_investment_signals_daily m
                        LEFT JOIN mart_btc_event_triggers t
                            ON CAST(m.trade_date_utc AS DATE) = CAST(t.trade_date_utc AS DATE)
                        WHERE CAST(m.trade_date_utc AS DATE) = ?;
                        """,
                        [trade_date],
                    ).fetchone()
                except Exception:
                    row = None

            if row is not None:
                spot = float(row[0]) if force_spot_price is None else force_spot_price
                sig = str(row[1]) if row[1] else "STANDARD_DCA"
                macro = bool(row[2]) if row[2] is not None else False
                mm = float(row[3]) if row[3] is not None else None
                mvrv = float(row[4]) if row[4] is not None else None
                fng = int(row[5]) if row[5] is not None else None
                r24 = float(row[6]) if row[6] is not None else None
                dd7 = float(row[7]) if row[7] is not None else None
                dd30 = float(row[8]) if row[8] is not None else None
                is_weekly = bool(row[9]) if row[9] is not None else (trade_date.weekday() == 6)
                is_dd = bool(row[10]) if row[10] is not None else None
                is_cap = bool(row[11]) if row[11] is not None else None
                is_froth = bool(row[12]) if row[12] is not None else None

                narrative = generate_narrative(sig, mm, mvrv, fng or 50)
                return {
                    "spot_price": spot,
                    "signal_regime": sig,
                    "has_macro": macro,
                    "narrative": narrative,
                    "mayer_multiple": mm,
                    "mvrv_ratio": mvrv,
                    "fng_value": fng,
                    "return_24h": r24,
                    "drawdown_7d": dd7,
                    "drawdown_30d": dd30,
                    "is_weekly_cadence_day": is_weekly,
                    "is_drawdown_event": is_dd,
                    "is_regime_capitulation": is_cap,
                    "is_regime_froth": is_froth,
                }

            # Query from mart_btc_investment_signals_daily alone if event_triggers had no row
            row_signals = None
            try:
                row_signals = con.execute(
                    """
                    SELECT market_close_usd, investment_signal, has_high_impact_macro_event,
                           mayer_multiple, mvrv_ratio, fng_value
                    FROM mart_btc_investment_signals_daily
                    WHERE CAST(trade_date_utc AS DATE) = ?;
                    """,
                    [trade_date],
                ).fetchone()
            except Exception:
                row_signals = None

            if row_signals is not None:
                spot = float(row_signals[0]) if force_spot_price is None else force_spot_price
                sig = str(row_signals[1]) if row_signals[1] else "STANDARD_DCA"
                macro = bool(row_signals[2]) if row_signals[2] is not None else False
                mm = float(row_signals[3]) if row_signals[3] is not None else None
                mvrv = float(row_signals[4]) if row_signals[4] is not None else None
                fng = int(row_signals[5]) if row_signals[5] is not None else 50

                prev_close = None
                try:
                    p_row = con.execute(
                        """
                        SELECT market_close_usd
                        FROM mart_btc_investment_signals_daily
                        WHERE CAST(trade_date_utc AS DATE) < ?
                        ORDER BY trade_date_utc DESC LIMIT 1;
                        """,
                        [trade_date],
                    ).fetchone()
                    if p_row and p_row[0] is not None:
                        prev_close = float(p_row[0])
                except Exception:
                    pass

                r24 = ((spot / prev_close) - 1.0) if prev_close and prev_close > 0 else None

                peak_7d = None
                try:
                    peak_row = con.execute(
                        """
                        SELECT MAX(market_close_usd)
                        FROM mart_btc_investment_signals_daily
                        WHERE CAST(trade_date_utc AS DATE) BETWEEN ? - INTERVAL 6 DAYS AND ?;
                        """,
                        [trade_date, trade_date],
                    ).fetchone()
                    if peak_row and peak_row[0] is not None:
                        peak_7d = float(peak_row[0])
                except Exception:
                    pass

                dd7 = ((spot / peak_7d) - 1.0) if peak_7d and peak_7d > 0 else 0.0

                narrative = generate_narrative(sig, mm, mvrv, fng)
                is_weekly = trade_date.weekday() == 6
                is_dd = (
                    (r24 is not None and r24 <= -0.05)
                    or (dd7 <= -0.12)
                    or (sig == "AGGRESSIVE_ACCUMULATE")
                )
                is_cap = (mvrv is not None and mvrv < 1.0) or (mm is not None and mm < 0.8)
                is_froth = (fng >= 80 and mm is not None and mm >= 2.0) or (sig == "HARD_FREEZE")

                return {
                    "spot_price": spot,
                    "signal_regime": sig,
                    "has_macro": macro,
                    "narrative": narrative,
                    "mayer_multiple": mm,
                    "mvrv_ratio": mvrv,
                    "fng_value": fng,
                    "return_24h": r24,
                    "drawdown_7d": dd7,
                    "drawdown_30d": dd7,
                    "is_weekly_cadence_day": is_weekly,
                    "is_drawdown_event": is_dd,
                    "is_regime_capitulation": is_cap,
                    "is_regime_froth": is_froth,
                }

            # Fallback to mart_btc_usd_daily
            fallback_row = None
            try:
                fallback_row = con.execute(
                    """
                    SELECT close
                    FROM mart_btc_usd_daily
                    WHERE CAST(trade_date_utc AS DATE) = ?
                    ORDER BY trade_date_utc DESC LIMIT 1;
                    """,
                    [trade_date],
                ).fetchone()
            except Exception:
                fallback_row = None

            if fallback_row is not None:
                spot = float(fallback_row[0]) if force_spot_price is None else force_spot_price
                return {
                    "spot_price": spot,
                    "signal_regime": "STANDARD_DCA",
                    "has_macro": False,
                    "narrative": "⚪ DCA STANDAR: Berdasarkan data harga harian.",
                    "mayer_multiple": None,
                    "mvrv_ratio": None,
                    "fng_value": 50,
                    "return_24h": None,
                    "drawdown_7d": None,
                    "drawdown_30d": None,
                    "is_weekly_cadence_day": trade_date.weekday() == 6,
                    "is_drawdown_event": False,
                    "is_regime_capitulation": False,
                    "is_regime_froth": False,
                }
        finally:
            con.close()

        if force_spot_price is not None:
            return {
                "spot_price": force_spot_price,
                "signal_regime": "STANDARD_DCA",
                "has_macro": False,
                "narrative": "⚪ DCA STANDAR: Eksekusi harga manual (forced spot price).",
                "mayer_multiple": None,
                "mvrv_ratio": None,
                "fng_value": 50,
                "return_24h": None,
                "drawdown_7d": None,
                "drawdown_30d": None,
                "is_weekly_cadence_day": trade_date.weekday() == 6,
                "is_drawdown_event": False,
                "is_regime_capitulation": False,
                "is_regime_froth": False,
            }

        if not self.allow_unpopulated:
            self.risk_guard.activate_kill_switch(
                reason=f"Market data unavailable in DuckDB for trade date {trade_date}"
            )
            raise MarketDataUnavailableError(
                f"No market data available in analytical marts for trade date {trade_date}. "
                "Trading halted fail-closed and kill switch activated."
            )

        # Fallback to live Coinbase spot ticker
        try:
            r = httpx.get(
                "https://api.exchange.coinbase.com/products/BTC-USD/ticker",
                headers={"User-Agent": "bitcoin-data-platform/0.1.0"},
                timeout=5.0,
            )
            if r.status_code == 200:
                spot = float(r.json()["price"])
                return {
                    "spot_price": spot,
                    "signal_regime": "STANDARD_DCA",
                    "has_macro": False,
                    "narrative": (
                        f"⚪ DCA STANDAR: Eksekusi harga spot live Coinbase (${spot:,.2f})."
                    ),
                    "mayer_multiple": None,
                    "mvrv_ratio": None,
                    "fng_value": 50,
                    "return_24h": None,
                    "drawdown_7d": None,
                    "drawdown_30d": None,
                    "is_weekly_cadence_day": trade_date.weekday() == 6,
                    "is_drawdown_event": False,
                    "is_regime_capitulation": False,
                    "is_regime_froth": False,
                }
        except Exception as ticker_err:
            logger.warning(f"Failed fetching live Coinbase ticker: {ticker_err}")

        raise MarketDataUnavailableError(
            f"No market data or spot price available for trade date {trade_date}"
        )

    def _query_day_signal_and_price(
        self,
        trade_date: date,
        force_spot_price: float | None = None,
    ) -> tuple[float, str, bool, str]:
        """Retrieve spot price, signal regime, macro status, and narrative for trade date."""
        data = self._query_triggers_and_signals(trade_date, force_spot_price)
        return (
            data["spot_price"],
            data["signal_regime"],
            data["has_macro"],
            data["narrative"],
        )

    def step(
        self,
        trade_date: date,
        daily_budget: float = 20.0,
        force_spot_price: float | None = None,
        committee_memo: InvestmentMemorandum | dict[str, Any] | None = None,
        execution_mode: str = "AUTO",
    ) -> PaperTradeRecord:
        """Advance paper portfolio by executing event-driven pacing strategy or manual order."""
        if daily_budget <= 0.0:
            raise ValueError(f"daily_budget must be > 0, got {daily_budget}")

        if self.risk_guard.is_kill_switch_active():
            raise PaperEngineError("RiskGuard validation rejected trade: Kill switch active")

        portfolio = self.get_portfolio_balance()

        # 1. Resolve committee memorandum (passed in or queried from DuckDB)
        memo_target = None
        memo_action = None
        if committee_memo is None:
            con_memo = self._get_connection(read_only=True)
            try:
                memo_row = con_memo.execute(
                    """
                    SELECT proposed_action, clamped_allocation_usd, executive_summary_id
                    FROM investment_committee_memos
                    WHERE memo_date = ?
                    ORDER BY created_at_utc DESC
                    LIMIT 1;
                    """,
                    [trade_date],
                ).fetchone()
                if memo_row:
                    committee_memo = {
                        "proposed_action": memo_row[0],
                        "clamped_allocation_usd": float(memo_row[1]),
                        "executive_summary_id": memo_row[2],
                    }
            except Exception:
                pass
            finally:
                con_memo.close()

        if committee_memo is not None:
            if isinstance(committee_memo, dict):
                memo_target = committee_memo.get("clamped_allocation_usd")
                memo_action = committee_memo.get("proposed_action")
            else:
                memo_target = getattr(committee_memo, "clamped_allocation_usd", None)
                act = getattr(committee_memo, "proposed_action", None)
                memo_action = getattr(act, "value", str(act)) if act is not None else None

        # 2. Decision-First Safe Abstention (HOLD):
        # If committee halted with DATA_UNAVAILABLE or <= 0 (excluding routine heartbeat/pacing)
        if memo_action not in ("HEARTBEAT", "NO_ACTION") and (
            memo_action == "DATA_UNAVAILABLE" or (memo_target is not None and memo_target <= 0.0)
        ):
            action_desc = memo_action or "DATA_UNAVAILABLE"
            trade_id = f"tr_{uuid.uuid4().hex[:12]}"
            ref_price = force_spot_price or 0.0
            if ref_price <= 0.0:
                try:
                    con_p = self._get_connection(read_only=True)
                    r_p = con_p.execute(
                        "SELECT close FROM mart_btc_usd_daily ORDER BY trade_date_utc DESC LIMIT 1;"
                    ).fetchone()
                    if r_p:
                        ref_price = float(r_p[0])
                    con_p.close()
                except Exception:
                    pass

            portfolio.last_updated_utc = datetime.now(UTC)
            trade_record = PaperTradeRecord(
                trade_id=trade_id,
                portfolio_id=self.portfolio_id,
                executed_at_utc=datetime.now(UTC),
                trade_date=trade_date,
                side="HOLD",
                signal_regime=action_desc,
                spot_price=ref_price,
                gross_amount_usd=0.0,
                fee_usd=0.0,
                net_amount_usd=0.0,
                btc_amount=0.0,
                narrative=f"🛑 KOMITE HALT: Alokasi disetujui $0.00 ({action_desc}).",
            )
            self._persist_trade_and_snapshot(trade_record, portfolio, ref_price)
            return trade_record

        # 3. For executable orders, query spot price, analytical signals, and triggers
        trigger_data = self._query_triggers_and_signals(
            trade_date=trade_date,
            force_spot_price=force_spot_price,
        )
        spot_price = trigger_data["spot_price"]
        signal_regime = trigger_data["signal_regime"]
        has_macro = trigger_data["has_macro"]
        fng_value = trigger_data.get("fng_value")
        mayer_multiple = trigger_data.get("mayer_multiple")
        norm_signal = signal_regime.upper().strip()

        # 4. Committee allocation override (if approved allocation > 0)
        if memo_target is not None and memo_target > 0.0:
            gross_amount_usd = min(float(memo_target), portfolio.base_cash + portfolio.reserve_cash)
            side = "BUY" if gross_amount_usd > 0.0 else "HOLD"
            base_buy = min(gross_amount_usd, portfolio.base_cash)
            rem = gross_amount_usd - base_buy
            reserve_draw = min(rem, portfolio.reserve_cash)
            base_deduction = base_buy
            reserve_deduction = reserve_draw
            narrative = (
                f"🏛️ EKSEKUSI KOMITE: Alokasi disetujui ${gross_amount_usd:,.2f} "
                f"[Base: ${base_buy:,.2f}, Cadangan: ${reserve_draw:,.2f}]."
            )
            risk_result = self.risk_guard.validate_trade(
                portfolio=portfolio,
                side=side,
                gross_amount_usd=gross_amount_usd,
                spot_price=spot_price,
                macro_event=has_macro,
                fng_value=fng_value,
                mayer_multiple=mayer_multiple,
            )
            if not risk_result.allowed:
                raise PaperEngineError(f"RiskGuard validation rejected trade: {risk_result.reason}")

            fee_rate = self.fee_bps / 10000.0
            fee_usd = round(gross_amount_usd * fee_rate, 4) if side == "BUY" else 0.0
            net_amount_usd = gross_amount_usd - fee_usd
            delta_btc = (
                (net_amount_usd / spot_price) if (side == "BUY" and spot_price > 0.0) else 0.0
            )

            portfolio.base_cash = round(max(0.0, portfolio.base_cash - base_deduction), 2)
            portfolio.reserve_cash = round(max(0.0, portfolio.reserve_cash - reserve_deduction), 2)
            portfolio.btc_balance = round(portfolio.btc_balance + delta_btc, 8)
            portfolio.last_updated_utc = datetime.now(UTC)
            if side == "BUY" and gross_amount_usd > 0.0:
                portfolio.total_trades += 1

            trade_id = f"tr_{uuid.uuid4().hex[:12]}"
            trade_record = PaperTradeRecord(
                trade_id=trade_id,
                portfolio_id=self.portfolio_id,
                executed_at_utc=datetime.now(UTC),
                trade_date=trade_date,
                side=side,
                signal_regime=norm_signal,
                spot_price=spot_price,
                gross_amount_usd=gross_amount_usd,
                fee_usd=fee_usd,
                net_amount_usd=net_amount_usd,
                btc_amount=delta_btc,
                narrative=narrative,
            )
            self._persist_trade_and_snapshot(trade_record, portfolio, spot_price)
            return trade_record

        # 5. Manual Force execution mode (Operator override)
        if execution_mode == "MANUAL_FORCE":
            base_slice = daily_budget
            gross_amount_usd = 0.0
            side = "BUY"
            base_deduction = 0.0
            reserve_deduction = 0.0
            reserve_addition = 0.0

            if has_macro:
                side = "HOLD"
                gross_amount_usd = 0.0
                reserve_addition = min(base_slice, portfolio.base_cash)
                base_deduction = reserve_addition
                narrative = (
                    f"🛑 JEDA MAKRO: Event makro USD dampak tinggi, "
                    f"${reserve_addition:,.2f} dialihkan ke cadangan taktis."
                )
            elif norm_signal == "HARD_FREEZE":
                side = "HOLD"
                gross_amount_usd = 0.0
                reserve_addition = min(base_slice, portfolio.base_cash)
                base_deduction = reserve_addition
                narrative = (
                    f"🔴 HENTIKAN PEMBELIAN: Pasar terlalu panas, "
                    f"${reserve_addition:,.2f} dialihkan ke cadangan taktis."
                )
            elif norm_signal == "DEFENSIVE_RESERVE":
                target_buy = 0.5 * base_slice
                target_reserve = 0.5 * base_slice
                actual_buy = min(target_buy, portfolio.base_cash)
                avail_after_buy = max(0.0, portfolio.base_cash - actual_buy)
                actual_reserve = min(target_reserve, avail_after_buy)
                gross_amount_usd = actual_buy
                base_deduction = actual_buy + actual_reserve
                reserve_addition = actual_reserve
                side = "BUY" if gross_amount_usd > 0.0 else "HOLD"
                narrative = (
                    f"🟡 CADANGAN DEFENSIF: Beli ${gross_amount_usd:,.2f} (0.5x), "
                    f"${actual_reserve:,.2f} dialihkan ke cadangan."
                )
            elif norm_signal == "AGGRESSIVE_ACCUMULATE":
                base_target = 2.0 * base_slice
                base_buy = min(base_target, portfolio.base_cash)
                reserve_draw = min(0.25 * portfolio.reserve_cash, portfolio.reserve_cash)
                gross_amount_usd = base_buy + reserve_draw
                base_deduction = base_buy
                reserve_deduction = reserve_draw
                side = "BUY" if gross_amount_usd > 0.0 else "HOLD"
                narrative = (
                    f"🟢 AKUMULASI AGRESIF: Beli ${gross_amount_usd:,.2f} "
                    f"(2.0x base + 25% cadangan). "
                    f"[Base: ${base_buy:,.2f}, Cadangan: ${reserve_draw:,.2f}]"
                )
            elif norm_signal == "OPPORTUNISTIC_ACCUMULATE":
                target_buy = 1.3 * base_slice
                gross_amount_usd = min(target_buy, portfolio.base_cash)
                base_deduction = gross_amount_usd
                side = "BUY" if gross_amount_usd > 0.0 else "HOLD"
                narrative = f"🔵 AKUMULASI OPORTUNISTIK: Beli ${gross_amount_usd:,.2f} (1.3x base)."
            else:
                target_buy = 1.0 * base_slice
                gross_amount_usd = min(target_buy, portfolio.base_cash)
                base_deduction = gross_amount_usd
                side = "BUY" if gross_amount_usd > 0.0 else "HOLD"
                narrative = f"⚪ DCA STANDAR: Beli ${gross_amount_usd:,.2f} (1.0x base)."

            risk_result = self.risk_guard.validate_trade(
                portfolio=portfolio,
                side=side,
                gross_amount_usd=gross_amount_usd,
                spot_price=spot_price,
                macro_event=has_macro,
                fng_value=fng_value,
                mayer_multiple=mayer_multiple,
            )
            if not risk_result.allowed:
                raise PaperEngineError(f"RiskGuard validation rejected trade: {risk_result.reason}")

            fee_rate = self.fee_bps / 10000.0
            fee_usd = round(gross_amount_usd * fee_rate, 4) if side == "BUY" else 0.0
            net_amount_usd = gross_amount_usd - fee_usd
            delta_btc = (
                (net_amount_usd / spot_price) if (side == "BUY" and spot_price > 0.0) else 0.0
            )

            portfolio.base_cash = round(max(0.0, portfolio.base_cash - base_deduction), 2)
            portfolio.reserve_cash = round(
                max(0.0, portfolio.reserve_cash - reserve_deduction + reserve_addition), 2
            )
            portfolio.btc_balance = round(portfolio.btc_balance + delta_btc, 8)
            portfolio.last_updated_utc = datetime.now(UTC)
            if side == "BUY" and gross_amount_usd > 0.0:
                portfolio.total_trades += 1

            trade_id = f"tr_{uuid.uuid4().hex[:12]}"
            trade_record = PaperTradeRecord(
                trade_id=trade_id,
                portfolio_id=self.portfolio_id,
                executed_at_utc=datetime.now(UTC),
                trade_date=trade_date,
                side=side,
                signal_regime=norm_signal,
                spot_price=spot_price,
                gross_amount_usd=gross_amount_usd,
                fee_usd=fee_usd,
                net_amount_usd=net_amount_usd,
                btc_amount=delta_btc,
                narrative=narrative,
            )
            self._persist_trade_and_snapshot(trade_record, portfolio, spot_price)
            return trade_record

        # 6. AUTO Execution Mode (Event-Driven Regime Pacing with PacingGuard)
        sniper_armed = self._get_sniper_armed()
        tactical_debits_48h = self._get_tactical_debits_48h(trade_date)

        decision, new_sniper_armed = self.pacing_guard.evaluate(
            trade_date=trade_date,
            base_cash=portfolio.base_cash,
            reserve_cash=portfolio.reserve_cash,
            sniper_armed=sniper_armed,
            return_24h=trigger_data.get("return_24h"),
            drawdown_7d=trigger_data.get("drawdown_7d"),
            drawdown_30d=trigger_data.get("drawdown_30d"),
            mvrv_ratio=trigger_data.get("mvrv_ratio"),
            prev_mvrv_ratio=trigger_data.get("prev_mvrv_ratio"),
            mayer_multiple=trigger_data.get("mayer_multiple"),
            prev_mayer_multiple=trigger_data.get("prev_mayer_multiple"),
            fng_value=fng_value,
            is_weekly_cadence_day=trigger_data.get("is_weekly_cadence_day"),
            has_high_impact_macro_event=has_macro,
            investment_signal=norm_signal,
            is_drawdown_event=trigger_data.get("is_drawdown_event"),
            is_regime_capitulation=trigger_data.get("is_regime_capitulation"),
            is_regime_froth=trigger_data.get("is_regime_froth"),
            tactical_debits_48h=tactical_debits_48h,
            execution_mode="AUTO",
            manual_budget=daily_budget if daily_budget != 20.0 else None,
        )

        trade_id = f"tr_{uuid.uuid4().hex[:12]}"
        portfolio.last_updated_utc = datetime.now(UTC)

        # 6A. Sideways Chop (NO_ACTION) - Zero execution, zero fee, zero cash burn
        if decision.action == "NO_ACTION":
            trade_record = PaperTradeRecord(
                trade_id=trade_id,
                portfolio_id=self.portfolio_id,
                executed_at_utc=datetime.now(UTC),
                trade_date=trade_date,
                side="NO_ACTION",
                signal_regime="NO_ACTION",
                spot_price=spot_price,
                gross_amount_usd=0.0,
                fee_usd=0.0,
                net_amount_usd=0.0,
                btc_amount=0.0,
                narrative=decision.narrative,
            )
            self._persist_trade_and_snapshot(trade_record, portfolio, spot_price)
            self._set_sniper_armed(new_sniper_armed, trade_date)
            return trade_record

        # 6B. HOLD (Froth Freeze or Hard Safety)
        if decision.action == "HOLD":
            trade_record = PaperTradeRecord(
                trade_id=trade_id,
                portfolio_id=self.portfolio_id,
                executed_at_utc=datetime.now(UTC),
                trade_date=trade_date,
                side="HOLD",
                signal_regime=decision.state.value,
                spot_price=spot_price,
                gross_amount_usd=0.0,
                fee_usd=0.0,
                net_amount_usd=0.0,
                btc_amount=0.0,
                narrative=decision.narrative,
            )
            self._persist_trade_and_snapshot(trade_record, portfolio, spot_price)
            self._set_sniper_armed(new_sniper_armed, trade_date)
            return trade_record

        # 6C. BUY (SNIPER_DEPLOYMENT or WEEKLY_CORE)
        gross_amount_usd = decision.authorized_amount_usd
        side = "BUY" if gross_amount_usd > 0.0 else "HOLD"
        pool_str = decision.pool.value
        is_sniper = decision.is_sniper

        risk_result = self.risk_guard.validate_trade(
            portfolio=portfolio,
            side=side,
            gross_amount_usd=gross_amount_usd,
            spot_price=spot_price,
            macro_event=has_macro,
            is_sniper=is_sniper,
            pool=pool_str,
            tactical_debits_48h=tactical_debits_48h,
            weekly_base_usd=self.weekly_base_usd,
            fng_value=fng_value,
            mayer_multiple=mayer_multiple,
        )
        if not risk_result.allowed:
            raise PaperEngineError(f"RiskGuard validation rejected trade: {risk_result.reason}")

        fee_rate = self.fee_bps / 10000.0  # 10 bps
        fee_usd = round(gross_amount_usd * fee_rate, 4) if side == "BUY" else 0.0
        net_amount_usd = gross_amount_usd - fee_usd
        delta_btc = (net_amount_usd / spot_price) if (side == "BUY" and spot_price > 0.0) else 0.0

        if is_sniper or pool_str in ("00_TACTICAL_RESERVE", "TACTICAL_RESERVE"):
            portfolio.reserve_cash = round(max(0.0, portfolio.reserve_cash - gross_amount_usd), 2)
        else:
            portfolio.base_cash = round(max(0.0, portfolio.base_cash - gross_amount_usd), 2)

        portfolio.btc_balance = round(portfolio.btc_balance + delta_btc, 8)
        if side == "BUY" and gross_amount_usd > 0.0:
            portfolio.total_trades += 1

        trade_record = PaperTradeRecord(
            trade_id=trade_id,
            portfolio_id=self.portfolio_id,
            executed_at_utc=datetime.now(UTC),
            trade_date=trade_date,
            side=side,
            signal_regime=decision.state.value,
            spot_price=spot_price,
            gross_amount_usd=gross_amount_usd,
            fee_usd=fee_usd,
            net_amount_usd=net_amount_usd,
            btc_amount=delta_btc,
            narrative=decision.narrative,
        )
        self._persist_trade_and_snapshot(trade_record, portfolio, spot_price)
        self._set_sniper_armed(new_sniper_armed, trade_date)
        return trade_record

    def _persist_trade_and_snapshot(
        self,
        trade_record: PaperTradeRecord,
        portfolio: PaperPortfolioBalance,
        spot_price: float,
    ) -> None:
        """Persist trade record, balance updates, and daily snapshot atomically to DuckDB."""
        con = self._get_connection(read_only=False)
        try:
            self._ensure_schema(con)

            # A. Update balance table
            con.execute(
                """
                INSERT OR REPLACE INTO paper_portfolio_balance (
                    portfolio_id, initial_cash, base_cash, reserve_cash,
                    btc_balance, total_contributed, last_updated_utc, total_trades
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
                """,
                [
                    portfolio.portfolio_id,
                    portfolio.initial_cash,
                    portfolio.base_cash,
                    portfolio.reserve_cash,
                    portfolio.btc_balance,
                    portfolio.total_contributed,
                    portfolio.last_updated_utc,
                    portfolio.total_trades,
                ],
            )

            # B. Insert trade ledger record
            con.execute(
                """
                INSERT INTO paper_trade_ledger (
                    trade_id, portfolio_id, executed_at_utc, trade_date,
                    side, signal_regime, spot_price, gross_amount_usd,
                    fee_usd, net_amount_usd, btc_amount, narrative
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                [
                    trade_record.trade_id,
                    trade_record.portfolio_id,
                    trade_record.executed_at_utc,
                    trade_record.trade_date,
                    trade_record.side,
                    trade_record.signal_regime,
                    trade_record.spot_price,
                    trade_record.gross_amount_usd,
                    trade_record.fee_usd,
                    trade_record.net_amount_usd,
                    trade_record.btc_amount,
                    trade_record.narrative,
                ],
            )

            # C. Determine Benchmark Equity ($1,000 Buy & Hold)
            earliest_snap = con.execute(
                """
                SELECT btc_price
                FROM paper_portfolio_snapshots_daily
                WHERE portfolio_id = ?
                ORDER BY snapshot_date ASC
                LIMIT 1;
                """,
                [self.portfolio_id],
            ).fetchone()

            if earliest_snap is not None and float(earliest_snap[0]) > 0.0:
                p_0 = float(earliest_snap[0])
            else:
                p_0 = spot_price if spot_price > 0.0 else 65000.0

            benchmark_equity = (
                (portfolio.initial_cash / p_0) * spot_price if p_0 > 0.0 else portfolio.initial_cash
            )

            total_cash = portfolio.total_cash
            portfolio_equity = total_cash + (portfolio.btc_balance * spot_price)
            unrealized_pnl_usd = portfolio_equity - portfolio.initial_cash
            unrealized_pnl_pct = (
                (unrealized_pnl_usd / portfolio.initial_cash) * 100.0
                if portfolio.initial_cash > 0.0
                else 0.0
            )

            snapshot = PaperSnapshotRecord(
                snapshot_date=trade_record.trade_date,
                portfolio_id=self.portfolio_id,
                base_cash=portfolio.base_cash,
                reserve_cash=portfolio.reserve_cash,
                total_cash=total_cash,
                btc_balance=portfolio.btc_balance,
                btc_price=spot_price,
                portfolio_equity=portfolio_equity,
                unrealized_pnl_usd=unrealized_pnl_usd,
                unrealized_pnl_pct=unrealized_pnl_pct,
                benchmark_equity=benchmark_equity,
            )

            con.execute(
                """
                INSERT OR REPLACE INTO paper_portfolio_snapshots_daily (
                    snapshot_date, portfolio_id, base_cash, reserve_cash,
                    total_cash, btc_balance, btc_price, portfolio_equity,
                    unrealized_pnl_usd, unrealized_pnl_pct, benchmark_equity
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                [
                    snapshot.snapshot_date,
                    snapshot.portfolio_id,
                    snapshot.base_cash,
                    snapshot.reserve_cash,
                    snapshot.total_cash,
                    snapshot.btc_balance,
                    snapshot.btc_price,
                    snapshot.portfolio_equity,
                    snapshot.unrealized_pnl_usd,
                    snapshot.unrealized_pnl_pct,
                    snapshot.benchmark_equity,
                ],
            )
        finally:
            con.close()

    def get_portfolio_summary(self, live_spot_price: float | None = None) -> PaperSummary:
        """Calculate consolidated portfolio performance metrics."""
        portfolio = self.get_portfolio_balance()

        con = self._get_connection(read_only=True)
        try:
            # 1. Spot Price Resolution
            current_spot = live_spot_price
            if current_spot is None:
                # Latest snapshot price
                latest_snap = con.execute(
                    """
                    SELECT btc_price, benchmark_equity
                    FROM paper_portfolio_snapshots_daily
                    WHERE portfolio_id = ?
                    ORDER BY snapshot_date DESC LIMIT 1;
                    """,
                    [self.portfolio_id],
                ).fetchone()
                if latest_snap:
                    current_spot = float(latest_snap[0])

            if current_spot is None:
                # Mart close fallback
                try:
                    mart_row = con.execute(
                        "SELECT close FROM mart_btc_usd_daily ORDER BY trade_date_utc DESC LIMIT 1;"
                    ).fetchone()
                    if mart_row:
                        current_spot = float(mart_row[0])
                except Exception:
                    pass

            if current_spot is None or current_spot <= 0.0:
                current_spot = 85000.0  # Fallback baseline

            # 2. Benchmark Resolution
            earliest_snap = con.execute(
                """
                SELECT btc_price
                FROM paper_portfolio_snapshots_daily
                WHERE portfolio_id = ?
                ORDER BY snapshot_date ASC LIMIT 1;
                """,
                [self.portfolio_id],
            ).fetchone()

            p_0 = float(earliest_snap[0]) if earliest_snap else current_spot
            benchmark_equity = (
                (portfolio.initial_cash / p_0) * current_spot
                if p_0 > 0.0
                else portfolio.initial_cash
            )

            # 3. Trade Metrics
            trade_stats = con.execute(
                """
                SELECT SUM(gross_amount_usd), COUNT(*)
                FROM paper_trade_ledger
                WHERE portfolio_id = ? AND side = 'BUY' AND gross_amount_usd > 0;
                """,
                [self.portfolio_id],
            ).fetchone()

            total_spent = float(trade_stats[0]) if trade_stats and trade_stats[0] else 0.0
        finally:
            con.close()

        total_btc = portfolio.btc_balance
        avg_buy_price = (total_spent / total_btc) if total_btc > 0.0 else 0.0

        if avg_buy_price > 0.0 and current_spot > 0.0:
            acquisition_discount_pct = ((current_spot - avg_buy_price) / current_spot) * 100.0
        else:
            acquisition_discount_pct = 0.0

        btc_value_usd = total_btc * current_spot
        total_equity = portfolio.total_cash + btc_value_usd
        unrealized_pnl_usd = total_equity - portfolio.initial_cash
        unrealized_pnl_pct = (
            (unrealized_pnl_usd / portfolio.initial_cash) * 100.0
            if portfolio.initial_cash > 0.0
            else 0.0
        )
        outperformance_usd = total_equity - benchmark_equity

        return PaperSummary(
            portfolio_id=self.portfolio_id,
            initial_cash=portfolio.initial_cash,
            total_equity=total_equity,
            unrealized_pnl_usd=unrealized_pnl_usd,
            unrealized_pnl_pct=unrealized_pnl_pct,
            base_cash=portfolio.base_cash,
            reserve_cash=portfolio.reserve_cash,
            total_cash=portfolio.total_cash,
            btc_balance=total_btc,
            btc_value_usd=btc_value_usd,
            avg_buy_price=avg_buy_price,
            current_spot_price=current_spot,
            acquisition_discount_pct=acquisition_discount_pct,
            total_trades=portfolio.total_trades,
            benchmark_equity=benchmark_equity,
            outperformance_usd=outperformance_usd,
        )

    def get_equity_series(self, limit: int = 90) -> list[dict[str, Any]]:
        """Retrieve daily chronological equity timeseries for charting."""
        con = self._get_connection(read_only=True)
        try:
            try:
                rows = con.execute(
                    """
                    SELECT snapshot_date, portfolio_equity, base_cash, reserve_cash,
                           (btc_balance * btc_price) AS btc_value, benchmark_equity
                    FROM paper_portfolio_snapshots_daily
                    WHERE portfolio_id = ?
                    ORDER BY snapshot_date DESC
                    LIMIT ?;
                    """,
                    [self.portfolio_id, limit],
                ).fetchall()
            except Exception:
                return []
        finally:
            con.close()

        # Reverse to chronological ASC order
        rows.reverse()
        result: list[dict[str, Any]] = []
        for r in rows:
            snap_date = r[0].isoformat() if isinstance(r[0], date) else str(r[0])
            result.append(
                {
                    "date": snap_date,
                    "equity": round(float(r[1]), 2),
                    "cash": round(float(r[2]), 2),
                    "reserve": round(float(r[3]), 2),
                    "btc_value": round(float(r[4]), 2),
                    "benchmark": round(float(r[5]), 2),
                }
            )
        return result

    def get_trade_blotter(self, limit: int = 50) -> list[dict[str, Any]]:
        """Retrieve recent executed trade records for the institutional order blotter."""
        con = self._get_connection(read_only=True)
        try:
            try:
                rows = con.execute(
                    """
                    SELECT trade_id, trade_date, side, signal_regime, spot_price,
                           gross_amount_usd, fee_usd, btc_amount, narrative
                    FROM paper_trade_ledger
                    WHERE portfolio_id = ?
                    ORDER BY executed_at_utc DESC
                    LIMIT ?;
                    """,
                    [self.portfolio_id, limit],
                ).fetchall()
            except Exception:
                return []
        finally:
            con.close()

        result: list[dict[str, Any]] = []
        for r in rows:
            t_date = r[1].isoformat() if isinstance(r[1], date) else str(r[1])
            result.append(
                {
                    "trade_id": str(r[0]),
                    "date": t_date,
                    "side": str(r[2]),
                    "signal": str(r[3]),
                    "spot_price": round(float(r[4]), 2),
                    "gross_usd": round(float(r[5]), 2),
                    "fee_usd": round(float(r[6]), 4),
                    "btc_amount": round(float(r[7]), 8),
                    "narrative": str(r[8]),
                }
            )
        return result
