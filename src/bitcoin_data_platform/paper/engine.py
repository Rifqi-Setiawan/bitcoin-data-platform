"""Forward Paper Trading Simulation Engine for systematic Bitcoin portfolio validation."""

from __future__ import annotations

import logging
import uuid
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import duckdb
import httpx

from bitcoin_data_platform.exceptions import MarketDataUnavailableError
from bitcoin_data_platform.paper.models import (
    PaperPortfolioBalance,
    PaperSnapshotRecord,
    PaperSummary,
    PaperTradeRecord,
)
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
        kill_switch_path: Path | str = "data/state/PAPER_KILL_SWITCH",
        allow_unpopulated: bool = False,
    ) -> None:
        self.db_path_str = str(db_path)
        self.db_path = Path(db_path) if self.db_path_str != ":memory:" else None
        self.portfolio_id = portfolio_id
        self.fee_bps = fee_bps
        self.risk_guard = risk_guard or RiskGuard(kill_switch_path=kill_switch_path)
        self.allow_unpopulated = allow_unpopulated

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
            """
        )

    def init_portfolio(
        self,
        initial_cash: float = 1000.0,
        overwrite: bool = False,
    ) -> PaperPortfolioBalance:
        """Initialize or retrieve the paper trading portfolio state.

        Partitions virtual capital into 70% Base Cash and 30% Tactical Reserve Cash.
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

            base_cash = round(0.70 * initial_cash, 2)
            reserve_cash = round(0.30 * initial_cash, 2)
            # Adjust rounding difference to guarantee exact initial_cash sum
            base_cash = round(initial_cash - reserve_cash, 2)
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
            return balance
        finally:
            con.close()

    def reset_portfolio(self, initial_cash: float = 1000.0) -> PaperPortfolioBalance:
        """Purge existing transactions and snapshots, re-initializing virtual capital."""
        con = self._get_connection(read_only=False)
        try:
            self._ensure_schema(con)
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

    def _query_day_signal_and_price(
        self,
        trade_date: date,
        force_spot_price: float | None = None,
    ) -> tuple[float, str, bool, str]:
        """Retrieve spot price, signal regime, macro status, and narrative for trade date."""
        con = self._get_connection(read_only=True)
        try:
            # Check if mart_btc_investment_signals_daily exists
            row = None
            try:
                row = con.execute(
                    """
                    SELECT market_close_usd, investment_signal, has_high_impact_macro_event,
                           mayer_multiple, mvrv_ratio, fng_value
                    FROM mart_btc_investment_signals_daily
                    WHERE CAST(trade_date_utc AS DATE) = ?;
                    """,
                    [trade_date],
                ).fetchone()
            except Exception:
                row = None

            # Fallback to mart_btc_usd_daily if view query returned no row
            if row is None:
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
                    return (
                        spot,
                        "STANDARD_DCA",
                        False,
                        "⚪ DCA STANDAR: Berdasarkan data harga harian.",
                    )
        finally:
            con.close()

        if row is not None:
            spot = float(row[0]) if force_spot_price is None else force_spot_price
            signal = str(row[1]) if row[1] else "STANDARD_DCA"
            macro = bool(row[2]) if row[2] is not None else False
            mm = float(row[3]) if row[3] is not None else None
            mvrv = float(row[4]) if row[4] is not None else None
            fng = int(row[5]) if row[5] is not None else 50
            narrative = generate_narrative(signal, mm, mvrv, fng)
            return spot, signal, macro, narrative

        if force_spot_price is not None:
            return (
                force_spot_price,
                "STANDARD_DCA",
                False,
                "⚪ DCA STANDAR: Eksekusi harga manual (forced spot price).",
            )

        # Analytical marts are unpopulated and no forced price provided
        if not self.allow_unpopulated:
            self.risk_guard.activate_kill_switch(
                reason=f"Market data unavailable in DuckDB for trade date {trade_date}"
            )
            raise MarketDataUnavailableError(
                f"No market data available in analytical marts for trade date {trade_date}. "
                "Trading halted fail-closed and kill switch activated."
            )

        # Fallback to live Coinbase spot ticker ONLY when allow_unpopulated=True is explicitly set
        try:
            r = httpx.get(
                "https://api.exchange.coinbase.com/products/BTC-USD/ticker",
                headers={"User-Agent": "bitcoin-data-platform/0.1.0"},
                timeout=5.0,
            )
            if r.status_code == 200:
                spot = float(r.json()["price"])
                return (
                    spot,
                    "STANDARD_DCA",
                    False,
                    f"⚪ DCA STANDAR: Eksekusi harga spot live Coinbase (${spot:,.2f}).",
                )
        except Exception as ticker_err:
            logger.warning(f"Failed fetching live Coinbase ticker: {ticker_err}")

        raise MarketDataUnavailableError(
            f"No market data or spot price available for trade date {trade_date}"
        )

    def step(
        self,
        trade_date: date,
        daily_budget: float = 10.0,
        force_spot_price: float | None = None,
    ) -> PaperTradeRecord:
        """Advance paper portfolio by executing the systematic DCA strategy for a given day."""
        if daily_budget <= 0.0:
            raise ValueError(f"daily_budget must be > 0, got {daily_budget}")

        portfolio = self.get_portfolio_balance()
        spot_price, signal_regime, has_macro, narrative = self._query_day_signal_and_price(
            trade_date=trade_date,
            force_spot_price=force_spot_price,
        )

        # 1. Determine Proposed Trade Action using DynamicReserveDCAStrategy logic
        base_slice = daily_budget
        norm_signal = signal_regime.upper().strip()

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
                f"🟢 AKUMULASI AGRESIF: Beli ${gross_amount_usd:,.2f} (2.0x base + 25% cadangan). "
                f"[Base: ${base_buy:,.2f}, Cadangan: ${reserve_draw:,.2f}]"
            )
        elif norm_signal == "OPPORTUNISTIC_ACCUMULATE":
            target_buy = 1.3 * base_slice
            gross_amount_usd = min(target_buy, portfolio.base_cash)
            base_deduction = gross_amount_usd
            side = "BUY" if gross_amount_usd > 0.0 else "HOLD"
            narrative = f"🔵 AKUMULASI OPORTUNISTIK: Beli ${gross_amount_usd:,.2f} (1.3x base)."
        else:
            # STANDARD_DCA default
            target_buy = 1.0 * base_slice
            gross_amount_usd = min(target_buy, portfolio.base_cash)
            base_deduction = gross_amount_usd
            side = "BUY" if gross_amount_usd > 0.0 else "HOLD"
            narrative = f"⚪ DCA STANDAR: Beli ${gross_amount_usd:,.2f} (1.0x base)."

        # 2. Risk Guard Verification
        risk_result = self.risk_guard.validate_trade(
            portfolio=portfolio,
            side=side,
            gross_amount_usd=gross_amount_usd,
            spot_price=spot_price,
            macro_event=has_macro,
        )

        if not risk_result.allowed:
            raise PaperEngineError(f"RiskGuard validation rejected trade: {risk_result.reason}")

        # 3. Apply Balances and Fee Deductions
        fee_rate = self.fee_bps / 10000.0  # e.g. 10.0 bps = 0.0010 (0.10%)
        fee_usd = round(gross_amount_usd * fee_rate, 4) if side == "BUY" else 0.0
        net_amount_usd = gross_amount_usd - fee_usd
        delta_btc = (net_amount_usd / spot_price) if (side == "BUY" and spot_price > 0.0) else 0.0

        portfolio.base_cash = round(
            max(0.0, portfolio.base_cash - base_deduction),
            2,
        )
        portfolio.reserve_cash = round(
            max(0.0, portfolio.reserve_cash - reserve_deduction + reserve_addition),
            2,
        )
        portfolio.btc_balance = round(portfolio.btc_balance + delta_btc, 8)
        portfolio.last_updated_utc = datetime.now(UTC)
        if side == "BUY" and gross_amount_usd > 0.0:
            portfolio.total_trades += 1

        # 4. Generate Trade Record
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

        # 5. Persist to DuckDB (atomic with snapshot)
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
                p_0 = spot_price

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
                snapshot_date=trade_date,
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

        return trade_record

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
