"""Backtest simulation engine for deterministic chronological historical backtesting."""

from __future__ import annotations

import logging
from datetime import date, datetime
from pathlib import Path

import duckdb

from bitcoin_data_platform.backtest.metrics import compute_strategy_result
from bitcoin_data_platform.backtest.models import (
    BacktestConfig,
    BacktestDayRecord,
    BenchmarkSummary,
    DailyPortfolioState,
    FrequencyType,
    StrategyResult,
    StrategyType,
)
from bitcoin_data_platform.backtest.strategies import (
    BaseStrategy,
    BlindDCAStrategy,
    DynamicReserveDCAStrategy,
    LumpSumStrategy,
)

logger = logging.getLogger(__name__)


class BacktestEngine:
    """Institutional-grade backtesting engine executing event-driven strategies."""

    def load_data_from_duckdb(
        self,
        db_path: Path,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> list[BacktestDayRecord]:
        """Load chronological historical market and signal records from DuckDB.

        Queries the conformed analytical view mart_btc_investment_signals_daily.

        Args:
            db_path: Path to the platform DuckDB database file.
            start_date: Optional inclusive start date.
            end_date: Optional inclusive end date.

        Returns:
            List of BacktestDayRecord ordered chronologically ascending.
        """
        if not db_path.exists():
            raise FileNotFoundError(f"Database file does not exist: {db_path}")

        con = duckdb.connect(str(db_path), read_only=True)
        try:
            # Check view existence
            sql_check = (
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_name = 'mart_btc_investment_signals_daily'"
            )
            views = [row[0] for row in con.execute(sql_check).fetchall()]
            if not views:
                raise ValueError(
                    "View mart_btc_investment_signals_daily not found in database. "
                    "Ensure analytical views are created before running backtests."
                )

            sql = """
            SELECT
                trade_date_utc,
                market_close_usd,
                sma_200,
                mayer_multiple,
                mvrv_ratio,
                fng_value,
                has_high_impact_macro_event,
                investment_signal
            FROM mart_btc_investment_signals_daily
            WHERE (? IS NULL OR CAST(trade_date_utc AS DATE) >= ?)
              AND (? IS NULL OR CAST(trade_date_utc AS DATE) <= ?)
            ORDER BY CAST(trade_date_utc AS DATE) ASC
            """

            start_str = start_date.isoformat() if start_date else None
            end_str = end_date.isoformat() if end_date else None

            rows = con.execute(sql, [start_str, start_str, end_str, end_str]).fetchall()

            records: list[BacktestDayRecord] = []
            for row in rows:
                raw_date = row[0]
                if isinstance(raw_date, datetime):
                    trade_d = raw_date.date()
                elif isinstance(raw_date, date):
                    trade_d = raw_date
                else:
                    trade_d = date.fromisoformat(str(raw_date)[:10])

                close_price = float(row[1]) if row[1] is not None else 0.0
                if close_price <= 0.0:
                    continue  # Skip corrupt/zero close days

                sma200 = float(row[2]) if row[2] is not None else None
                mm = float(row[3]) if row[3] is not None else None
                mvrv = float(row[4]) if row[4] is not None else None
                fng = int(row[5]) if row[5] is not None else None
                macro_event = bool(row[6]) if row[6] is not None else False
                signal = str(row[7]) if row[7] is not None else "STANDARD_DCA"

                records.append(
                    BacktestDayRecord(
                        trade_date=trade_d,
                        market_close_usd=close_price,
                        sma_200=sma200,
                        mayer_multiple=mm,
                        mvrv_ratio=mvrv,
                        fng_value=fng,
                        has_high_impact_macro_event=macro_event,
                        investment_signal=signal,
                    )
                )

            return records

        finally:
            con.close()

    def run_strategy(
        self,
        records: list[BacktestDayRecord],
        strategy_type: StrategyType,
        config: BacktestConfig,
    ) -> StrategyResult:
        """Run single strategy event loop chronologically across historical records.

        Args:
            records: Sorted historical market and signal day records.
            strategy_type: Strategy implementation to evaluate.
            config: Backtest simulation configuration.

        Returns:
            StrategyResult with full metrics, accounting, and daily states.
        """
        if not records:
            return StrategyResult(strategy_type=strategy_type, config=config)

        # Ensure chronological ordering
        sorted_records = sorted(records, key=lambda r: r.trade_date)

        # Strategy instantiation
        strategy: BaseStrategy
        if strategy_type == StrategyType.LUMP_SUM:
            strategy = LumpSumStrategy(config)
        elif strategy_type == StrategyType.BLIND_DCA:
            strategy = BlindDCAStrategy(config)
        elif strategy_type == StrategyType.DYNAMIC_RESERVE:
            strategy = DynamicReserveDCAStrategy(config)
        else:
            raise ValueError(f"Unsupported strategy type: {strategy_type}")

        # Account balances
        cash_balance: float = 0.0
        reserve_cash_balance: float = 0.0
        btc_balance: float = 0.0
        total_contributed: float = 0.0

        peak_equity: float = 0.0
        daily_states: list[DailyPortfolioState] = []
        buys: list[tuple[float, float]] = []
        market_prices: list[float] = [r.market_close_usd for r in sorted_records]

        for idx, day in enumerate(sorted_records):
            # 1. Capital injection scheduling
            cash_flow = 0.0
            is_injection_day = False

            if strategy_type == StrategyType.LUMP_SUM:
                if idx == 0:
                    cash_flow = config.initial_cash
                    is_injection_day = True
                    total_contributed += cash_flow
                    cash_balance += cash_flow
            else:
                is_injection_day = (
                    True if config.frequency == FrequencyType.DAILY else (idx % 7 == 0)
                )

                if is_injection_day:
                    cash_flow = config.periodic_amount
                    total_contributed += cash_flow
                    cash_balance += cash_flow

            # 2. Strategy evaluation snapshot
            equity_before_step = (
                cash_balance + reserve_cash_balance + btc_balance * day.market_close_usd
            )
            snapshot = DailyPortfolioState(
                trade_date=day.trade_date,
                cash_balance=cash_balance,
                reserve_cash_balance=reserve_cash_balance,
                btc_balance=btc_balance,
                btc_price=day.market_close_usd,
                portfolio_equity=equity_before_step,
                total_contributed=total_contributed,
                daily_cash_flow=cash_flow,
                daily_return=0.0,
                drawdown=0.0,
                action_taken="",
            )

            # 3. Strategy decision
            buy_amount_usd, reserve_add_usd, description = strategy.step(
                day=day,
                state=snapshot,
                is_injection_day=is_injection_day,
            )

            # 4. Tactical reserve pool transfer
            if reserve_add_usd != 0.0:
                cash_balance -= reserve_add_usd
                reserve_cash_balance += reserve_add_usd

            # 5. Order execution & fee deduction
            actual_buy_usd = min(buy_amount_usd, max(0.0, cash_balance))
            fee_usd = actual_buy_usd * (config.fee_bps / 10000.0)
            net_buy_usd = max(0.0, actual_buy_usd - fee_usd)

            delta_btc = net_buy_usd / day.market_close_usd if day.market_close_usd > 0.0 else 0.0

            cash_balance -= actual_buy_usd
            btc_balance += delta_btc

            if actual_buy_usd > 0.0:
                buys.append((actual_buy_usd, delta_btc))

            # 6. End-of-day mark-to-market valuation
            portfolio_equity = (
                cash_balance + reserve_cash_balance + btc_balance * day.market_close_usd
            )

            # 7. Daily return (time-weighted formula)
            if idx == 0:
                daily_return = 0.0
            else:
                prev_equity = daily_states[-1].portfolio_equity
                if prev_equity > 0.0:
                    daily_return = (portfolio_equity - cash_flow - prev_equity) / prev_equity
                else:
                    daily_return = 0.0

            # 8. Drawdown calculation
            if portfolio_equity > peak_equity:
                peak_equity = portfolio_equity

            drawdown = (portfolio_equity - peak_equity) / peak_equity if peak_equity > 0.0 else 0.0

            daily_states.append(
                DailyPortfolioState(
                    trade_date=day.trade_date,
                    cash_balance=round(cash_balance, 4),
                    reserve_cash_balance=round(reserve_cash_balance, 4),
                    btc_balance=btc_balance,
                    btc_price=day.market_close_usd,
                    portfolio_equity=portfolio_equity,
                    total_contributed=total_contributed,
                    daily_cash_flow=cash_flow,
                    daily_return=daily_return,
                    drawdown=drawdown,
                    action_taken=description,
                )
            )

        return compute_strategy_result(
            strategy_type=strategy_type,
            config=config,
            daily_states=daily_states,
            buys=buys,
            market_prices=market_prices,
        )

    def run_benchmark(
        self,
        records: list[BacktestDayRecord],
        config: BacktestConfig,
    ) -> BenchmarkSummary:
        """Execute all standard strategies across the identical dataset and timeline.

        Benchmarks:
          1. Lump Sum Buy & Hold
          2. Blind DCA
          3. Dynamic Reserve DCA

        Args:
            records: Historical simulation data records.
            config: Common simulation configuration.

        Returns:
            BenchmarkSummary containing comparative results.
        """
        if not records:
            today = date.today()
            return BenchmarkSummary(
                start_date=today,
                end_date=today,
                duration_days=0,
                results={},
            )

        sorted_records = sorted(records, key=lambda r: r.trade_date)
        start_date = sorted_records[0].trade_date
        end_date = sorted_records[-1].trade_date
        duration_days = (end_date - start_date).days + 1

        results: dict[StrategyType, StrategyResult] = {}
        for strat_type in [
            StrategyType.LUMP_SUM,
            StrategyType.BLIND_DCA,
            StrategyType.DYNAMIC_RESERVE,
        ]:
            results[strat_type] = self.run_strategy(
                records=sorted_records,
                strategy_type=strat_type,
                config=config,
            )

        return BenchmarkSummary(
            start_date=start_date,
            end_date=end_date,
            duration_days=duration_days,
            results=results,
        )
