"""Signals package for investment signals and news sentinel."""

from bitcoin_data_platform.signals.generator import (
    InvestmentSignal,
    SignalGenerator,
    compute_signal_strength,
    generate_narrative,
)
from bitcoin_data_platform.signals.news_sentinel import (
    NewsAlert,
    NewsSentinel,
    NewsSentinelError,
    compute_alert_id,
)

__all__ = [
    "InvestmentSignal",
    "NewsAlert",
    "NewsSentinel",
    "NewsSentinelError",
    "SignalGenerator",
    "compute_alert_id",
    "compute_signal_strength",
    "generate_narrative",
]
