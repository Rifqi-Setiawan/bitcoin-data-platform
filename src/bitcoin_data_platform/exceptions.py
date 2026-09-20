"""Platform exceptions for Bitcoin Data Platform."""

from __future__ import annotations


class BitcoinDataPlatformError(Exception):
    """Base exception for all Bitcoin Data Platform runtime and data errors."""


class MarketDataUnavailableError(BitcoinDataPlatformError):
    """Raised when required market data or analytical marts are unpopulated or unavailable."""
