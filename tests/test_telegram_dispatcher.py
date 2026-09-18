"""Tests for TelegramDispatcher, message formatting, dry-run, retries, and failure modes."""

import urllib.error
from datetime import UTC, date, datetime
from typing import Any
from unittest.mock import MagicMock

import pytest

from bitcoin_data_platform.alerts.telegram_dispatcher import (
    TelegramDispatcher,
    format_news_alert_message,
    format_signal_message,
)
from bitcoin_data_platform.signals.generator import InvestmentSignal
from bitcoin_data_platform.signals.news_sentinel import NewsAlert


def _sample_signal(
    signal_type: str = "AGGRESSIVE_ACCUMULATE",
    mayer: float = 0.75,
    mvrv: float = 0.95,
    fng: int = 20,
    fng_class: str = "Extreme Fear",
    macro: bool = True,
    strength: str = "STRONG",
    narrative: str = "🟢 AKUMULASI AGRESIF: Bitcoin di bawah nilai wajar. Peluang beli terbaik.",
) -> InvestmentSignal:
    return InvestmentSignal(
        signal_date_utc=date(2026, 9, 18),
        generated_at_utc=datetime(2026, 9, 18, 12, 0, tzinfo=UTC),
        market_close_usd=65432.10,
        sma_200=70000.0,
        mayer_multiple=mayer,
        mvrv_ratio=mvrv,
        fng_value=fng,
        fng_classification=fng_class,
        has_high_impact_macro_event=macro,
        investment_signal=signal_type,
        signal_strength=strength,
        narrative=narrative,
    )


def _sample_news_alert(
    severity: str = "CRITICAL",
    title: str = "Bridge Protocol Hacked for $50M",
    keywords: list[str] | None = None,
) -> NewsAlert:
    return NewsAlert(
        alert_id="test-alert-id-123",
        title=title,
        link="https://www.coindesk.com/hack-bridge",
        published_utc=datetime(2026, 9, 18, 14, 30, tzinfo=UTC),
        matched_keywords=keywords or ["hack", "exploit"],
        severity=severity,
        ingested_at_utc=datetime(2026, 9, 18, 14, 35, tzinfo=UTC),
    )


def test_send_signal_formats_correctly() -> None:
    """19. test_send_signal_formats_correctly: message contains all KPI fields."""
    signal = _sample_signal()
    msg = format_signal_message(signal)

    assert "SINYAL INVESTASI BITCOIN" in msg
    assert "2026-09-18" in msg
    assert "Harga: $65,432.10" in msg
    assert "Mayer Multiple: 0.75" in msg
    assert "MVRV: 0.95" in msg
    assert "Fear & Greed: 20 (Extreme Fear)" in msg
    assert "Macro Event: Ya" in msg
    assert "AKUMULASI AGRESIF" in msg
    assert "Kekuatan: STRONG" in msg
    assert signal.narrative in msg


def test_send_news_alert_formats_correctly() -> None:
    """20. test_send_news_alert_formats_correctly: message contains severity + keywords."""
    alert = _sample_news_alert(severity="CRITICAL", keywords=["hack", "exploit"])
    msg = format_news_alert_message(alert)

    assert "ALERT CRITICAL: Bridge Protocol Hacked for $50M" in msg
    assert "https://www.coindesk.com/hack-bridge" in msg
    assert "2026-09-18 14:30 UTC" in msg
    assert "Keywords: hack, exploit" in msg
    assert "Review sebelum mengambil keputusan investasi." in msg


def test_send_dry_run_does_not_call_api(capsys: pytest.CaptureFixture[str]) -> None:
    """21. test_send_dry_run_does_not_call_api: dry run prints only and does not call network."""
    mock_urlopen = MagicMock()
    dispatcher = TelegramDispatcher(
        bot_token="fake_token",
        chat_id="fake_chat",
        dry_run=True,
        urlopen=mock_urlopen,
    )

    signal = _sample_signal()
    success = dispatcher.send_signal(signal)

    assert success is True
    assert mock_urlopen.call_count == 0

    captured = capsys.readouterr()
    assert "SINYAL INVESTASI BITCOIN" in captured.out
    assert "AKUMULASI AGRESIF" in captured.out


def test_send_retries_on_transient_error() -> None:
    """22. test_send_retries_on_transient_error: retry on 500/503 HTTP status."""
    call_count = 0
    sleep_records: list[float] = []

    def mock_urlopen(req: Any, timeout: float = 15) -> Any:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise urllib.error.HTTPError(
                url="https://api.telegram.org",
                code=503,
                msg="Service Unavailable",
                hdrs={},  # type: ignore[arg-type]
                fp=None,
            )
        # 2nd call succeeds
        resp = MagicMock()
        resp.status = 200
        return resp

    dispatcher = TelegramDispatcher(
        bot_token="test_token",
        chat_id="test_chat",
        dry_run=False,
        urlopen=mock_urlopen,
        sleeper=sleep_records.append,
        max_retries=3,
    )

    signal = _sample_signal()
    success = dispatcher.send_signal(signal)

    assert success is True
    assert call_count == 2
    assert len(sleep_records) == 1


def test_send_fails_gracefully_on_bad_token() -> None:
    """23. test_send_fails_gracefully_on_bad_token: returns False without crashing."""
    secret_token = "123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11"

    def mock_urlopen(req: Any, timeout: float = 15) -> Any:
        raise urllib.error.HTTPError(
            url=f"https://api.telegram.org/bot{secret_token}/sendMessage",
            code=401,
            msg="Unauthorized: invalid token",
            hdrs={},  # type: ignore[arg-type]
            fp=None,
        )

    dispatcher = TelegramDispatcher(
        bot_token=secret_token,
        chat_id="test_chat",
        dry_run=False,
        urlopen=mock_urlopen,
        max_retries=3,
    )

    signal = _sample_signal()
    success = dispatcher.send_signal(signal)

    assert success is False
    # Verify token is redacted in helper
    assert secret_token not in dispatcher._redact_token(f"Error for {secret_token}")
    assert "[REDACTED]" in dispatcher._redact_token(f"Error for {secret_token}")


def test_send_signal_bahasa_indonesia() -> None:
    """24. test_send_signal_bahasa_indonesia: narrative and labels in Bahasa Indonesia."""
    signal = _sample_signal(
        signal_type="OPPORTUNISTIC_ACCUMULATE",
        narrative=(
            "🔵 AKUMULASI OPORTUNISTIK: Bitcoin di zona diskon (Mayer 0.90). "
            "Beli lebih dari normal."
        ),
        macro=False,
    )
    msg = format_signal_message(signal)

    assert "SINYAL INVESTASI BITCOIN" in msg
    assert "Harga:" in msg
    assert "Macro Event: Tidak" in msg
    assert "AKUMULASI OPORTUNISTIK" in msg
    assert "Kekuatan:" in msg
    assert "Beli lebih dari normal" in msg
