"""Telegram Bot API dispatcher for investment signals and emergency alerts."""

import json
import logging
import os
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from typing import Any

from bitcoin_data_platform.signals.generator import InvestmentSignal
from bitcoin_data_platform.signals.news_sentinel import NewsAlert

logger = logging.getLogger(__name__)

SIGNAL_EMOJIS: dict[str, str] = {
    "AGGRESSIVE_ACCUMULATE": "🟢",
    "OPPORTUNISTIC_ACCUMULATE": "🔵",
    "STANDARD_DCA": "⚪",
    "DEFENSIVE_RESERVE": "🟡",
    "HARD_FREEZE": "🔴",
}

SIGNAL_DISPLAY_NAMES: dict[str, str] = {
    "AGGRESSIVE_ACCUMULATE": "AKUMULASI AGRESIF",
    "OPPORTUNISTIC_ACCUMULATE": "AKUMULASI OPORTUNISTIK",
    "STANDARD_DCA": "DCA STANDAR",
    "DEFENSIVE_RESERVE": "CADANGAN DEFENSIF",
    "HARD_FREEZE": "STOP TOTAL",
}


def format_signal_message(signal: InvestmentSignal) -> str:
    """Format an investment signal into Telegram Markdown message in Bahasa Indonesia."""
    emoji = SIGNAL_EMOJIS.get(signal.investment_signal, "⚪")
    signal_name = SIGNAL_DISPLAY_NAMES.get(signal.investment_signal, signal.investment_signal)

    mm_str = f"{signal.mayer_multiple:.2f}" if signal.mayer_multiple is not None else "N/A"
    mvrv_str = f"{signal.mvrv_ratio:.2f}" if signal.mvrv_ratio is not None else "N/A"
    macro_str = "Ya" if signal.has_high_impact_macro_event else "Tidak"

    lines = [
        "📊 *SINYAL INVESTASI BITCOIN*",
        f"📅 {signal.signal_date_utc.isoformat()}",
        "",
        f"💰 Harga: ${signal.market_close_usd:,.2f}",
        f"📈 Mayer Multiple: {mm_str}",
        f"🔗 MVRV: {mvrv_str}",
        f"😱 Fear & Greed: {signal.fng_value} ({signal.fng_classification})",
        f"🗓️ Macro Event: {macro_str}",
        "",
        f"{emoji} *{signal_name}*",
        f"_{signal.narrative}_",
        "",
        f"Kekuatan: {signal.signal_strength}",
    ]
    return "\n".join(lines)


def format_news_alert_message(alert: NewsAlert) -> str:
    """Format a news sentinel alert into Telegram Markdown message."""
    keywords_str = (
        ", ".join(alert.matched_keywords)
        if isinstance(alert.matched_keywords, list | tuple | set)
        else str(alert.matched_keywords)
    )
    pub_str = alert.published_utc.strftime("%Y-%m-%d %H:%M UTC")

    lines = [
        f"🚨 *ALERT {alert.severity}: {alert.title}*",
        "",
        f"🔗 {alert.link}",
        f"📅 {pub_str}",
        f"🔍 Keywords: {keywords_str}",
        "",
        "⚠️ _Review sebelum mengambil keputusan investasi._",
    ]
    return "\n".join(lines)


class TelegramDispatcher:
    """Dispatches formatted messages to Telegram chat via Bot API."""

    def __init__(
        self,
        *,
        bot_token: str | None = None,
        chat_id: str | None = None,
        base_url: str = "https://api.telegram.org",
        dry_run: bool = False,
        max_retries: int = 3,
        base_backoff_seconds: float = 0.5,
        transport: Any = None,
        urlopen: Callable[..., Any] | None = None,
        sleeper: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.bot_token = bot_token or os.environ.get("TELEGRAM_BOT_TOKEN", "")
        self.chat_id = chat_id or os.environ.get("TELEGRAM_CHAT_ID", "")
        self.base_url = base_url.rstrip("/")
        self.dry_run = dry_run
        self.max_retries = max_retries
        self.base_backoff_seconds = base_backoff_seconds
        self._transport = transport
        self._urlopen = urlopen
        self._sleeper = sleeper
        self._clock = clock

    def _redact_token(self, msg: str) -> str:
        """Sanitize error messages and URLs so the bot token is never exposed."""
        if self.bot_token and self.bot_token in msg:
            return msg.replace(self.bot_token, "[REDACTED]")
        return msg

    def send_text(self, text: str, parse_mode: str = "Markdown") -> bool:
        """Send raw text message to Telegram chat with retry handling."""
        if self.dry_run:
            print(text)
            return True

        if not self.bot_token or not self.chat_id:
            logger.error("Cannot send Telegram message: bot_token or chat_id is missing")
            return False

        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": parse_mode,
        }
        data = json.dumps(payload).encode("utf-8")
        url = f"{self.base_url}/bot{self.bot_token}/sendMessage"

        headers = {
            "Content-Type": "application/json",
            "User-Agent": "bitcoin-data-platform/0.1.0",
        }
        req = urllib.request.Request(url, data=data, headers=headers)

        attempt = 0
        while attempt <= self.max_retries:
            attempt += 1
            try:
                if self._transport is not None:
                    if callable(self._transport):
                        resp = self._transport(req)
                    elif hasattr(self._transport, "open"):
                        resp = self._transport.open(req)
                    else:
                        resp = self._transport.send(req)
                elif self._urlopen is not None:
                    resp = self._urlopen(req)
                else:
                    resp = urllib.request.urlopen(req, timeout=15)

                status_code = getattr(resp, "status", getattr(resp, "code", 200))
                if 200 <= status_code < 300:
                    return True
                elif status_code in (429, 500, 502, 503, 504):
                    if attempt <= self.max_retries:
                        delay = self.base_backoff_seconds * (2 ** (attempt - 1))
                        self._sleeper(delay)
                        continue
                    logger.error(
                        f"Telegram API returned transient status {status_code} after retries"
                    )
                    return False
                else:
                    logger.error(f"Telegram API request failed with status {status_code}")
                    return False

            except urllib.error.HTTPError as exc:
                if exc.code in (429, 500, 502, 503, 504):
                    if attempt <= self.max_retries:
                        delay = self.base_backoff_seconds * (2 ** (attempt - 1))
                        self._sleeper(delay)
                        continue
                    logger.error(f"Telegram HTTP error {exc.code} after retries")
                    return False
                else:
                    # Client errors (e.g. 400 Bad Request, 401 Unauthorized, 404 Not Found)
                    safe_reason = self._redact_token(str(exc.reason))
                    logger.error(f"Telegram HTTP error {exc.code}: {safe_reason}")
                    return False

            except (urllib.error.URLError, TimeoutError, OSError) as exc:
                if attempt <= self.max_retries:
                    delay = self.base_backoff_seconds * (2 ** (attempt - 1))
                    self._sleeper(delay)
                    continue
                safe_err = self._redact_token(str(exc))
                logger.error(f"Telegram network failure after retries: {safe_err}")
                return False

            except Exception as exc:
                safe_err = self._redact_token(str(exc))
                logger.error(f"Unexpected error while sending Telegram message: {safe_err}")
                return False

        return False

    def send_signal(self, signal: InvestmentSignal) -> bool:
        """Format and dispatch an investment signal message."""
        text = format_signal_message(signal)
        return self.send_text(text, parse_mode="Markdown")

    def send_news_alert(self, alert: NewsAlert) -> bool:
        """Format and dispatch a news sentinel alert message."""
        text = format_news_alert_message(alert)
        return self.send_text(text, parse_mode="Markdown")
