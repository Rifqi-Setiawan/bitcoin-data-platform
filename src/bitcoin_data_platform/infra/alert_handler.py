"""Low-noise failure alert handler with sanitization and stateful deduplication."""

import json
import os
import re
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from bitcoin_data_platform.time_range import format_canonical_utc, parse_iso_utc

DEFAULT_ALERT_STATE_PATH = Path("/srv/data/bitcoin-data-platform/state/alert_state.json")
DEFAULT_THROTTLE_WINDOW_SECONDS = 7200  # 2 hours


def scrub_message(msg: str) -> str:
    """Scrub sensitive credentials, tokens, and internal paths from message text."""
    if not msg:
        return ""

    scrubbed = msg

    # Redact URL credentials: http(s)://user:pass@host -> http(s)://[REDACTED]@host
    scrubbed = re.sub(r"(https?://)([^:]+):([^@]+)@", r"\1[REDACTED]@", scrubbed)

    # Redact common credential patterns (api_key=xyz, token: abc, bearer xyz, password=xyz)
    scrubbed = re.sub(
        r"(?i)(api[_-]?key|token|secret|password|bearer|authorization)\s*([:=])\s*([^\s,;]+)",
        r"\1\2[REDACTED]",
        scrubbed,
    )
    scrubbed = re.sub(r"(?i)\bbearer\s+([a-zA-Z0-9_\-\.]+)", r"Bearer [REDACTED]", scrubbed)

    # Scrub private home directories and internal user profiles
    scrubbed = re.sub(r"/home/[a-zA-Z0-9_\-]+(/|$)", r"/home/[USER]\1", scrubbed)
    scrubbed = re.sub(
        r"/srv/apps/[a-zA-Z0-9_\-]+/profiles/[a-zA-Z0-9_\-]+", r"/srv/apps/[APP]", scrubbed
    )

    return scrubbed


def _load_alert_state(state_file: Path) -> dict[str, Any]:
    """Load alert state from JSON file or return empty structure."""
    if not state_file.exists():
        return {"units": {}}
    try:
        with open(state_file, encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, dict) and "units" in data and isinstance(data["units"], dict):
                return data
            return {"units": {}}
    except Exception:
        return {"units": {}}


def _save_alert_state(state_file: Path, state: dict[str, Any]) -> None:
    """Save alert state atomically using a temporary file."""
    state_file.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = state_file.with_suffix(".tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)
    os.replace(tmp_path, state_file)


def dispatch_failure_alert(
    failed_unit: str,
    *,
    state_file: Path | str | None = None,
    message: str | None = None,
    throttle_window_seconds: int = DEFAULT_THROTTLE_WINDOW_SECONDS,
    now_utc: datetime | None = None,
    output_stream: Any = None,
) -> dict[str, Any]:
    """Process failure alert for a failed systemd unit with deduplication and sanitization.

    Suppresses alerts if the same unit failed with the same message within
    throttle_window_seconds (default 2 hours). Emits structured JSON to
    output_stream (default stderr).
    """
    eval_now = now_utc if now_utc is not None else datetime.now(UTC)
    eval_now = eval_now.replace(tzinfo=UTC) if eval_now.tzinfo is None else eval_now.astimezone(UTC)

    target_state_path = Path(state_file) if state_file is not None else DEFAULT_ALERT_STATE_PATH
    raw_msg = message if message is not None else f"Unit {failed_unit} reported execution failure."
    clean_msg = scrub_message(raw_msg)

    state = _load_alert_state(target_state_path)
    unit_entry = state["units"].get(failed_unit)

    throttled = False
    consecutive_count = 1

    if unit_entry is not None:
        last_alert_str = unit_entry.get("last_alert_at_utc")
        last_msg = unit_entry.get("last_message")
        consecutive_count = unit_entry.get("consecutive_failures", 1) + 1

        if last_alert_str is not None:
            try:
                last_dt = parse_iso_utc(last_alert_str, "last_alert_at_utc")
                elapsed = (eval_now - last_dt).total_seconds()
                if elapsed < throttle_window_seconds and last_msg == clean_msg:
                    throttled = True
            except Exception:
                throttled = False

    alert_payload = {
        "event": "service_failure_alert",
        "failed_unit": failed_unit,
        "timestamp_utc": format_canonical_utc(eval_now),
        "severity": "CRITICAL",
        "status": "FAILED",
        "message": clean_msg,
        "throttled": throttled,
        "consecutive_failures": consecutive_count,
    }

    if not throttled:
        state["units"][failed_unit] = {
            "last_alert_at_utc": format_canonical_utc(eval_now),
            "last_message": clean_msg,
            "consecutive_failures": consecutive_count,
        }
        try:
            _save_alert_state(target_state_path, state)
        except Exception as exc:
            alert_payload["state_save_error"] = str(exc)

    stream = output_stream if output_stream is not None else sys.stderr
    stream.write(json.dumps(alert_payload) + "\n")

    return alert_payload
