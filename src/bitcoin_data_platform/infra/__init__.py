"""Infrastructure, systemd integration, and failure alert handling."""

from bitcoin_data_platform.infra.alert_handler import (
    dispatch_failure_alert,
    scrub_message,
)

__all__ = [
    "dispatch_failure_alert",
    "scrub_message",
]
