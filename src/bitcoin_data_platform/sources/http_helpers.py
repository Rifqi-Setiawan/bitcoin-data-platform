"""Shared HTTP client rate limiting and retry backoff helpers."""

import random
from collections.abc import Callable

import httpx


def apply_rate_limit(
    last_request_started_at: float | None,
    min_interval: float,
    clock: Callable[[], float],
    sleeper: Callable[[float], None],
) -> float:
    """Enforce minimum spacing between outbound requests and return start timestamp."""
    if last_request_started_at is not None:
        elapsed = clock() - last_request_started_at
        if elapsed < min_interval:
            sleeper(min_interval - elapsed)
    return clock()


def compute_backoff_delay(
    attempt: int,
    response: httpx.Response | None,
    base_backoff: float,
    max_backoff: float,
    jitter: bool = True,
) -> float:
    """Calculate backoff duration, respecting Retry-After header when present."""
    if response is not None:
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                delay = float(retry_after.strip())
                if delay >= 0:
                    return delay
            except ValueError:
                pass

    backoff = base_backoff * (2**attempt)
    if jitter:
        backoff += random.uniform(0.0, 0.5)
    return float(min(backoff, max_backoff))
