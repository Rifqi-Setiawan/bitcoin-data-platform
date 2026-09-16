"""Bitcoin Data Engineering Platform."""

from bitcoin_data_platform.ingestion.window_planner import (
    PlannedWindow,
    plan_windows,
)

__version__ = "0.1.0"

__all__ = [
    "PlannedWindow",
    "__version__",
    "plan_windows",
]
