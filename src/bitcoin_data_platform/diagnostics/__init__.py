"""Phase 11: Automated Operational Diagnostics and Bounded Self-Healing framework."""

from bitcoin_data_platform.diagnostics.cli import (
    handle_diagnostics_cli,
    register_diagnostics_cli,
)
from bitcoin_data_platform.diagnostics.collector import TelemetryCollector
from bitcoin_data_platform.diagnostics.healer import RemediationRunner
from bitcoin_data_platform.diagnostics.models import (
    IncidentRecord,
    IncidentReport,
    RemediationPlan,
    RemediationResult,
    TelemetryBundle,
)
from bitcoin_data_platform.diagnostics.report import IncidentReportGenerator
from bitcoin_data_platform.diagnostics.triage import TriageEngine

__all__ = [
    "IncidentRecord",
    "TelemetryBundle",
    "RemediationPlan",
    "RemediationResult",
    "IncidentReport",
    "TelemetryCollector",
    "TriageEngine",
    "RemediationRunner",
    "IncidentReportGenerator",
    "handle_diagnostics_cli",
    "register_diagnostics_cli",
]
