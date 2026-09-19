"""Data contracts and domain models for automated scheduling pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any


class PipelineCadence(StrEnum):
    """Execution cadences for automated pipeline tiers."""

    HOURLY = "hourly"
    DAILY = "daily"
    WEEKLY = "weekly"


class JobStatus(StrEnum):
    """Execution status for jobs and pipeline steps."""

    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"
    LOCKED = "LOCKED"


@dataclass(frozen=True)
class JobStepResult:
    """Execution result of an individual pipeline step."""

    step_name: str
    status: JobStatus
    duration_seconds: float
    error_message: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PipelineRunReport:
    """Comprehensive execution report of a pipeline cadence run."""

    run_id: str
    cadence: PipelineCadence
    started_at_utc: datetime
    completed_at_utc: datetime
    overall_status: JobStatus
    steps: list[JobStepResult]
    total_duration_seconds: float

    def to_dict(self) -> dict[str, Any]:
        """Convert run report to dictionary representation."""
        return {
            "run_id": self.run_id,
            "cadence": self.cadence.value,
            "started_at_utc": self.started_at_utc.isoformat(),
            "completed_at_utc": self.completed_at_utc.isoformat(),
            "overall_status": self.overall_status.value,
            "total_duration_seconds": self.total_duration_seconds,
            "steps": [
                {
                    "step_name": s.step_name,
                    "status": s.status.value,
                    "duration_seconds": s.duration_seconds,
                    "error_message": s.error_message,
                    "metadata": s.metadata,
                }
                for s in self.steps
            ],
        }
