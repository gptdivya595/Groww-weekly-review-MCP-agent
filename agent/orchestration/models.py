from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, Field

from agent.pulse_types import DeliveryTarget


class StageExecution(BaseModel):
    name: str
    status: Literal["completed", "skipped", "failed"]
    duration_ms: int | None = None
    detail: str | None = None


class PipelineRunResult(BaseModel):
    run_id: str
    product_slug: str
    iso_week: str
    target: DeliveryTarget
    status: Literal["completed", "failed"]
    resumed: bool = False
    stages: list[StageExecution] = Field(default_factory=list)
    summary_path: Path
    warning: str | None = None

    def to_metadata(self) -> dict[str, Any]:
        return {
            "phase": "phase-7",
            "placeholder": False,
            "orchestration_target": self.target.value,
            "orchestration_status": self.status,
            "orchestration_resumed": self.resumed,
            "orchestration_summary_path": str(self.summary_path),
            "orchestration_stages": [stage.model_dump(mode="json") for stage in self.stages],
            "warning": self.warning,
            "error": None,
        }


class WeeklyBatchItem(BaseModel):
    product_slug: str
    iso_week: str
    status: Literal["completed", "failed"]
    run_id: str | None = None
    summary_path: Path | None = None
    error: str | None = None


class WeeklyBatchResult(BaseModel):
    iso_week: str
    target: DeliveryTarget
    items: list[WeeklyBatchItem] = Field(default_factory=list)

    @property
    def failed_count(self) -> int:
        return sum(1 for item in self.items if item.status == "failed")
