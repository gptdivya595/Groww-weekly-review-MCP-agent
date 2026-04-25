from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class Stage(str, Enum):
    INGEST = "ingest"
    CLUSTER = "cluster"
    SUMMARIZE = "summarize"
    RENDER = "render"
    PUBLISH = "publish"
    RUN = "run"


class DeliveryTarget(str, Enum):
    DOCS = "docs"
    GMAIL = "gmail"
    ALL = "all"


class ProductConfig(BaseModel):
    slug: str
    display_name: str
    app_store_app_id: str | None = None
    google_play_package: str | None = None
    google_doc_id: str | None = None
    stakeholder_emails: list[str] = Field(default_factory=list)
    default_lookback_weeks: int = 10
    country: str = "in"
    lang: str = "en"
    active: bool = True


class RunWindow(BaseModel):
    iso_week: str
    week_start: datetime
    week_end: datetime
    lookback_start: datetime
    lookback_weeks: int


class RunRecord(BaseModel):
    run_id: str
    product_slug: str
    stage: Stage
    status: str
    iso_week: str
    lookback_weeks: int
    started_at: datetime
    week_start: datetime
    week_end: datetime
    lookback_start: datetime


class StoredRunRecord(BaseModel):
    run_id: str
    product_slug: str
    stage: str
    status: str
    iso_week: str
    lookback_weeks: int
    started_at: datetime
    completed_at: datetime | None = None
    week_start: datetime
    week_end: datetime
    lookback_start: datetime
    metadata: dict[str, Any] = Field(default_factory=dict)


class StoredDeliveryRecord(BaseModel):
    delivery_id: str
    run_id: str
    target: str
    status: str
    external_id: str | None = None
    external_link: str | None = None
    payload_hash: str | None = None
    created_at: datetime
    updated_at: datetime
