from __future__ import annotations

import json
from datetime import datetime
from enum import Enum
from hashlib import sha1
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from agent.ingestion.pii import scrub_review_text


def normalize_text(value: str | None) -> str | None:
    if value is None:
        return None

    normalized = value.strip()
    return normalized or None


def make_json_safe(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(key): make_json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [make_json_safe(item) for item in value]
    return value


def stable_review_id(source: str, external_id: str) -> str:
    return sha1(f"{source}:{external_id}".encode()).hexdigest()


def stable_body_hash(text: str | None) -> str | None:
    normalized = normalize_text(text)
    if normalized is None:
        return None
    return sha1(normalized.encode("utf-8")).hexdigest()


class ReviewSource(str, Enum):
    APPSTORE = "appstore"
    PLAYSTORE = "playstore"
    CSV_UPLOAD = "csv_upload"


class RawReview(BaseModel):
    model_config = ConfigDict(extra="ignore")

    source: ReviewSource
    external_id: str
    rating: int | None = None
    title: str | None = None
    body: str | None = None
    author_alias: str | None = None
    review_created_at: datetime | None = None
    review_updated_at: datetime | None = None
    locale: str | None = None
    app_version: str | None = None
    source_url: str | None = None
    raw_payload: dict[str, Any] = Field(default_factory=dict)

    @property
    def review_id(self) -> str:
        return stable_review_id(self.source.value, self.external_id)

    @property
    def pii_scrubbed_body(self) -> str | None:
        return scrub_review_text(self.body)

    @property
    def body_hash(self) -> str | None:
        return stable_body_hash(self.body)

    def as_db_record(self, product_slug: str) -> dict[str, object | None]:
        return {
            "review_id": self.review_id,
            "product_slug": product_slug,
            "source": self.source.value,
            "external_id": self.external_id,
            "rating": self.rating,
            "title": normalize_text(self.title),
            "body": normalize_text(self.body),
            "author_alias": normalize_text(self.author_alias),
            "review_created_at": self.review_created_at.isoformat()
            if self.review_created_at
            else None,
            "review_updated_at": self.review_updated_at.isoformat()
            if self.review_updated_at
            else None,
            "locale": normalize_text(self.locale),
            "app_version": normalize_text(self.app_version),
            "source_url": normalize_text(self.source_url),
            "raw_payload_json": json.dumps(make_json_safe(self.raw_payload), sort_keys=True),
            "pii_scrubbed_body": self.pii_scrubbed_body,
            "body_hash": self.body_hash,
        }

    def audit_record(self, product_slug: str) -> dict[str, Any]:
        return {
            "review_id": self.review_id,
            "product_slug": product_slug,
            "source": self.source.value,
            "external_id": self.external_id,
            "rating": self.rating,
            "title": normalize_text(self.title),
            "body": normalize_text(self.body),
            "pii_scrubbed_body": self.pii_scrubbed_body,
            "author_alias": normalize_text(self.author_alias),
            "review_created_at": self.review_created_at.isoformat()
            if self.review_created_at
            else None,
            "review_updated_at": self.review_updated_at.isoformat()
            if self.review_updated_at
            else None,
            "locale": normalize_text(self.locale),
            "app_version": normalize_text(self.app_version),
            "source_url": normalize_text(self.source_url),
            "body_hash": self.body_hash,
            "raw_payload": make_json_safe(self.raw_payload),
        }


class ReviewUpsertStats(BaseModel):
    inserted: int = 0
    updated: int = 0
    unchanged: int = 0


class SourceIngestionReport(BaseModel):
    source: ReviewSource
    status: str
    fetched: int = 0
    error: str | None = None


class IngestionResult(BaseModel):
    run_id: str
    product_slug: str
    iso_week: str
    lookback_weeks: int
    total_reviews: int
    snapshot_path: Path
    degraded: bool = False
    upsert: ReviewUpsertStats
    sources: list[SourceIngestionReport] = Field(default_factory=list)

    def to_metadata(self) -> dict[str, Any]:
        return {
            "phase": "phase-1",
            "placeholder": False,
            "product_slug": self.product_slug,
            "iso_week": self.iso_week,
            "lookback_weeks": self.lookback_weeks,
            "snapshot_path": str(self.snapshot_path),
            "total_reviews": self.total_reviews,
            "degraded": self.degraded,
            "upsert": self.upsert.model_dump(mode="json"),
            "source_breakdown": {
                report.source.value: report.model_dump(mode="json") for report in self.sources
            },
        }
