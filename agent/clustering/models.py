from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field


class ReviewDocument(BaseModel):
    review_id: str
    product_slug: str
    source: str
    rating: int | None = None
    title: str | None = None
    body: str | None = None
    pii_scrubbed_body: str | None = None
    locale: str | None = None
    body_hash: str | None = None
    review_created_at: datetime | None = None
    review_updated_at: datetime | None = None

    @property
    def text(self) -> str:
        return (self.pii_scrubbed_body or self.body or "").strip()


class FilteredReviewSet(BaseModel):
    eligible_reviews: list[ReviewDocument] = Field(default_factory=list)
    filtered_language: int = 0
    filtered_too_short: int = 0
    filtered_duplicate_body: int = 0


class EmbeddingStats(BaseModel):
    cache_hits: int = 0
    cache_misses: int = 0


class ClusterArtifact(BaseModel):
    cluster_id: str
    run_id: str
    label: int
    size: int
    review_ids: list[str] = Field(default_factory=list)
    representative_review_ids: list[str] = Field(default_factory=list)
    keyphrases: list[str] = Field(default_factory=list)
    medoid_review_id: str
    average_rating: float | None = None
    rating_stddev: float | None = None


class ClusteringResult(BaseModel):
    run_id: str
    product_slug: str
    iso_week: str
    embedding_provider: str
    embedding_model: str
    total_reviews_window: int
    eligible_reviews: int
    filtered_language: int
    filtered_too_short: int
    filtered_duplicate_body: int
    cluster_count: int
    noise_count: int
    noise_ratio: float
    embedding_stats: EmbeddingStats
    clusters: list[ClusterArtifact] = Field(default_factory=list)
    low_signal: bool = False
    warning: str | None = None

    def to_metadata(self) -> dict[str, Any]:
        return {
            "phase": "phase-2",
            "placeholder": False,
            "product_slug": self.product_slug,
            "iso_week": self.iso_week,
            "embedding_provider": self.embedding_provider,
            "embedding_model": self.embedding_model,
            "total_reviews_window": self.total_reviews_window,
            "eligible_reviews": self.eligible_reviews,
            "filtered_language": self.filtered_language,
            "filtered_too_short": self.filtered_too_short,
            "filtered_duplicate_body": self.filtered_duplicate_body,
            "cluster_count": self.cluster_count,
            "noise_count": self.noise_count,
            "noise_ratio": self.noise_ratio,
            "low_signal": self.low_signal,
            "warning": self.warning,
            "embedding_cache": self.embedding_stats.model_dump(mode="json"),
        }


class PersistedEmbedding(BaseModel):
    review_id: str
    vector: list[float]


class PersistedCluster(BaseModel):
    cluster_id: str
    run_id: str
    label: int
    size: int
    review_ids: list[str]
    representative_review_ids: list[str]
    keyphrases: list[str]
    medoid_review_id: str
    average_rating: float | None = None
    rating_stddev: float | None = None


class CacheRecord(BaseModel):
    text_hash: str
    provider: str
    model: str
    vector: list[float]
    created_at: datetime
    cache_path: Path
