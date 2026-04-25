from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from agent.clustering.models import ReviewDocument


class ClusterEvidence(BaseModel):
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
    reviews: list[ReviewDocument] = Field(default_factory=list)


class ValidatedQuote(BaseModel):
    review_id: str
    text: str


class ThemeDraft(BaseModel):
    name: str
    summary: str
    quote_review_id: str | None = None
    quote_text: str | None = None
    action_ideas: list[str] = Field(default_factory=list)


class SummarizedTheme(BaseModel):
    theme_id: str
    run_id: str
    cluster_id: str
    name: str
    summary: str
    keyphrases: list[str] = Field(default_factory=list)
    medoid_review_id: str | None = None
    quote_review_id: str | None = None
    quote_text: str | None = None
    action_ideas: list[str] = Field(default_factory=list)
    representative_review_ids: list[str] = Field(default_factory=list)
    coverage_count: int
    average_rating: float | None = None
    rating_stddev: float | None = None
    model_provider: str
    model_name: str
    low_coverage: bool = False


class SummarizationResult(BaseModel):
    run_id: str
    product_slug: str
    iso_week: str
    summarization_provider: str
    summarization_model: str
    clusters_available: int
    clusters_summarized: int
    theme_count: int
    invalid_quote_count: int
    quote_omission_count: int
    retry_count: int
    fallback_count: int
    low_signal: bool = False
    warning: str | None = None
    themes: list[SummarizedTheme] = Field(default_factory=list)

    def to_metadata(self) -> dict[str, Any]:
        return {
            "phase": "phase-3",
            "placeholder": False,
            "product_slug": self.product_slug,
            "iso_week": self.iso_week,
            "summarization_provider": self.summarization_provider,
            "summarization_model": self.summarization_model,
            "clusters_available": self.clusters_available,
            "clusters_summarized": self.clusters_summarized,
            "theme_count": self.theme_count,
            "invalid_quote_count": self.invalid_quote_count,
            "quote_omission_count": self.quote_omission_count,
            "retry_count": self.retry_count,
            "fallback_count": self.fallback_count,
            "low_signal": self.low_signal,
            "warning": self.warning,
        }
