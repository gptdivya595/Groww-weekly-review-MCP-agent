from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, Field

DOC_SECTION_URL_PLACEHOLDER = "{{DOC_SECTION_URL}}"


class RenderedTheme(BaseModel):
    theme_id: str
    name: str
    summary: str
    quote_text: str | None = None
    quote_review_id: str | None = None
    action_idea: str | None = None
    coverage_count: int
    average_rating: float | None = None
    low_coverage: bool = False


class DocsBlock(BaseModel):
    type: Literal["heading", "paragraph", "numbered_list", "bullet_list", "blockquote"]
    level: int | None = None
    text: str | None = None
    items: list[str] = Field(default_factory=list)


class DocsRequestTree(BaseModel):
    anchor_key: str
    section_heading: str
    machine_key_line: str
    blocks: list[DocsBlock] = Field(default_factory=list)
    markdown: str


class EmailTeaserPayload(BaseModel):
    subject: str
    docs_link_placeholder: str = DOC_SECTION_URL_PLACEHOLDER
    plain_text_template: str
    html_template: str


class RenderArtifact(BaseModel):
    run_id: str
    product_slug: str
    product_display_name: str
    iso_week: str
    lookback_weeks: int
    section_heading: str
    anchor_key: str
    machine_key_line: str
    period_label: str
    available_theme_count: int
    rendered_theme_count: int
    top_themes: list[RenderedTheme] = Field(default_factory=list)
    quotes: list[str] = Field(default_factory=list)
    action_ideas: list[str] = Field(default_factory=list)
    who_this_helps: list[str] = Field(default_factory=list)
    docs_request_tree: DocsRequestTree
    email_teaser: EmailTeaserPayload
    docs_payload_hash: str
    email_payload_hash: str
    artifact_hash: str


class RenderResult(BaseModel):
    run_id: str
    product_slug: str
    iso_week: str
    anchor_key: str
    available_theme_count: int
    rendered_theme_count: int
    quote_count: int
    action_count: int
    docs_block_count: int
    artifact_path: Path
    docs_payload_hash: str
    email_payload_hash: str
    artifact_hash: str
    low_signal: bool = False
    warning: str | None = None

    def to_metadata(self) -> dict[str, Any]:
        return {
            "phase": "phase-4",
            "placeholder": False,
            "product_slug": self.product_slug,
            "iso_week": self.iso_week,
            "anchor_key": self.anchor_key,
            "available_theme_count": self.available_theme_count,
            "rendered_theme_count": self.rendered_theme_count,
            "quote_count": self.quote_count,
            "action_count": self.action_count,
            "docs_block_count": self.docs_block_count,
            "render_artifact_path": str(self.artifact_path),
            "docs_payload_hash": self.docs_payload_hash,
            "email_payload_hash": self.email_payload_hash,
            "artifact_hash": self.artifact_hash,
            "low_signal": self.low_signal,
            "warning": self.warning,
        }
