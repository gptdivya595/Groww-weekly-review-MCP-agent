from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel


class DocsPublishResult(BaseModel):
    run_id: str
    product_slug: str
    iso_week: str
    delivery_id: str
    document_id: str
    heading_id: str | None = None
    deep_link: str
    document_url: str
    payload_hash: str
    artifact_path: Path
    publish_action: Literal["appended", "already_exists"]
    published: bool
    warning: str | None = None

    def to_metadata(self) -> dict[str, Any]:
        return {
            "phase": "phase-5",
            "placeholder": False,
            "docs_delivery_id": self.delivery_id,
            "gdoc_id": self.document_id,
            "gdoc_heading_id": self.heading_id,
            "gdoc_deep_link": self.deep_link,
            "gdoc_document_url": self.document_url,
            "docs_payload_hash": self.payload_hash,
            "render_artifact_path": str(self.artifact_path),
            "docs_publish_action": self.publish_action,
            "docs_published": self.published,
            "warning": self.warning,
        }


class GmailPublishResult(BaseModel):
    run_id: str
    product_slug: str
    iso_week: str
    delivery_id: str
    docs_deep_link: str
    draft_id: str | None = None
    message_id: str | None = None
    thread_id: str | None = None
    thread_link: str | None = None
    payload_hash: str
    artifact_path: Path
    publish_mode: Literal["draft", "send"]
    publish_action: Literal[
        "draft_created",
        "draft_updated",
        "draft_reused",
        "sent",
        "already_sent",
    ]
    drafted: bool
    sent: bool
    warning: str | None = None

    def to_metadata(self) -> dict[str, Any]:
        return {
            "phase": "phase-6",
            "placeholder": False,
            "gmail_delivery_id": self.delivery_id,
            "gmail_draft_id": self.draft_id,
            "gmail_message_id": self.message_id,
            "gmail_thread_id": self.thread_id,
            "gmail_thread_link": self.thread_link,
            "gmail_payload_hash": self.payload_hash,
            "gmail_publish_mode": self.publish_mode,
            "gmail_publish_action": self.publish_action,
            "gmail_drafted": self.drafted,
            "gmail_sent": self.sent,
            "email_docs_link": self.docs_deep_link,
            "render_artifact_path": str(self.artifact_path),
            "warning": self.warning,
        }
