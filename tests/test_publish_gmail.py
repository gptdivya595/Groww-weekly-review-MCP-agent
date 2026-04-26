from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from agent.config import Settings
from agent.mcp_client.docs_client import DocsAppendResult, DocsDocument, DocsSection
from agent.mcp_client.gmail_client import (
    GmailDraftResult,
    GmailMcpClient,
    GmailSendResult,
    GmailToolNames,
)
from agent.mcp_client.session import McpSession
from agent.publish.gmail_pipeline import run_gmail_publish_for_run
from agent.pulse_types import ProductConfig, StoredRunRecord
from agent.rendering.pipeline import RenderService
from agent.storage import Storage
from agent.summarization.models import SummarizedTheme


def _build_settings(tmp_path: Path, *, confirm_send: bool = False) -> Settings:
    return Settings(
        db_path=tmp_path / "pulse.sqlite",
        products_file=tmp_path / "products.yaml",
        raw_data_dir=tmp_path / "raw",
        embedding_cache_dir=tmp_path / "cache" / "embeddings",
        artifacts_dir=tmp_path / "artifacts",
        confirm_send=confirm_send,
        render_max_themes=3,
        render_max_quotes=3,
        render_max_action_ideas=3,
        render_email_teaser_themes=2,
    )


def _build_product() -> ProductConfig:
    return ProductConfig(
        slug="groww",
        display_name="Groww",
        app_store_app_id="1404871703",
        google_play_package="com.nextbillion.groww",
        google_doc_id="replace-with-google-doc-id",
        stakeholder_emails=["product-team@example.com"],
        default_lookback_weeks=8,
        country="in",
        lang="en",
        active=True,
    )


def _build_run_record(run_id: str) -> StoredRunRecord:
    return StoredRunRecord(
        run_id=run_id,
        product_slug="groww",
        stage="render",
        status="completed",
        iso_week="2026-W17",
        lookback_weeks=8,
        started_at=datetime(2026, 4, 23, 12, 0, tzinfo=UTC),
        completed_at=datetime(2026, 4, 23, 12, 5, tzinfo=UTC),
        week_start=datetime(2026, 4, 20, 0, 0, tzinfo=UTC),
        week_end=datetime(2026, 4, 26, 23, 59, tzinfo=UTC),
        lookback_start=datetime(2026, 3, 2, 0, 0, tzinfo=UTC),
        metadata={"phase": "phase-4"},
    )


def _prepare_rendered_run(
    tmp_path: Path,
    *,
    confirm_send: bool = False,
) -> tuple[Settings, Storage, ProductConfig, StoredRunRecord]:
    settings = _build_settings(tmp_path, confirm_send=confirm_send)
    storage = Storage(settings.db_path)
    storage.initialize()
    product = _build_product()
    storage.seed_products([product])

    run_id = "run_phase6_gmail"
    run_record = _build_run_record(run_id)
    storage.upsert_run(
        run_id=run_record.run_id,
        product_slug=run_record.product_slug,
        iso_week=run_record.iso_week,
        stage=run_record.stage,
        status=run_record.status,
        lookback_weeks=run_record.lookback_weeks,
        week_start=run_record.week_start.isoformat(),
        week_end=run_record.week_end.isoformat(),
        lookback_start=run_record.lookback_start.isoformat(),
        metadata=run_record.metadata,
    )
    storage.replace_themes(
        run_id=run_id,
        themes=[
            SummarizedTheme(
                theme_id=f"{run_id}_theme_01",
                run_id=run_id,
                cluster_id=f"{run_id}_cluster_01",
                name="App Performance & Stability",
                summary="Reviews repeatedly mention freezes during market open across 12 reviews.",
                keyphrases=["freeze", "lag"],
                medoid_review_id="review-1",
                quote_review_id="review-1",
                quote_text="The app freezes exactly when the market opens.",
                action_ideas=["Instrument peak-load flows tied to crashes and lag."],
                representative_review_ids=["review-1"],
                coverage_count=12,
                average_rating=1.5,
                rating_stddev=0.5,
                model_provider="heuristic",
                model_name="heuristic-v1",
                low_coverage=False,
            )
        ],
    )

    render_result = RenderService(settings=settings, storage=storage).run(
        run_record=_build_run_record(run_id),
        product=product,
    )
    storage.update_run_status(
        run_id,
        status="completed",
        stage="render",
        metadata=render_result.to_metadata(),
        completed=True,
    )
    stored_run = storage.get_run(run_id)
    assert stored_run is not None
    return settings, storage, product, stored_run


class FakeDocsClient:
    def __init__(self) -> None:
        self.append_calls = 0
        self.sections: list[DocsSection] = []

    def ensure_document(
        self,
        *,
        preferred_document_id: str | None,
        title: str,
    ) -> Any:
        document_id = preferred_document_id or "doc-groww"
        return self.get_document(document_id)

    def get_document(self, document_id: str) -> Any:
        return DocsDocument(
            document_id=document_id,
            document_url=f"https://docs.google.com/document/d/{document_id}/edit",
            text_content="\n\n".join(section.text_content for section in self.sections),
            sections=list(self.sections),
        )

    def append_section(
        self,
        *,
        document_id: str,
        request_tree: Any,
    ) -> Any:
        self.append_calls += 1
        heading_id = "h.gmail"
        deep_link = f"https://docs.google.com/document/d/{document_id}/edit#heading={heading_id}"
        self.sections.append(
            DocsSection(
                heading=request_tree.section_heading,
                heading_id=heading_id,
                deep_link=deep_link,
                text_content=request_tree.markdown,
                machine_key_line=request_tree.machine_key_line,
            )
        )
        return DocsAppendResult(
            document_id=document_id,
            heading_id=heading_id,
            deep_link=deep_link,
            document_url=f"https://docs.google.com/document/d/{document_id}/edit",
        )

    def close(self) -> None:
        return None


class FakeGmailClient:
    def __init__(self) -> None:
        self.create_calls: list[dict[str, Any]] = []
        self.update_calls: list[dict[str, Any]] = []
        self.send_calls: list[dict[str, Any]] = []

    def create_draft(
        self,
        *,
        to: list[str],
        subject: str,
        plain_text_body: str,
        html_body: str,
        idempotency_key: str,
    ) -> GmailDraftResult:
        self.create_calls.append(
            {
                "to": list(to),
                "subject": subject,
                "plain_text_body": plain_text_body,
                "html_body": html_body,
                "idempotency_key": idempotency_key,
            }
        )
        draft_number = len(self.create_calls)
        return GmailDraftResult(
            draft_id=f"draft-{draft_number}",
            thread_id="thread-1",
            thread_link="https://mail.google.com/mail/u/0/#inbox/thread-1",
        )

    def update_draft(
        self,
        *,
        draft_id: str,
        to: list[str],
        subject: str,
        plain_text_body: str,
        html_body: str,
        idempotency_key: str,
        thread_id: str | None = None,
    ) -> GmailDraftResult:
        self.update_calls.append(
            {
                "draft_id": draft_id,
                "to": list(to),
                "subject": subject,
                "plain_text_body": plain_text_body,
                "html_body": html_body,
                "idempotency_key": idempotency_key,
                "thread_id": thread_id,
            }
        )
        return GmailDraftResult(
            draft_id=draft_id,
            thread_id=thread_id or "thread-1",
            thread_link="https://mail.google.com/mail/u/0/#inbox/thread-1",
        )

    def send_draft(self, *, draft_id: str) -> GmailSendResult:
        self.send_calls.append({"draft_id": draft_id})
        message_number = len(self.send_calls)
        return GmailSendResult(
            message_id=f"msg-{message_number}",
            draft_id=draft_id,
            thread_id="thread-1",
            thread_link="https://mail.google.com/mail/u/0/#inbox/thread-1",
        )

    def close(self) -> None:
        return None


class FakeTransport:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.notifications: list[tuple[str, dict[str, Any]]] = []

    def start(self) -> None:
        return None

    def request(self, method: str, params: dict[str, Any] | None = None) -> Any:
        payload = params or {}
        self.calls.append((method, payload))
        if method == "initialize":
            return {
                "protocolVersion": "2024-11-05",
                "serverInfo": {"name": "fake-gmail", "version": "1.0.0"},
            }
        if method == "tools/list":
            return {
                "tools": [
                    {"name": "gmail.create_draft"},
                    {"name": "gmail.update_draft"},
                    {"name": "gmail.send_draft"},
                ]
            }
        if method == "tools/call":
            tool_name = payload["name"]
            arguments = payload["arguments"]
            if tool_name in {"gmail.create_draft", "gmail.update_draft"}:
                return {
                    "content": [
                        {
                            "type": "text",
                            "text": (
                                '{'
                                f'"draftId":"{arguments.get("draftId", "draft-1")}",'
                                '"threadId":"thread-1",'
                                '"threadLink":"https://mail.google.com/mail/u/0/#inbox/thread-1"'
                                '}'
                            ),
                        }
                    ],
                }
            if tool_name == "gmail.send_draft":
                return {
                    "content": [
                        {
                            "type": "text",
                            "text": (
                                '{'
                                '"messageId":"msg-1",'
                                f'"draftId":"{arguments["draftId"]}",'
                                '"threadId":"thread-1",'
                                '"threadLink":"https://mail.google.com/mail/u/0/#inbox/thread-1"'
                                '}'
                            ),
                        }
                    ],
                }
        raise AssertionError(f"Unexpected request: {method} {payload}")

    def notify(self, method: str, params: dict[str, Any] | None = None) -> None:
        self.notifications.append((method, params or {}))

    def close(self) -> None:
        return None


def test_gmail_publish_drafts_by_default_and_reuses_same_run(tmp_path: Path) -> None:
    settings, storage, product, run_record = _prepare_rendered_run(tmp_path, confirm_send=False)
    docs_client = FakeDocsClient()
    gmail_client = FakeGmailClient()

    first = run_gmail_publish_for_run(
        settings=settings,
        storage=storage,
        run_record=run_record,
        product=product,
        docs_client=docs_client,
        gmail_client=gmail_client,
    )
    second = run_gmail_publish_for_run(
        settings=settings,
        storage=storage,
        run_record=run_record,
        product=product,
        docs_client=docs_client,
        gmail_client=gmail_client,
    )

    delivery = storage.get_delivery(run_record.run_id, "gmail")

    assert first.publish_mode == "draft"
    assert first.publish_action == "draft_created"
    assert first.drafted is True
    assert first.sent is False
    assert second.publish_action == "draft_reused"
    assert gmail_client.create_calls and len(gmail_client.create_calls) == 1
    assert gmail_client.update_calls == []
    assert gmail_client.send_calls == []
    assert docs_client.append_calls == 1
    assert (
        "https://docs.google.com/document/d/doc-groww/edit#heading=h.gmail"
        in gmail_client.create_calls[0]["plain_text_body"]
    )
    assert delivery is not None
    assert delivery.status == "drafted"
    assert delivery.external_id == "draft-1"


def test_gmail_publish_sends_once_when_confirm_send_enabled(tmp_path: Path) -> None:
    settings, storage, product, run_record = _prepare_rendered_run(tmp_path, confirm_send=True)
    docs_client = FakeDocsClient()
    gmail_client = FakeGmailClient()

    first = run_gmail_publish_for_run(
        settings=settings,
        storage=storage,
        run_record=run_record,
        product=product,
        docs_client=docs_client,
        gmail_client=gmail_client,
    )
    second = run_gmail_publish_for_run(
        settings=settings,
        storage=storage,
        run_record=run_record,
        product=product,
        docs_client=docs_client,
        gmail_client=gmail_client,
    )

    delivery = storage.get_delivery(run_record.run_id, "gmail")

    assert first.publish_mode == "send"
    assert first.publish_action == "sent"
    assert first.sent is True
    assert second.publish_action == "already_sent"
    assert gmail_client.create_calls and len(gmail_client.create_calls) == 1
    assert gmail_client.send_calls == [{"draft_id": "draft-1"}]
    assert delivery is not None
    assert delivery.status == "sent"
    assert delivery.external_id == "msg-1"


def test_gmail_publish_force_delivery_sends_again_for_same_run(tmp_path: Path) -> None:
    settings, storage, product, run_record = _prepare_rendered_run(tmp_path, confirm_send=True)
    docs_client = FakeDocsClient()
    gmail_client = FakeGmailClient()

    first = run_gmail_publish_for_run(
        settings=settings,
        storage=storage,
        run_record=run_record,
        product=product,
        docs_client=docs_client,
        gmail_client=gmail_client,
    )
    second = run_gmail_publish_for_run(
        settings=settings,
        storage=storage,
        run_record=storage.get_run(run_record.run_id) or run_record,
        product=product,
        docs_client=docs_client,
        gmail_client=gmail_client,
        force_delivery=True,
    )

    delivery = storage.get_delivery(run_record.run_id, "gmail")

    assert first.publish_action == "sent"
    assert second.publish_action == "resent"
    assert second.message_id == "msg-2"
    assert second.warning == "Manual Gmail resend completed for this existing report."
    assert gmail_client.create_calls and len(gmail_client.create_calls) == 2
    assert gmail_client.send_calls == [{"draft_id": "draft-1"}, {"draft_id": "draft-2"}]
    assert delivery is not None
    assert delivery.status == "sent"
    assert delivery.external_id == "msg-2"


def test_gmail_mcp_client_emits_expected_json_rpc_calls() -> None:
    transport = FakeTransport()
    session = McpSession(
        transport=transport,
        protocol_version="2024-11-05",
        client_name="weekly-product-review-pulse",
        client_version="0.1.0",
    )
    client = GmailMcpClient(
        session=session,
        tool_names=GmailToolNames(
            create_draft="gmail.create_draft",
            update_draft="gmail.update_draft",
            send_draft="gmail.send_draft",
        ),
    )

    client.create_draft(
        to=["product-team@example.com"],
        subject="Groww Weekly Review Pulse - 2026-W17",
        plain_text_body="Read full report: https://docs.google.com/document/d/doc-groww/edit#heading=h.gmail",
        html_body="<p>Read full report</p>",
        idempotency_key="pulse-groww-2026-w17:gmail",
    )
    client.update_draft(
        draft_id="draft-1",
        to=["product-team@example.com"],
        subject="Groww Weekly Review Pulse - 2026-W17",
        plain_text_body="updated",
        html_body="<p>updated</p>",
        idempotency_key="pulse-groww-2026-w17:gmail",
        thread_id="thread-1",
    )
    client.send_draft(draft_id="draft-1")

    assert transport.notifications == [("notifications/initialized", {})]
    assert [method for method, _ in transport.calls] == [
        "initialize",
        "tools/list",
        "tools/call",
        "tools/call",
        "tools/call",
    ]
    assert transport.calls[2][1]["name"] == "gmail.create_draft"
    assert transport.calls[2][1]["arguments"]["to"] == ["product-team@example.com"]
    assert transport.calls[2][1]["arguments"]["body"].startswith("Read full report:")
    assert transport.calls[3][1]["name"] == "gmail.update_draft"
    assert transport.calls[3][1]["arguments"]["draftId"] == "draft-1"
    assert transport.calls[3][1]["arguments"]["body"] == "updated"
    assert transport.calls[4][1] == {
        "name": "gmail.send_draft",
        "arguments": {"draftId": "draft-1"},
    }
