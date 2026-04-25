from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from agent.config import Settings
from agent.mcp_client.docs_client import (
    DocsAppendResult,
    DocsDocument,
    DocsMcpClient,
    DocsSection,
    DocsToolNames,
)
from agent.mcp_client.session import McpSession
from agent.publish.docs_pipeline import run_docs_publish_for_run
from agent.pulse_types import ProductConfig, StoredRunRecord
from agent.rendering.models import RenderArtifact
from agent.rendering.pipeline import RenderService
from agent.storage import Storage
from agent.summarization.models import SummarizedTheme


def _build_settings(tmp_path: Path) -> Settings:
    return Settings(
        db_path=tmp_path / "pulse.sqlite",
        products_file=tmp_path / "products.yaml",
        raw_data_dir=tmp_path / "raw",
        embedding_cache_dir=tmp_path / "cache" / "embeddings",
        artifacts_dir=tmp_path / "artifacts",
        render_max_themes=3,
        render_max_quotes=3,
        render_max_action_ideas=3,
        render_email_teaser_themes=2,
    )


def _build_product(google_doc_id: str | None = "replace-with-google-doc-id") -> ProductConfig:
    return ProductConfig(
        slug="groww",
        display_name="Groww",
        app_store_app_id="1404871703",
        google_play_package="com.nextbillion.groww",
        google_doc_id=google_doc_id,
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
    google_doc_id: str | None = "replace-with-google-doc-id",
) -> tuple[Settings, Storage, ProductConfig, StoredRunRecord]:
    settings = _build_settings(tmp_path)
    storage = Storage(settings.db_path)
    storage.initialize()
    product = _build_product(google_doc_id=google_doc_id)
    storage.seed_products([product])

    run_id = "run_phase5_docs"
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
            ),
            SummarizedTheme(
                theme_id=f"{run_id}_theme_02",
                run_id=run_id,
                cluster_id=f"{run_id}_cluster_02",
                name="Customer Support Friction",
                summary="Reviews repeatedly mention slow support responses across 10 reviews.",
                keyphrases=["support", "response"],
                medoid_review_id="review-2",
                quote_review_id="review-2",
                quote_text="Support takes days to reply and ticket status is never clear.",
                action_ideas=["Expose clearer ticket status and expected response times."],
                representative_review_ids=["review-2"],
                coverage_count=10,
                average_rating=2.0,
                rating_stddev=0.4,
                model_provider="heuristic",
                model_name="heuristic-v1",
                low_coverage=False,
            ),
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
        self.document_id: str | None = None
        self.document_url: str | None = None
        self.title: str | None = None
        self.sections: list[DocsSection] = []
        self.append_calls = 0

    def ensure_document(
        self,
        *,
        preferred_document_id: str | None,
        title: str,
    ) -> DocsDocument:
        if self.document_id is None:
            self.document_id = preferred_document_id or "doc-groww"
            self.document_url = f"https://docs.google.com/document/d/{self.document_id}/edit"
            self.title = title
        return self.get_document(self.document_id)

    def get_document(self, document_id: str) -> DocsDocument:
        assert self.document_id is not None
        text_content = "\n\n".join(section.text_content for section in self.sections)
        return DocsDocument(
            document_id=document_id,
            title=self.title,
            document_url=self.document_url,
            text_content=text_content,
            sections=list(self.sections),
        )

    def append_section(
        self,
        *,
        document_id: str,
        request_tree: Any,
    ) -> DocsAppendResult:
        assert self.document_url is not None
        self.append_calls += 1
        heading_id = f"h.{len(self.sections) + 1}"
        deep_link = f"{self.document_url}#heading={heading_id}"
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
            document_url=self.document_url,
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
                "serverInfo": {"name": "fake-docs", "version": "1.0.0"},
            }
        if method == "tools/list":
            return {
                "tools": [
                    {"name": "docs.get_document"},
                    {"name": "docs.create_document"},
                    {"name": "docs.append_section"},
                ]
            }
        if method == "tools/call":
            tool_name = payload["name"]
            if tool_name == "docs.get_document":
                return {
                    "content": [
                        {
                            "type": "text",
                            "text": "Content (0 characters):\n---\n",
                        }
                    ],
                }
            if tool_name == "docs.append_section":
                return {
                    "content": [
                        {
                            "type": "text",
                            "text": "Successfully appended 42 characters of markdown.",
                        }
                    ],
                }
        raise AssertionError(f"Unexpected request: {method} {payload}")

    def notify(self, method: str, params: dict[str, Any] | None = None) -> None:
        self.notifications.append((method, params or {}))

    def close(self) -> None:
        return None


def test_docs_publish_is_idempotent_and_persists_delivery(tmp_path: Path) -> None:
    settings, storage, product, run_record = _prepare_rendered_run(tmp_path)
    fake_docs = FakeDocsClient()

    first = run_docs_publish_for_run(
        settings=settings,
        storage=storage,
        run_record=run_record,
        product=product,
        docs_client=fake_docs,
    )
    second = run_docs_publish_for_run(
        settings=settings,
        storage=storage,
        run_record=run_record,
        product=product,
        docs_client=fake_docs,
    )

    delivery = storage.get_delivery(run_record.run_id, "docs")

    assert first.published is True
    assert first.publish_action == "appended"
    assert second.published is False
    assert second.publish_action == "already_exists"
    assert fake_docs.append_calls == 1
    assert storage.get_product_google_doc_id("groww") == "doc-groww"
    assert delivery is not None
    assert delivery.external_id == "doc-groww#h.1"
    assert delivery.external_link == first.deep_link
    assert delivery.payload_hash == first.payload_hash


def test_docs_publish_detects_existing_anchor_without_prior_delivery(tmp_path: Path) -> None:
    settings, storage, product, run_record = _prepare_rendered_run(tmp_path)
    fake_docs = FakeDocsClient()

    first = run_docs_publish_for_run(
        settings=settings,
        storage=storage,
        run_record=run_record,
        product=product,
        docs_client=fake_docs,
    )
    with storage.connect() as connection:
        connection.execute(
            "DELETE FROM deliveries WHERE run_id = ? AND target = ?",
            (run_record.run_id, "docs"),
        )

    before_append_count = fake_docs.append_calls
    result = run_docs_publish_for_run(
        settings=settings,
        storage=storage,
        run_record=run_record,
        product=product,
        docs_client=fake_docs,
    )

    assert first.published is True
    assert result.published is False
    assert result.publish_action == "already_exists"
    assert fake_docs.append_calls == before_append_count


def test_docs_mcp_client_emits_expected_json_rpc_calls(tmp_path: Path) -> None:
    _, _, _, run_record = _prepare_rendered_run(tmp_path)
    artifact_path = Path(run_record.metadata["render_artifact_path"])
    artifact = RenderArtifact.model_validate_json(artifact_path.read_text(encoding="utf-8"))
    transport = FakeTransport()
    session = McpSession(
        transport=transport,
        protocol_version="2024-11-05",
        client_name="weekly-product-review-pulse",
        client_version="0.1.0",
    )
    client = DocsMcpClient(
        session=session,
        tool_names=DocsToolNames(
            get_document="docs.get_document",
            create_document="docs.create_document",
            append_section="docs.append_section",
        ),
    )

    document = client.get_document("doc-123")
    client.append_section(document_id=document.document_id, request_tree=artifact.docs_request_tree)

    assert transport.notifications == [("notifications/initialized", {})]
    assert [method for method, _ in transport.calls] == [
        "initialize",
        "tools/list",
        "tools/call",
        "tools/call",
    ]
    get_call = transport.calls[2][1]
    append_call = transport.calls[3][1]
    assert get_call == {
        "name": "docs.get_document",
        "arguments": {"documentId": "doc-123", "format": "text"},
    }
    assert append_call == {
        "name": "docs.append_section",
        "arguments": {
            "documentId": "doc-123",
            "markdown": artifact.docs_request_tree.markdown,
        },
    }
