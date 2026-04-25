from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

import agent.orchestration.pipeline as orchestration_pipeline
from agent.clustering.models import ClusteringResult, EmbeddingStats
from agent.config import Settings
from agent.ingestion.models import IngestionResult, ReviewUpsertStats, SourceIngestionReport
from agent.orchestration.locks import RunLockError
from agent.orchestration.pipeline import (
    build_run_audit_payload,
    run_pipeline_for_product,
    run_weekly_for_products,
)
from agent.publish.models import DocsPublishResult, GmailPublishResult
from agent.pulse_types import DeliveryTarget, ProductConfig, RunWindow, StoredRunRecord
from agent.storage import Storage
from agent.summarization.models import SummarizationResult


def _build_settings(tmp_path: Path, *, confirm_send: bool = False) -> Settings:
    return Settings(
        db_path=tmp_path / "pulse.sqlite",
        products_file=tmp_path / "products.yaml",
        raw_data_dir=tmp_path / "raw",
        embedding_cache_dir=tmp_path / "cache" / "embeddings",
        artifacts_dir=tmp_path / "artifacts",
        locks_dir=tmp_path / "locks",
        confirm_send=confirm_send,
    )


def _build_product(slug: str = "groww") -> ProductConfig:
    return ProductConfig(
        slug=slug,
        display_name=slug.replace("-", " ").title(),
        app_store_app_id="app-id",
        google_play_package=f"com.example.{slug}",
        google_doc_id="replace-with-google-doc-id",
        stakeholder_emails=["product-team@example.com"],
        default_lookback_weeks=8,
        country="in",
        lang="en",
        active=True,
    )


def _install_stage_fakes(
    monkeypatch: pytest.MonkeyPatch,
    *,
    settings: Settings,
    storage: Storage,
    docs_call_counter: list[int] | None = None,
    gmail_fail_once: bool = False,
    failing_product_slug: str | None = None,
) -> None:
    docs_counter = docs_call_counter if docs_call_counter is not None else [0]
    gmail_counter = [0]

    def fake_ingest(
        *,
        settings: Settings,
        storage: Storage,
        product: ProductConfig,
        window: RunWindow,
        run_id: str,
    ) -> IngestionResult:
        snapshot_path = settings.raw_data_dir / product.slug / f"{run_id}.jsonl"
        snapshot_path.parent.mkdir(parents=True, exist_ok=True)
        snapshot_path.write_text("", encoding="utf-8")
        return IngestionResult(
            run_id=run_id,
            product_slug=product.slug,
            iso_week="2026-W17",
            lookback_weeks=product.default_lookback_weeks,
            total_reviews=4,
            snapshot_path=snapshot_path,
            degraded=False,
            upsert=ReviewUpsertStats(inserted=4, updated=0, unchanged=0),
            sources=[
                SourceIngestionReport(source="appstore", status="ok", fetched=2),  # type: ignore[arg-type]
                SourceIngestionReport(source="playstore", status="ok", fetched=2),  # type: ignore[arg-type]
            ],
        )

    def fake_cluster(
        *,
        settings: Settings,
        storage: Storage,
        run_record: StoredRunRecord,
    ) -> ClusteringResult:
        run_id = run_record.run_id
        product_slug = run_record.product_slug
        iso_week = run_record.iso_week
        return ClusteringResult(
            run_id=run_id,
            product_slug=product_slug,
            iso_week=iso_week,
            embedding_provider="synthetic",
            embedding_model="synthetic-v1",
            total_reviews_window=4,
            eligible_reviews=4,
            filtered_language=0,
            filtered_too_short=0,
            filtered_duplicate_body=0,
            cluster_count=2,
            noise_count=0,
            noise_ratio=0.0,
            embedding_stats=EmbeddingStats(cache_hits=0, cache_misses=4),
            clusters=[],
        )

    def fake_summarize(
        *,
        settings: Settings,
        storage: Storage,
        run_record: StoredRunRecord,
    ) -> SummarizationResult:
        return SummarizationResult(
            run_id=run_record.run_id,
            product_slug=run_record.product_slug,
            iso_week=run_record.iso_week,
            summarization_provider="heuristic",
            summarization_model="heuristic-v1",
            clusters_available=2,
            clusters_summarized=2,
            theme_count=2,
            invalid_quote_count=0,
            quote_omission_count=0,
            retry_count=0,
            fallback_count=0,
            low_signal=False,
            themes=[],
        )

    def fake_render(
        *,
        settings: Settings,
        storage: Storage,
        run_record: StoredRunRecord,
        product: ProductConfig,
    ) -> object:
        artifact_path = (
            settings.artifacts_dir / "render" / product.slug / f"{run_record.run_id}.json"
        )
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        artifact_path.write_text(json.dumps({"run_id": run_record.run_id}), encoding="utf-8")

        class _RenderResult:
            def __init__(self) -> None:
                self.run_id = run_record.run_id
                self.product_slug = product.slug
                self.iso_week = run_record.iso_week
                self.anchor_key = f"pulse-{product.slug}-{run_record.iso_week.lower()}"
                self.available_theme_count = 2
                self.rendered_theme_count = 2
                self.quote_count = 2
                self.action_count = 2
                self.docs_block_count = 8
                self.artifact_path = artifact_path
                self.docs_payload_hash = "docs-hash"
                self.email_payload_hash = "email-hash"
                self.artifact_hash = "artifact-hash"
                self.low_signal = False
                self.warning = None

            def to_metadata(self) -> dict[str, object]:
                return {
                    "phase": "phase-4",
                    "placeholder": False,
                    "product_slug": product.slug,
                    "iso_week": run_record.iso_week,
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

        return _RenderResult()

    def fake_docs_publish(
        *,
        settings: Settings,
        storage: Storage,
        run_record: StoredRunRecord,
        product: ProductConfig,
        docs_client: object | None = None,
    ) -> DocsPublishResult:
        docs_counter[0] += 1
        artifact_path = Path(run_record.metadata["render_artifact_path"])
        delivery_id = storage.upsert_delivery(
            run_id=run_record.run_id,
            target="docs",
            status="completed",
            external_id="doc-123#h.1",
            external_link="https://docs.google.com/document/d/doc-123/edit#heading=h.1",
            payload_hash="docs-hash",
        )
        return DocsPublishResult(
            run_id=run_record.run_id,
            product_slug=product.slug,
            iso_week=run_record.iso_week,
            delivery_id=delivery_id,
            document_id="doc-123",
            heading_id="h.1",
            deep_link="https://docs.google.com/document/d/doc-123/edit#heading=h.1",
            document_url="https://docs.google.com/document/d/doc-123/edit",
            payload_hash="docs-hash",
            artifact_path=artifact_path,
            publish_action="appended",
            published=True,
        )

    def fake_gmail_publish(
        *,
        settings: Settings,
        storage: Storage,
        run_record: StoredRunRecord,
        product: ProductConfig,
        docs_result: object | None = None,
        docs_client: object | None = None,
        gmail_client: object | None = None,
    ) -> GmailPublishResult:
        gmail_counter[0] += 1
        if failing_product_slug is not None and product.slug == failing_product_slug:
            raise RuntimeError(f"Forced Gmail failure for {product.slug}.")
        if gmail_fail_once and gmail_counter[0] == 1:
            raise RuntimeError("Transient Gmail failure.")

        artifact_path = Path(run_record.metadata["render_artifact_path"])
        if settings.confirm_send:
            delivery_id = storage.upsert_delivery(
                run_id=run_record.run_id,
                target="gmail",
                status="sent",
                external_id="msg-123",
                external_link="https://mail.google.com/mail/u/0/#inbox/thread-123",
                payload_hash="email-hash",
            )
            return GmailPublishResult(
                run_id=run_record.run_id,
                product_slug=product.slug,
                iso_week=run_record.iso_week,
                delivery_id=delivery_id,
                docs_deep_link="https://docs.google.com/document/d/doc-123/edit#heading=h.1",
                draft_id="draft-123",
                message_id="msg-123",
                thread_id="thread-123",
                thread_link="https://mail.google.com/mail/u/0/#inbox/thread-123",
                payload_hash="email-hash",
                artifact_path=artifact_path,
                publish_mode="send",
                publish_action="sent",
                drafted=True,
                sent=True,
            )

        delivery_id = storage.upsert_delivery(
            run_id=run_record.run_id,
            target="gmail",
            status="drafted",
            external_id="draft-123",
            external_link="https://mail.google.com/mail/u/0/#inbox/thread-123",
            payload_hash="email-hash",
        )
        return GmailPublishResult(
            run_id=run_record.run_id,
            product_slug=product.slug,
            iso_week=run_record.iso_week,
            delivery_id=delivery_id,
            docs_deep_link="https://docs.google.com/document/d/doc-123/edit#heading=h.1",
            draft_id="draft-123",
            message_id=None,
            thread_id="thread-123",
            thread_link="https://mail.google.com/mail/u/0/#inbox/thread-123",
            payload_hash="email-hash",
            artifact_path=artifact_path,
            publish_mode="draft",
            publish_action="draft_created",
            drafted=True,
            sent=False,
        )

    monkeypatch.setattr(orchestration_pipeline, "run_ingestion_for_run", fake_ingest)
    monkeypatch.setattr(orchestration_pipeline, "run_clustering_for_run", fake_cluster)
    monkeypatch.setattr(orchestration_pipeline, "run_summarization_for_run", fake_summarize)
    monkeypatch.setattr(orchestration_pipeline, "run_render_for_run", fake_render)
    monkeypatch.setattr(orchestration_pipeline, "run_docs_publish_for_run", fake_docs_publish)
    monkeypatch.setattr(orchestration_pipeline, "run_gmail_publish_for_run", fake_gmail_publish)


def test_run_pipeline_executes_full_stage_order_and_writes_summary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _build_settings(tmp_path)
    storage = Storage(settings.db_path)
    storage.initialize()
    product = _build_product()
    storage.seed_products([product])
    _install_stage_fakes(monkeypatch, settings=settings, storage=storage)

    result = run_pipeline_for_product(
        settings=settings,
        storage=storage,
        product=product,
        iso_week="2026-W17",
        lookback_weeks=8,
        target=DeliveryTarget.ALL,
    )

    run_record = storage.get_run(result.run_id)
    assert run_record is not None
    assert result.status == "completed"
    assert [stage.name for stage in result.stages] == [
        "ingest",
        "cluster",
        "summarize",
        "render",
        "publish_docs",
        "publish_gmail",
    ]
    assert all(stage.status == "completed" for stage in result.stages)
    assert run_record.stage == "run"
    assert run_record.status == "completed"
    assert run_record.metadata["phase"] == "phase-7"
    assert result.summary_path.exists()

    payload = json.loads(result.summary_path.read_text(encoding="utf-8"))
    assert payload["run"]["run_id"] == result.run_id
    assert payload["run"]["metadata"]["orchestration_status"] == "completed"
    assert payload["run"]["metadata"]["error"] is None
    assert len(payload["deliveries"]) == 2
    assert payload["orchestration"]["status"] == "completed"


def test_run_pipeline_resumes_and_retries_only_gmail_after_docs_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _build_settings(tmp_path, confirm_send=True)
    storage = Storage(settings.db_path)
    storage.initialize()
    product = _build_product()
    storage.seed_products([product])
    docs_counter = [0]
    _install_stage_fakes(
        monkeypatch,
        settings=settings,
        storage=storage,
        docs_call_counter=docs_counter,
        gmail_fail_once=True,
    )

    with pytest.raises(RuntimeError, match="Transient Gmail failure"):
        run_pipeline_for_product(
            settings=settings,
            storage=storage,
            product=product,
            iso_week="2026-W17",
            lookback_weeks=8,
            target=DeliveryTarget.ALL,
        )

    resumed = run_pipeline_for_product(
        settings=settings,
        storage=storage,
        product=product,
        iso_week="2026-W17",
        lookback_weeks=8,
        target=DeliveryTarget.ALL,
    )

    assert resumed.resumed is True
    assert docs_counter[0] == 1
    assert [stage.status for stage in resumed.stages[:5]] == [
        "skipped",
        "skipped",
        "skipped",
        "skipped",
        "skipped",
    ]
    assert resumed.stages[5].name == "publish_gmail"
    assert resumed.stages[5].status == "completed"

    run_record = storage.get_run(resumed.run_id)
    assert run_record is not None
    assert run_record.metadata["orchestration_status"] == "completed"
    assert run_record.metadata.get("warning") is None
    assert run_record.metadata.get("error") is None

    payload = json.loads(resumed.summary_path.read_text(encoding="utf-8"))
    assert payload["run"]["metadata"]["orchestration_status"] == "completed"
    assert payload["run"]["metadata"]["error"] is None


def test_run_pipeline_blocks_when_lock_exists(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _build_settings(tmp_path)
    storage = Storage(settings.db_path)
    storage.initialize()
    product = _build_product()
    storage.seed_products([product])
    _install_stage_fakes(monkeypatch, settings=settings, storage=storage)

    lock_path = settings.locks_dir / "groww-2026-w17.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path.write_text(
        json.dumps({"acquired_at": datetime.now(UTC).isoformat()}),
        encoding="utf-8",
    )

    with pytest.raises(RunLockError):
        run_pipeline_for_product(
            settings=settings,
            storage=storage,
            product=product,
            iso_week="2026-W17",
            lookback_weeks=8,
            target=DeliveryTarget.ALL,
        )


def test_run_weekly_isolates_products_and_continues_after_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _build_settings(tmp_path)
    storage = Storage(settings.db_path)
    storage.initialize()
    products = [_build_product("groww"), _build_product("kuvera")]
    storage.seed_products(products)
    _install_stage_fakes(
        monkeypatch,
        settings=settings,
        storage=storage,
        failing_product_slug="kuvera",
    )

    result = run_weekly_for_products(
        settings=settings,
        storage=storage,
        products=products,
        iso_week="2026-W17",
        lookback_weeks=8,
        target=DeliveryTarget.ALL,
    )

    assert len(result.items) == 2
    assert result.failed_count == 1
    assert {item.product_slug: item.status for item in result.items} == {
        "groww": "completed",
        "kuvera": "failed",
    }


def test_build_run_audit_payload_includes_external_ids(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _build_settings(tmp_path)
    storage = Storage(settings.db_path)
    storage.initialize()
    product = _build_product()
    storage.seed_products([product])
    _install_stage_fakes(monkeypatch, settings=settings, storage=storage)

    result = run_pipeline_for_product(
        settings=settings,
        storage=storage,
        product=product,
        iso_week="2026-W17",
        lookback_weeks=8,
        target=DeliveryTarget.GMAIL,
    )
    run_record = storage.get_run(result.run_id)
    assert run_record is not None

    payload = build_run_audit_payload(storage=storage, run_record=run_record)

    assert payload["run"]["run_id"] == result.run_id
    assert {delivery["target"] for delivery in payload["deliveries"]} == {"docs", "gmail"}
