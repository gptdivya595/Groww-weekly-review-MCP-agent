from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from agent.config import Settings
from agent.pulse_types import ProductConfig, StoredRunRecord
from agent.rendering.models import DOC_SECTION_URL_PLACEHOLDER
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
        stage="summarize",
        status="completed",
        iso_week="2026-W17",
        lookback_weeks=8,
        started_at=datetime(2026, 4, 23, 12, 0, tzinfo=UTC),
        completed_at=datetime(2026, 4, 23, 12, 5, tzinfo=UTC),
        week_start=datetime(2026, 4, 20, 0, 0, tzinfo=UTC),
        week_end=datetime(2026, 4, 26, 23, 59, tzinfo=UTC),
        lookback_start=datetime(2026, 3, 2, 0, 0, tzinfo=UTC),
        metadata={"phase": "phase-3"},
    )


def test_render_service_is_deterministic_and_capped(tmp_path: Path) -> None:
    settings = _build_settings(tmp_path)
    storage = Storage(settings.db_path)
    storage.initialize()
    run_id = "run_render_fixture"
    storage.replace_themes(
        run_id=run_id,
        themes=[
            SummarizedTheme(
                theme_id=f"{run_id}_theme_01",
                run_id=run_id,
                cluster_id=f"{run_id}_cluster_01",
                name="App Performance & Stability",
                summary=(
                    "Reviews repeatedly mention freezes and lag during market open "
                    "across 12 reviews."
                ),
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
                quote_text="Support asked me to email [REDACTED_EMAIL] for resolution.",
                action_ideas=["Expose clearer ticket status and expected response times."],
                representative_review_ids=["review-2"],
                coverage_count=10,
                average_rating=2.0,
                rating_stddev=0.4,
                model_provider="heuristic",
                model_name="heuristic-v1",
                low_coverage=False,
            ),
            SummarizedTheme(
                theme_id=f"{run_id}_theme_03",
                run_id=run_id,
                cluster_id=f"{run_id}_cluster_03",
                name="Ease of Use for New Investors",
                summary=(
                    "Reviews repeatedly praise the beginner-friendly experience "
                    "across 9 reviews."
                ),
                keyphrases=["easy", "beginner"],
                medoid_review_id="review-3",
                quote_review_id="review-3",
                quote_text="Very easy-to-use trading app with a clean interface.",
                action_ideas=[
                    "Keep the beginner-friendly onboarding path explicit in future releases."
                ],
                representative_review_ids=["review-3"],
                coverage_count=9,
                average_rating=4.8,
                rating_stddev=0.3,
                model_provider="heuristic",
                model_name="heuristic-v1",
                low_coverage=False,
            ),
            SummarizedTheme(
                theme_id=f"{run_id}_theme_04",
                run_id=run_id,
                cluster_id=f"{run_id}_cluster_04",
                name="Feature Gaps for Power Users",
                summary="Reviews ask for richer analytics and filters across 7 reviews.",
                keyphrases=["analytics", "filters"],
                medoid_review_id="review-4",
                quote_review_id="review-4",
                quote_text="Please add more advanced analytics for serious investors.",
                action_ideas=["Prioritize richer analytics and filter controls for power users."],
                representative_review_ids=["review-4"],
                coverage_count=7,
                average_rating=3.6,
                rating_stddev=0.2,
                model_provider="heuristic",
                model_name="heuristic-v1",
                low_coverage=False,
            ),
        ],
    )

    service = RenderService(settings=settings, storage=storage)
    first = service.run(run_record=_build_run_record(run_id), product=_build_product())
    second = service.run(run_record=_build_run_record(run_id), product=_build_product())

    assert first.anchor_key == "pulse-groww-2026-w17"
    assert first.docs_payload_hash == second.docs_payload_hash
    assert first.email_payload_hash == second.email_payload_hash
    assert first.artifact_hash == second.artifact_hash
    assert first.rendered_theme_count == 3
    assert first.quote_count == 2
    assert first.action_count == 3
    assert first.warning is not None
    assert "Rendered the top 3 themes out of 4 available themes." in first.warning
    assert "Dropped 1 quotes" in first.warning

    artifact = json.loads(first.artifact_path.read_text(encoding="utf-8"))
    assert artifact["anchor_key"] == "pulse-groww-2026-w17"
    assert len(artifact["top_themes"]) == 3
    assert all("[REDACTED_" not in quote for quote in artifact["quotes"])
    assert artifact["email_teaser"]["docs_link_placeholder"] == DOC_SECTION_URL_PLACEHOLDER
    assert DOC_SECTION_URL_PLACEHOLDER in artifact["email_teaser"]["plain_text_template"]
    assert DOC_SECTION_URL_PLACEHOLDER in artifact["email_teaser"]["html_template"]


def test_render_service_builds_low_signal_artifact_when_no_themes_exist(tmp_path: Path) -> None:
    settings = _build_settings(tmp_path)
    storage = Storage(settings.db_path)
    storage.initialize()
    run_id = "run_render_low_signal"

    service = RenderService(settings=settings, storage=storage)
    result = service.run(run_record=_build_run_record(run_id), product=_build_product())

    assert result.low_signal is True
    assert result.rendered_theme_count == 0
    assert result.quote_count == 0
    assert result.action_count == 1

    artifact = json.loads(result.artifact_path.read_text(encoding="utf-8"))
    assert artifact["top_themes"] == []
    assert artifact["quotes"] == []
    assert artifact["action_ideas"] == [
        "Review the grouped evidence and manually triage this low-signal week."
    ]
