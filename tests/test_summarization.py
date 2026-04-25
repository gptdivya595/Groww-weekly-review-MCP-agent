from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from agent.clustering.models import PersistedCluster, ReviewDocument
from agent.config import Settings
from agent.ingestion.models import RawReview, ReviewSource
from agent.pulse_types import StoredRunRecord
from agent.storage import Storage
from agent.summarization.llm_client import SummarizationClientError
from agent.summarization.models import ClusterEvidence, ThemeDraft
from agent.summarization.pipeline import SummarizationService, run_summarization_for_run
from agent.summarization.verbatim import validate_quote_candidate


def _build_settings(tmp_path: Path, *, provider: str) -> Settings:
    return Settings(
        db_path=tmp_path / "pulse.sqlite",
        products_file=tmp_path / "products.yaml",
        raw_data_dir=tmp_path / "raw",
        embedding_cache_dir=tmp_path / "cache" / "embeddings",
        summarization_provider=provider,
        summarization_model="fake-openai" if provider == "openai" else "heuristic-v1",
        summarization_max_clusters=5,
        summarization_max_reviews_per_cluster=5,
        summarization_retry_attempts=2,
        summarization_max_output_tokens=300,
        summarization_low_coverage_threshold=4,
    )


def _build_run_record(run_id: str) -> StoredRunRecord:
    return StoredRunRecord(
        run_id=run_id,
        product_slug="groww",
        stage="cluster",
        status="completed",
        iso_week="2026-W17",
        lookback_weeks=8,
        started_at=datetime(2026, 4, 23, 12, 0, tzinfo=UTC),
        completed_at=datetime(2026, 4, 23, 12, 5, tzinfo=UTC),
        week_start=datetime(2026, 4, 20, 0, 0, tzinfo=UTC),
        week_end=datetime(2026, 4, 26, 23, 59, tzinfo=UTC),
        lookback_start=datetime(2026, 3, 2, 0, 0, tzinfo=UTC),
        metadata={"phase": "phase-2"},
    )


def _seed_cluster_fixture(
    storage: Storage,
    run_id: str,
    *,
    include_performance: bool = True,
    include_support: bool = True,
) -> list[RawReview]:
    all_reviews: list[RawReview] = []
    clusters: list[PersistedCluster] = []

    performance_reviews = [
        RawReview(
            source=ReviewSource.PLAYSTORE,
            external_id="perf-1",
            rating=1,
            title="Freezes at market open",
            body=(
                "The app freezes exactly when the market opens and order placement "
                "becomes impossible."
            ),
            author_alias="perf-1",
            review_created_at=datetime(2026, 4, 20, 9, 0, tzinfo=UTC),
            review_updated_at=datetime(2026, 4, 20, 9, 0, tzinfo=UTC),
            locale="en-in",
            app_version="1.0.0",
            source_url="https://example.com/perf-1",
            raw_payload={"fixture": "perf-1"},
        ),
        RawReview(
            source=ReviewSource.PLAYSTORE,
            external_id="perf-2",
            rating=2,
            title="Laggy refresh",
            body="Lag during portfolio refresh makes trading stressful and unreliable.",
            author_alias="perf-2",
            review_created_at=datetime(2026, 4, 21, 9, 0, tzinfo=UTC),
            review_updated_at=datetime(2026, 4, 21, 9, 0, tzinfo=UTC),
            locale="en-in",
            app_version="1.0.0",
            source_url="https://example.com/perf-2",
            raw_payload={"fixture": "perf-2"},
        ),
        RawReview(
            source=ReviewSource.PLAYSTORE,
            external_id="perf-3",
            rating=1,
            title="Crashes on holdings screen",
            body="Crashes happen whenever I try to check my holdings after login.",
            author_alias="perf-3",
            review_created_at=datetime(2026, 4, 22, 9, 0, tzinfo=UTC),
            review_updated_at=datetime(2026, 4, 22, 9, 0, tzinfo=UTC),
            locale="en-in",
            app_version="1.0.0",
            source_url="https://example.com/perf-3",
            raw_payload={"fixture": "perf-3"},
        ),
    ]
    support_reviews = [
        RawReview(
            source=ReviewSource.PLAYSTORE,
            external_id="support-1",
            rating=2,
            title="Slow support",
            body="Support takes days to reply and ticket status is never clear.",
            author_alias="support-1",
            review_created_at=datetime(2026, 4, 20, 11, 0, tzinfo=UTC),
            review_updated_at=datetime(2026, 4, 20, 11, 0, tzinfo=UTC),
            locale="en-in",
            app_version="1.0.0",
            source_url="https://example.com/support-1",
            raw_payload={"fixture": "support-1"},
        ),
        RawReview(
            source=ReviewSource.PLAYSTORE,
            external_id="support-2",
            rating=2,
            title="No update on issue",
            body="The response from support is slow and the app gives no update on my complaint.",
            author_alias="support-2",
            review_created_at=datetime(2026, 4, 21, 11, 0, tzinfo=UTC),
            review_updated_at=datetime(2026, 4, 21, 11, 0, tzinfo=UTC),
            locale="en-in",
            app_version="1.0.0",
            source_url="https://example.com/support-2",
            raw_payload={"fixture": "support-2"},
        ),
        RawReview(
            source=ReviewSource.PLAYSTORE,
            external_id="support-3",
            rating=1,
            title="Unresolved complaint",
            body="Customer support asked me to wait but my issue stayed unresolved for a week.",
            author_alias="support-3",
            review_created_at=datetime(2026, 4, 22, 11, 0, tzinfo=UTC),
            review_updated_at=datetime(2026, 4, 22, 11, 0, tzinfo=UTC),
            locale="en-in",
            app_version="1.0.0",
            source_url="https://example.com/support-3",
            raw_payload={"fixture": "support-3"},
        ),
    ]

    if include_performance:
        all_reviews.extend(performance_reviews)
        clusters.append(
            PersistedCluster(
                cluster_id=f"{run_id}_cluster_01",
                run_id=run_id,
                label=0,
                size=len(performance_reviews),
                review_ids=[review.review_id for review in performance_reviews],
                representative_review_ids=[
                    performance_reviews[0].review_id,
                    performance_reviews[1].review_id,
                ],
                keyphrases=["crash", "freeze", "lag"],
                medoid_review_id=performance_reviews[0].review_id,
                average_rating=1.33,
                rating_stddev=0.47,
            )
        )

    if include_support:
        all_reviews.extend(support_reviews)
        clusters.append(
            PersistedCluster(
                cluster_id=f"{run_id}_cluster_02",
                run_id=run_id,
                label=1,
                size=len(support_reviews),
                review_ids=[review.review_id for review in support_reviews],
                representative_review_ids=[
                    support_reviews[0].review_id,
                    support_reviews[2].review_id,
                ],
                keyphrases=["support", "ticket", "response"],
                medoid_review_id=support_reviews[0].review_id,
                average_rating=1.67,
                rating_stddev=0.47,
            )
        )

    storage.upsert_reviews("groww", all_reviews)
    storage.replace_clusters(run_id=run_id, clusters=clusters)
    return all_reviews


def test_validate_quote_candidate_accepts_real_review_excerpt() -> None:
    reviews = [
        ReviewDocument(
            review_id="support-1",
            product_slug="groww",
            source="playstore",
            pii_scrubbed_body="Support takes days to reply and ticket status is never clear.",
            locale="en-in",
        )
    ]

    validated = validate_quote_candidate(
        '"Support takes days to reply and ticket status is never clear."',
        reviews,
    )

    assert validated is not None
    assert validated.review_id == "support-1"
    assert validated.text == "Support takes days to reply and ticket status is never clear."


def test_validate_quote_candidate_rejects_hallucinated_text() -> None:
    reviews = [
        ReviewDocument(
            review_id="support-1",
            product_slug="groww",
            source="playstore",
            pii_scrubbed_body="Support takes days to reply and ticket status is never clear.",
            locale="en-in",
        )
    ]

    validated = validate_quote_candidate(
        "The app stole my money and support vanished.",
        reviews,
    )

    assert validated is None


def test_summarization_service_persists_grounded_themes(tmp_path: Path) -> None:
    settings = _build_settings(tmp_path, provider="heuristic")
    storage = Storage(settings.db_path)
    storage.initialize()
    run_id = "run_summarization_fixture"
    reviews = _seed_cluster_fixture(storage, run_id)

    result = run_summarization_for_run(
        settings=settings,
        storage=storage,
        run_record=_build_run_record(run_id),
    )

    assert result.theme_count == 2
    assert result.invalid_quote_count == 0
    assert result.quote_omission_count == 0

    persisted = storage.fetch_themes_for_run(run_id)
    review_text_by_id = {review.review_id: review.body or "" for review in reviews}

    assert len(persisted) == 2
    assert all(theme.action_ideas for theme in persisted)
    assert all(theme.low_coverage for theme in persisted)
    assert all("Signal is limited" in theme.summary for theme in persisted)
    assert all(theme.quote_text is not None for theme in persisted)
    for theme in persisted:
        assert theme.quote_review_id is not None
        assert theme.quote_text in review_text_by_id[theme.quote_review_id]


class InvalidQuoteClient:
    provider_name = "openai"
    model_name = "fake-openai"

    def summarize_cluster(self, evidence: ClusterEvidence) -> ThemeDraft:
        return ThemeDraft(
            name="Recurring Customer Feedback",
            summary="Reviews repeatedly mention product friction that needs follow through.",
            quote_review_id="missing-review",
            quote_text="This wording never appears in the stored reviews.",
            action_ideas=["Expose ticket status and expected response times in the app."],
        )


def test_summarization_omits_invalid_quotes(tmp_path: Path) -> None:
    settings = _build_settings(tmp_path, provider="openai")
    storage = Storage(settings.db_path)
    storage.initialize()
    run_id = "run_invalid_quote_fixture"
    _seed_cluster_fixture(storage, run_id)

    service = SummarizationService(
        settings=settings,
        storage=storage,
        client=InvalidQuoteClient(),
    )
    result = service.run(run_record=_build_run_record(run_id))

    assert result.theme_count == 2
    assert result.invalid_quote_count == 2
    assert result.quote_omission_count == 2
    assert all(theme.quote_text is None for theme in result.themes)


class FlakyClient:
    provider_name = "openai"
    model_name = "fake-openai"

    def __init__(self) -> None:
        self.calls = 0

    def summarize_cluster(self, evidence: ClusterEvidence) -> ThemeDraft:
        self.calls += 1
        if self.calls == 1:
            raise SummarizationClientError("malformed JSON")
        return ThemeDraft(
            name="Customer Support Friction",
            summary=(
                "Reviews repeatedly mention slow responses and poor visibility into "
                "ticket updates."
            ),
            quote_review_id=evidence.reviews[0].review_id,
            quote_text=evidence.reviews[0].text,
            action_ideas=["Expose ticket status and expected response times in the app."],
        )


def test_summarization_retries_after_client_error(tmp_path: Path) -> None:
    settings = _build_settings(tmp_path, provider="openai")
    storage = Storage(settings.db_path)
    storage.initialize()
    run_id = "run_retry_fixture"
    _seed_cluster_fixture(storage, run_id, include_performance=False, include_support=True)

    client = FlakyClient()
    service = SummarizationService(
        settings=settings,
        storage=storage,
        client=client,
    )
    result = service.run(run_record=_build_run_record(run_id))

    assert client.calls == 2
    assert result.retry_count == 1
    assert result.fallback_count == 0
    assert result.theme_count == 1
    assert result.invalid_quote_count == 0
    assert result.themes[0].quote_text is not None
    assert result.themes[0].model_provider == "openai"
