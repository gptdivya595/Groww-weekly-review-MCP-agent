from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import numpy as np

from agent.clustering.filters import filter_reviews, parse_supported_languages
from agent.clustering.models import ReviewDocument
from agent.clustering.pipeline import ClusteringService
from agent.config import Settings
from agent.ingestion.models import RawReview, ReviewSource
from agent.pulse_types import StoredRunRecord
from agent.storage import Storage


class SyntheticEmbeddingProvider:
    provider_name = "synthetic"
    model_name = "synthetic-v1"

    def embed_texts(self, texts: list[str]) -> np.ndarray:
        vectors: list[list[float]] = []
        for text in texts:
            lowered = text.lower()
            if "crash" in lowered or "lag" in lowered:
                base = [5.0, 0.0, 0.0, 0.0]
            elif "support" in lowered or "ticket" in lowered:
                base = [0.0, 5.0, 0.0, 0.0]
            elif "portfolio" in lowered or "navigation" in lowered:
                base = [0.0, 0.0, 5.0, 0.0]
            else:
                base = [0.0, 0.0, 0.0, 5.0]
            offset = (sum(ord(char) for char in lowered) % 7) / 100.0
            vectors.append([value + offset for value in base])
        return np.asarray(vectors, dtype=np.float32)


def _build_settings(tmp_path: Path) -> Settings:
    return Settings(
        db_path=tmp_path / "pulse.sqlite",
        products_file=tmp_path / "products.yaml",
        raw_data_dir=tmp_path / "raw",
        embedding_cache_dir=tmp_path / "cache" / "embeddings",
        log_level="INFO",
        timezone="Asia/Kolkata",
        max_run_cost_usd=5.0,
        confirm_send=False,
        http_timeout_seconds=5.0,
        appstore_max_pages=2,
        playstore_page_size=3,
        playstore_max_pages=2,
        cluster_embedding_provider="synthetic",
        cluster_embedding_model="synthetic-v1",
        cluster_supported_languages="en",
        cluster_min_text_chars=20,
        cluster_min_cluster_size=4,
        cluster_umap_n_neighbors=8,
        cluster_umap_min_dist=0.0,
        cluster_umap_components=3,
        cluster_random_state=42,
        cluster_keyphrases_top_n=4,
        cluster_representatives_per_cluster=3,
        cluster_noise_warning_ratio=0.35,
        cluster_hash_dimensions=64,
    )


def _insert_reviews(storage: Storage) -> None:
    now = datetime(2026, 4, 23, 12, 0, tzinfo=UTC)
    reviews: list[RawReview] = []
    topics = [
        ("crash", 1, "The app crash issue appears during market open and causes severe lag."),
        ("support", 2, "Support ticket handling is slow and the response takes too long."),
        (
            "portfolio",
            3,
            "Portfolio navigation is confusing and analysis screens are hard to use.",
        ),
        ("education", 5, "Learning content is good and beginner guidance feels very useful."),
    ]

    index = 0
    for topic, rating, template in topics:
        for variation in range(10):
            reviews.append(
                RawReview(
                    source=ReviewSource.PLAYSTORE,
                    external_id=f"{topic}-{variation}",
                    rating=rating if variation % 2 == 0 else min(rating + 1, 5),
                    title=f"{topic.title()} review {variation}",
                    body=f"{template} Variation {variation}.",
                    author_alias=f"user-{index}",
                    review_created_at=now - timedelta(days=variation),
                    review_updated_at=now - timedelta(days=variation),
                    locale="en-in",
                    app_version="1.0.0",
                    source_url=f"https://example.com/{topic}/{variation}",
                    raw_payload={"topic": topic, "variation": variation},
                )
            )
            index += 1

    reviews.extend(
        [
            RawReview(
                source=ReviewSource.PLAYSTORE,
                external_id="short-1",
                rating=2,
                title="Too short",
                body="Bad app",
                author_alias="short-user",
                review_created_at=now,
                review_updated_at=now,
                locale="en-in",
                app_version="1.0.0",
                source_url="https://example.com/short",
                raw_payload={"topic": "short"},
            ),
            RawReview(
                source=ReviewSource.PLAYSTORE,
                external_id="hi-1",
                rating=3,
                title="Hindi",
                body="yah application bahut acchi hai lekin support slow hai",
                author_alias="lang-user",
                review_created_at=now,
                review_updated_at=now,
                locale="hi-in",
                app_version="1.0.0",
                source_url="https://example.com/lang",
                raw_payload={"topic": "language"},
            ),
            RawReview(
                source=ReviewSource.PLAYSTORE,
                external_id="dup-1",
                rating=1,
                title="Duplicate one",
                body=(
                    "The app crash issue appears during market open and causes severe lag. "
                    "Variation 0."
                ),
                author_alias="dup-user",
                review_created_at=now,
                review_updated_at=now,
                locale="en-in",
                app_version="1.0.0",
                source_url="https://example.com/dup",
                raw_payload={"topic": "duplicate"},
            ),
        ]
    )

    storage.upsert_reviews("groww", reviews)


def _build_run_record() -> StoredRunRecord:
    return StoredRunRecord(
        run_id="run_cluster_fixture",
        product_slug="groww",
        stage="ingest",
        status="completed",
        iso_week="2026-W17",
        lookback_weeks=8,
        started_at=datetime(2026, 4, 23, 12, 0, tzinfo=UTC),
        completed_at=datetime(2026, 4, 23, 12, 5, tzinfo=UTC),
        week_start=datetime(2026, 4, 20, 0, 0, tzinfo=UTC),
        week_end=datetime(2026, 4, 26, 23, 59, tzinfo=UTC),
        lookback_start=datetime(2026, 3, 2, 0, 0, tzinfo=UTC),
        metadata={"phase": "phase-1"},
    )


def test_language_filter_keeps_supported_rows() -> None:
    reviews = [
        ReviewDocument(
            review_id="en-1",
            product_slug="groww",
            source="playstore",
            pii_scrubbed_body="This app is helpful and the support is good.",
            locale="en-in",
        ),
        ReviewDocument(
            review_id="hi-1",
            product_slug="groww",
            source="playstore",
            pii_scrubbed_body="yah application bahut acchi hai lekin support slow hai",
            locale="hi-in",
        ),
    ]

    filtered = filter_reviews(
        reviews,
        supported_languages=parse_supported_languages("en"),
        min_text_chars=20,
    )

    assert [review.review_id for review in filtered.eligible_reviews] == ["en-1"]
    assert filtered.filtered_language == 1


def test_length_filter_removes_too_short_reviews() -> None:
    reviews = [
        ReviewDocument(
            review_id="short",
            product_slug="groww",
            source="playstore",
            pii_scrubbed_body="Too short",
            locale="en-in",
        ),
        ReviewDocument(
            review_id="long",
            product_slug="groww",
            source="playstore",
            pii_scrubbed_body="This review is definitely long enough to survive filtering.",
            locale="en-in",
        ),
    ]

    filtered = filter_reviews(
        reviews,
        supported_languages=parse_supported_languages("en"),
        min_text_chars=20,
    )

    assert [review.review_id for review in filtered.eligible_reviews] == ["long"]
    assert filtered.filtered_too_short == 1


def test_clustering_service_persists_embeddings_and_clusters(tmp_path: Path) -> None:
    settings = _build_settings(tmp_path)
    storage = Storage(settings.db_path)
    storage.initialize()
    _insert_reviews(storage)
    run_record = _build_run_record()
    service = ClusteringService(
        settings=settings,
        storage=storage,
        provider=SyntheticEmbeddingProvider(),
    )

    first_result = service.run(run_record=run_record)
    second_result = service.run(run_record=run_record)

    assert first_result.total_reviews_window == 43
    assert first_result.eligible_reviews == 40
    assert first_result.filtered_language == 1
    assert first_result.filtered_too_short == 1
    assert first_result.filtered_duplicate_body == 1
    assert first_result.cluster_count == 4
    assert first_result.noise_count == 0
    assert first_result.noise_ratio == 0.0
    assert first_result.embedding_stats.cache_hits == 0
    assert first_result.embedding_stats.cache_misses == 40
    assert {cluster.size for cluster in first_result.clusters} == {10}

    assert second_result.embedding_stats.cache_hits == 40
    assert second_result.embedding_stats.cache_misses == 0
    assert [cluster.review_ids for cluster in second_result.clusters] == [
        cluster.review_ids for cluster in first_result.clusters
    ]

    with storage.connect() as connection:
        embedding_count = connection.execute(
            "SELECT COUNT(*) AS count FROM review_embeddings WHERE run_id = ?",
            (run_record.run_id,),
        ).fetchone()
        cluster_count = connection.execute(
            "SELECT COUNT(*) AS count FROM clusters WHERE run_id = ?",
            (run_record.run_id,),
        ).fetchone()
        keyphrases_row = connection.execute(
            "SELECT keyphrases_json FROM clusters WHERE run_id = ? ORDER BY cluster_id LIMIT 1",
            (run_record.run_id,),
        ).fetchone()

    assert embedding_count is not None
    assert embedding_count["count"] == 40
    assert cluster_count is not None
    assert cluster_count["count"] == 4
    assert keyphrases_row is not None
    assert json.loads(keyphrases_row["keyphrases_json"])
