from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from agent.config import Settings
from agent.ingestion.appstore import AppStoreReviewClient
from agent.ingestion.models import IngestionResult, RawReview, ReviewSource, SourceIngestionReport
from agent.ingestion.playstore import PlayStoreReviewClient
from agent.logging import get_logger
from agent.pulse_types import ProductConfig, RunWindow
from agent.storage import Storage
from agent.telemetry import record_average_rating, record_reviews_ingested, start_span

MIN_REVIEW_TIME = datetime.min.replace(tzinfo=UTC)


def build_appstore_client(settings: Settings) -> AppStoreReviewClient:
    return AppStoreReviewClient(
        timeout_seconds=settings.http_timeout_seconds,
        max_pages=settings.appstore_max_pages,
    )


def build_playstore_client(settings: Settings) -> PlayStoreReviewClient:
    return PlayStoreReviewClient(
        page_size=settings.playstore_page_size,
        max_pages=settings.playstore_max_pages,
    )


def run_ingestion_for_run(
    *,
    settings: Settings,
    storage: Storage,
    product: ProductConfig,
    window: RunWindow,
    run_id: str,
) -> IngestionResult:
    service = IngestionService(
        settings=settings,
        storage=storage,
        appstore_client=build_appstore_client(settings),
        playstore_client=build_playstore_client(settings),
    )
    return service.run(product=product, window=window, run_id=run_id)


class IngestionService:
    def __init__(
        self,
        *,
        settings: Settings,
        storage: Storage,
        appstore_client: AppStoreReviewClient,
        playstore_client: PlayStoreReviewClient,
    ) -> None:
        self.settings = settings
        self.storage = storage
        self.appstore_client = appstore_client
        self.playstore_client = playstore_client
        self.logger = get_logger("pulse.ingestion")

    def run(
        self,
        *,
        product: ProductConfig,
        window: RunWindow,
        run_id: str,
    ) -> IngestionResult:
        with start_span(
            "ingestion.run",
            {
                "product_slug": product.slug,
                "iso_week": window.iso_week,
            },
        ):
            source_reports: list[SourceIngestionReport] = []
            collected_reviews: list[RawReview] = []
            configured_sources = 0

            if self._is_configured_identifier(product.app_store_app_id):
                configured_sources += 1
                report, reviews = self._fetch_appstore(product=product, window=window)
                source_reports.append(report)
                collected_reviews.extend(reviews)
            else:
                source_reports.append(
                    SourceIngestionReport(
                        source=ReviewSource.APPSTORE,
                        status="skipped",
                        error="Missing app_store_app_id",
                    )
                )

            if self._is_configured_identifier(product.google_play_package):
                configured_sources += 1
                report, reviews = self._fetch_playstore(product=product, window=window)
                source_reports.append(report)
                collected_reviews.extend(reviews)
            else:
                source_reports.append(
                    SourceIngestionReport(
                        source=ReviewSource.PLAYSTORE,
                        status="skipped",
                        error="Missing google_play_package",
                    )
                )

            if configured_sources == 0:
                raise ValueError(
                    f"Product {product.slug} is missing both App Store and Play Store identifiers."
                )

            if not collected_reviews and any(report.status == "error" for report in source_reports):
                raise RuntimeError("All configured review sources failed.")

            deduped_reviews = self._deduplicate_reviews(collected_reviews)
            snapshot_path = self._write_snapshot(product.slug, run_id, deduped_reviews)
            upsert_stats = self.storage.upsert_reviews(product.slug, deduped_reviews)
            degraded = any(report.status in {"empty", "error"} for report in source_reports)
            average_rating = _average_rating(deduped_reviews)
            if average_rating is not None:
                record_average_rating(source="combined", value=average_rating)

            return IngestionResult(
                run_id=run_id,
                product_slug=product.slug,
                iso_week=window.iso_week,
                lookback_weeks=window.lookback_weeks,
                total_reviews=len(deduped_reviews),
                snapshot_path=snapshot_path,
                degraded=degraded,
                upsert=upsert_stats,
                sources=source_reports,
            )

    def _fetch_appstore(
        self,
        *,
        product: ProductConfig,
        window: RunWindow,
    ) -> tuple[SourceIngestionReport, list[RawReview]]:
        with start_span("ingestion.source", {"source": "appstore"}):
            try:
                reviews = self.appstore_client.fetch_reviews(
                    app_id=str(product.app_store_app_id),
                    country=product.country,
                    lookback_start=window.lookback_start,
                    week_end=window.week_end,
                )
            except Exception as exc:
                record_reviews_ingested(source="appstore", count=0, status="error")
                self.logger.warning("ingestion_source_failed", source="appstore", error=str(exc))
                return (
                    SourceIngestionReport(
                        source=ReviewSource.APPSTORE,
                        status="error",
                        error=str(exc),
                    ),
                    [],
                )

            status = "ok" if reviews else "empty"
            record_reviews_ingested(source="appstore", count=len(reviews), status=status)
            return (
                SourceIngestionReport(
                    source=ReviewSource.APPSTORE,
                    status=status,
                    fetched=len(reviews),
                ),
                reviews,
            )

    def _fetch_playstore(
        self,
        *,
        product: ProductConfig,
        window: RunWindow,
    ) -> tuple[SourceIngestionReport, list[RawReview]]:
        with start_span("ingestion.source", {"source": "playstore"}):
            try:
                reviews = self.playstore_client.fetch_reviews(
                    package_name=str(product.google_play_package),
                    lang=product.lang,
                    country=product.country,
                    lookback_start=window.lookback_start,
                    week_end=window.week_end,
                )
            except Exception as exc:
                record_reviews_ingested(source="playstore", count=0, status="error")
                self.logger.warning("ingestion_source_failed", source="playstore", error=str(exc))
                return (
                    SourceIngestionReport(
                        source=ReviewSource.PLAYSTORE,
                        status="error",
                        error=str(exc),
                    ),
                    [],
                )

            status = "ok" if reviews else "empty"
            record_reviews_ingested(source="playstore", count=len(reviews), status=status)
            return (
                SourceIngestionReport(
                    source=ReviewSource.PLAYSTORE,
                    status=status,
                    fetched=len(reviews),
                ),
                reviews,
            )

    def _write_snapshot(
        self,
        product_slug: str,
        run_id: str,
        reviews: list[RawReview],
    ) -> Path:
        snapshot_path = self.settings.raw_data_dir / product_slug / f"{run_id}.jsonl"
        snapshot_path.parent.mkdir(parents=True, exist_ok=True)

        with snapshot_path.open("w", encoding="utf-8") as handle:
            for review in sorted(reviews, key=self._review_sort_key):
                handle.write(json.dumps(review.audit_record(product_slug), sort_keys=True))
                handle.write("\n")

        return snapshot_path

    @staticmethod
    def _deduplicate_reviews(reviews: list[RawReview]) -> list[RawReview]:
        deduped: dict[str, RawReview] = {}

        for review in reviews:
            current = deduped.get(review.review_id)
            if current is None:
                deduped[review.review_id] = review
                continue

            if IngestionService._review_rank(review) > IngestionService._review_rank(current):
                deduped[review.review_id] = review

        return list(deduped.values())

    @staticmethod
    def _review_sort_key(review: RawReview) -> tuple[str, str, str]:
        review_time = review.review_updated_at or review.review_created_at or MIN_REVIEW_TIME
        return (review.source.value, review_time.isoformat(), review.external_id)

    @staticmethod
    def _review_rank(review: RawReview) -> tuple[str, str]:
        review_time = review.review_updated_at or review.review_created_at or MIN_REVIEW_TIME
        return (review_time.isoformat(), review.external_id)

    @staticmethod
    def _is_configured_identifier(value: str | None) -> bool:
        return bool(value and not value.lower().startswith("replace"))


def _average_rating(reviews: list[RawReview]) -> float | None:
    ratings = [review.rating for review in reviews if review.rating is not None]
    if not ratings:
        return None
    return sum(ratings) / len(ratings)
