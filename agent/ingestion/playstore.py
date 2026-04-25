from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from agent.ingestion.models import RawReview, ReviewSource
from agent.logging import get_logger

PlayStorePageFetcher = Callable[
    [str, str, str, int, str | None],
    tuple[list[dict[str, Any]], str | None],
]


class PlayStoreReviewClient:
    def __init__(
        self,
        *,
        page_size: int = 200,
        max_pages: int = 5,
        fetch_page: PlayStorePageFetcher | None = None,
    ) -> None:
        self.page_size = page_size
        self.max_pages = max_pages
        self.fetch_page = fetch_page or self._default_fetch_page
        self.logger = get_logger("pulse.ingestion.playstore")

    def fetch_reviews(
        self,
        *,
        package_name: str,
        lang: str,
        country: str,
        lookback_start: datetime,
        week_end: datetime,
    ) -> list[RawReview]:
        continuation_token: str | None = None
        reviews: list[RawReview] = []

        for _ in range(self.max_pages):
            batch_items, continuation_token = self.fetch_page(
                package_name,
                lang,
                country,
                self.page_size,
                continuation_token,
            )

            if not batch_items:
                break

            batch_reviews = self.parse_batch(
                batch_items,
                package_name=package_name,
                lang=lang,
                country=country,
            )
            page_has_recent_reviews = False

            for review in batch_reviews:
                review_time = review.review_updated_at or review.review_created_at
                if review_time is None:
                    continue

                if review_time >= lookback_start:
                    page_has_recent_reviews = True

                if lookback_start <= review_time <= week_end:
                    reviews.append(review)

            if continuation_token is None or not page_has_recent_reviews:
                break

        return reviews

    def _default_fetch_page(
        self,
        package_name: str,
        lang: str,
        country: str,
        page_size: int,
        continuation_token: str | None,
    ) -> tuple[list[dict[str, Any]], str | None]:
        try:
            from google_play_scraper import Sort
            from google_play_scraper import reviews as google_play_reviews
        except ImportError as exc:  # pragma: no cover - dependency is required in Phase 1
            raise RuntimeError("google-play-scraper is not installed.") from exc

        results, next_token = google_play_reviews(
            app_id=package_name,
            lang=lang,
            country=country,
            sort=Sort.NEWEST,
            count=page_size,
            continuation_token=continuation_token,
        )
        return results, next_token

    def parse_batch(
        self,
        items: list[dict[str, Any]],
        *,
        package_name: str,
        lang: str,
        country: str,
    ) -> list[RawReview]:
        reviews: list[RawReview] = []

        for item in items:
            external_id = str(item.get("reviewId") or "").strip()
            if not external_id:
                self.logger.warning("playstore_review_skipped", reason="missing_review_id")
                continue

            recorded_at = self._parse_datetime(item.get("at") or item.get("updated"))
            rating = self._coerce_int(item.get("score"))

            reviews.append(
                RawReview(
                    source=ReviewSource.PLAYSTORE,
                    external_id=external_id,
                    rating=rating,
                    title=self._coerce_text(item.get("title")),
                    body=self._coerce_text(item.get("content")),
                    author_alias=self._coerce_text(item.get("userName")),
                    review_created_at=recorded_at,
                    review_updated_at=recorded_at,
                    locale=f"{lang.lower()}-{country.lower()}",
                    app_version=self._coerce_text(
                        item.get("appVersion") or item.get("reviewCreatedVersion")
                    ),
                    source_url=(
                        "https://play.google.com/store/apps/details"
                        f"?id={package_name}&reviewId={external_id}"
                    ),
                    raw_payload=item,
                )
            )

        return reviews

    @staticmethod
    def _coerce_int(value: Any) -> int | None:
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _coerce_text(value: Any) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    @staticmethod
    def _parse_datetime(value: Any) -> datetime | None:
        if value is None:
            return None

        if isinstance(value, datetime):
            return value if value.tzinfo is not None else value.replace(tzinfo=UTC)

        text = str(value).strip()
        if not text:
            return None

        parsed = datetime.fromisoformat(text)
        return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)
