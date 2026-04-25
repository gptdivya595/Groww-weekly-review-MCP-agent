from __future__ import annotations

import json
from pathlib import Path

from agent.config import Settings
from agent.ingestion.appstore import AppStoreReviewClient
from agent.ingestion.models import RawReview, stable_review_id
from agent.ingestion.pipeline import IngestionService
from agent.ingestion.playstore import PlayStoreReviewClient
from agent.pulse_types import ProductConfig
from agent.storage import Storage
from agent.time_utils import resolve_iso_week_window

FIXTURES = Path(__file__).parent / "fixtures"


def _fixture_text(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


def _fixture_json(name: str) -> list[dict[str, object]]:
    return json.loads(_fixture_text(name))


def _review_snapshot(
    reviews: list[RawReview],
    product_slug: str,
) -> list[dict[str, str | int | None]]:
    return [
        {
            "review_id": review.review_id,
            "source": review.source.value,
            "external_id": review.external_id,
            "rating": review.rating,
            "title": review.title,
            "body": review.body,
            "pii_scrubbed_body": review.pii_scrubbed_body,
            "author_alias": review.author_alias,
            "review_updated_at": review.review_updated_at.isoformat()
            if review.review_updated_at
            else None,
            "source_url": review.source_url,
            "product_slug": product_slug,
        }
        for review in reviews
    ]


def _build_settings(tmp_path: Path) -> Settings:
    return Settings(
        db_path=tmp_path / "pulse.sqlite",
        products_file=tmp_path / "products.yaml",
        raw_data_dir=tmp_path / "raw",
        log_level="INFO",
        timezone="Asia/Kolkata",
        max_run_cost_usd=5.0,
        confirm_send=False,
        http_timeout_seconds=5.0,
        appstore_max_pages=2,
        playstore_page_size=3,
        playstore_max_pages=2,
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


def test_appstore_fixture_replay_produces_deterministic_snapshot() -> None:
    payloads = {
        1: _fixture_text("appstore_page1.xml"),
        2: _fixture_text("appstore_page2.xml"),
    }

    client = AppStoreReviewClient(
        max_pages=2,
        fetch_page=lambda app_id, country, page: payloads.get(page, ""),
    )
    window = resolve_iso_week_window("2026-W17", 8, "Asia/Kolkata")

    reviews = client.fetch_reviews(
        app_id="1404871703",
        country="in",
        lookback_start=window.lookback_start,
        week_end=window.week_end,
    )

    assert _review_snapshot(reviews, "groww") == [
        {
            "review_id": stable_review_id("appstore", "1001"),
            "source": "appstore",
            "external_id": "1001",
            "rating": 5,
            "title": "App runs well",
            "body": "Reach me at help@example.com for follow-up.",
            "pii_scrubbed_body": "Reach me at [REDACTED_EMAIL] for follow-up.",
            "author_alias": "alice",
            "review_updated_at": "2026-04-21T10:30:00+05:30",
            "source_url": "https://apps.apple.com/in/review?id=1404871703",
            "product_slug": "groww",
        },
        {
            "review_id": stable_review_id("appstore", "1002"),
            "source": "appstore",
            "external_id": "1002",
            "rating": 2,
            "title": "Needs support fixes",
            "body": "Support asked for 99999 88888 and 1234 5678 9012.",
            "pii_scrubbed_body": (
                "Support asked for [REDACTED_PHONE] and [REDACTED_AADHAAR]."
            ),
            "author_alias": "bob",
            "review_updated_at": "2026-04-18T09:00:00+05:30",
            "source_url": "https://apps.apple.com/in/review?id=1404871703",
            "product_slug": "groww",
        },
    ]


def test_playstore_fixture_replay_produces_deterministic_snapshot() -> None:
    payloads = {
        None: _fixture_json("playstore_page1.json"),
        "page-2": _fixture_json("playstore_page2.json"),
    }

    def fake_fetch_page(
        package_name: str,
        lang: str,
        country: str,
        page_size: int,
        continuation_token: str | None,
    ) -> tuple[list[dict[str, object]], str | None]:
        if continuation_token is None:
            return payloads[None], "page-2"
        return payloads["page-2"], None

    client = PlayStoreReviewClient(page_size=3, max_pages=2, fetch_page=fake_fetch_page)
    window = resolve_iso_week_window("2026-W17", 8, "Asia/Kolkata")

    reviews = client.fetch_reviews(
        package_name="com.nextbillion.groww",
        lang="en",
        country="in",
        lookback_start=window.lookback_start,
        week_end=window.week_end,
    )

    assert _review_snapshot(reviews, "groww") == [
        {
            "review_id": stable_review_id("playstore", "gp-1"),
            "source": "playstore",
            "external_id": "gp-1",
            "rating": 5,
            "title": None,
            "body": "Love the app, email me at trader@example.com",
            "pii_scrubbed_body": "Love the app, email me at [REDACTED_EMAIL]",
            "author_alias": "carol",
            "review_updated_at": "2026-04-22T08:00:00+00:00",
            "source_url": (
                "https://play.google.com/store/apps/details"
                "?id=com.nextbillion.groww&reviewId=gp-1"
            ),
            "product_slug": "groww",
        },
        {
            "review_id": stable_review_id("playstore", "gp-2"),
            "source": "playstore",
            "external_id": "gp-2",
            "rating": 1,
            "title": None,
            "body": "Portfolio page crashes, call me on +91-9876543210",
            "pii_scrubbed_body": "Portfolio page crashes, call me on [REDACTED_PHONE]",
            "author_alias": "dan",
            "review_updated_at": "2026-04-15T08:00:00+00:00",
            "source_url": (
                "https://play.google.com/store/apps/details"
                "?id=com.nextbillion.groww&reviewId=gp-2"
            ),
            "product_slug": "groww",
        },
    ]


def test_ingestion_service_writes_snapshot_and_is_idempotent(tmp_path: Path) -> None:
    settings = _build_settings(tmp_path)
    storage = Storage(settings.db_path)
    storage.initialize()

    appstore_client = AppStoreReviewClient(
        max_pages=2,
        fetch_page=lambda app_id, country, page: {
            1: _fixture_text("appstore_page1.xml"),
            2: _fixture_text("appstore_page2.xml"),
        }.get(page, ""),
    )

    def fake_playstore_fetch(
        package_name: str,
        lang: str,
        country: str,
        page_size: int,
        continuation_token: str | None,
    ) -> tuple[list[dict[str, object]], str | None]:
        if continuation_token is None:
            return _fixture_json("playstore_page1.json"), "page-2"
        return _fixture_json("playstore_page2.json"), None

    playstore_client = PlayStoreReviewClient(
        page_size=3,
        max_pages=2,
        fetch_page=fake_playstore_fetch,
    )
    service = IngestionService(
        settings=settings,
        storage=storage,
        appstore_client=appstore_client,
        playstore_client=playstore_client,
    )
    product = _build_product()
    window = resolve_iso_week_window("2026-W17", 8, "Asia/Kolkata")

    first_result = service.run(product=product, window=window, run_id="run_fixture")
    second_result = service.run(product=product, window=window, run_id="run_fixture_repeat")

    assert first_result.total_reviews == 4
    assert first_result.upsert.inserted == 4
    assert first_result.upsert.updated == 0
    assert first_result.upsert.unchanged == 0
    assert first_result.snapshot_path.exists()
    assert first_result.degraded is False
    assert {report.status for report in first_result.sources} == {"ok"}

    assert second_result.total_reviews == 4
    assert second_result.upsert.inserted == 0
    assert second_result.upsert.updated == 0
    assert second_result.upsert.unchanged == 4
    assert second_result.snapshot_path.exists()

    with first_result.snapshot_path.open("r", encoding="utf-8") as handle:
        lines = [json.loads(line) for line in handle]

    assert len(lines) == 4
    assert {line["review_id"] for line in lines} == {
        stable_review_id("appstore", "1001"),
        stable_review_id("appstore", "1002"),
        stable_review_id("playstore", "gp-1"),
        stable_review_id("playstore", "gp-2"),
    }

    with storage.connect() as connection:
        review_count = connection.execute("SELECT COUNT(*) AS count FROM reviews").fetchone()
        scrubbed_row = connection.execute(
            "SELECT pii_scrubbed_body FROM reviews WHERE external_id = ?",
            ("gp-2",),
        ).fetchone()

    assert review_count is not None
    assert review_count["count"] == 4
    assert scrubbed_row is not None
    assert scrubbed_row["pii_scrubbed_body"] == (
        "Portfolio page crashes, call me on [REDACTED_PHONE]"
    )
