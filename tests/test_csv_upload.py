from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from agent.ingestion.csv_upload import parse_uploaded_reviews
from agent.ingestion.models import RawReview, ReviewSource
from agent.storage import Storage


def test_parse_uploaded_reviews_accepts_flexible_columns_and_fallback_timestamp() -> None:
    csv_text = """
platform,review,score,user
Google Play,"The app freezes whenever I refresh my portfolio.",1,Asha
App Store,"Support takes too long to reply and the ticket flow is confusing.",2,Rahul
    """.strip()

    fallback_review_time = datetime(2026, 4, 26, 18, 0, tzinfo=UTC)
    parsed = parse_uploaded_reviews(
        csv_text,
        filename="groww-reviews.csv",
        fallback_review_time=fallback_review_time,
    )

    assert parsed.total_rows == 2
    assert parsed.accepted_rows == 2
    assert parsed.skipped_rows == 0
    assert parsed.derived_timestamp_rows == 2
    assert [review.source for review in parsed.reviews] == [
        ReviewSource.PLAYSTORE,
        ReviewSource.APPSTORE,
    ]
    assert all(review.review_created_at == fallback_review_time for review in parsed.reviews)
    assert parsed.reviews[0].body == "The app freezes whenever I refresh my portfolio."


def test_run_review_links_keep_uploaded_reviews_isolated_per_run(tmp_path: Path) -> None:
    storage = Storage(tmp_path / "pulse.sqlite")
    storage.initialize()

    run_window = {
        "product_slug": "groww",
        "iso_week": "2026-W17",
        "lookback_weeks": 8,
        "week_start": datetime(2026, 4, 20, 0, 0, tzinfo=UTC).isoformat(),
        "week_end": datetime(2026, 4, 26, 23, 59, tzinfo=UTC).isoformat(),
        "lookback_start": datetime(2026, 3, 2, 0, 0, tzinfo=UTC).isoformat(),
    }

    storage.upsert_run(
        run_id="run_scrape",
        stage="ingest",
        status="completed",
        metadata={"input_mode": "scrape"},
        **run_window,
    )
    storage.upsert_run(
        run_id="run_csv",
        stage="ingest",
        status="completed",
        metadata={"input_mode": "csv_upload"},
        **run_window,
    )

    scrape_review = RawReview(
        source=ReviewSource.PLAYSTORE,
        external_id="scrape-1",
        rating=1,
        title="Scraped issue",
        body="This review should belong only to the scraped run.",
        review_created_at=datetime(2026, 4, 22, 12, 0, tzinfo=UTC),
        review_updated_at=datetime(2026, 4, 22, 12, 0, tzinfo=UTC),
    )
    upload_review = RawReview(
        source=ReviewSource.CSV_UPLOAD,
        external_id="upload-1",
        rating=2,
        title="Uploaded issue",
        body="This review should belong only to the CSV upload run.",
        review_created_at=datetime(2026, 4, 22, 13, 0, tzinfo=UTC),
        review_updated_at=datetime(2026, 4, 22, 13, 0, tzinfo=UTC),
    )
    storage.upsert_reviews("groww", [scrape_review, upload_review])
    storage.replace_run_review_ids(run_id="run_scrape", review_ids=[scrape_review.review_id])
    storage.replace_run_review_ids(run_id="run_csv", review_ids=[upload_review.review_id])

    scrape_run = storage.get_run("run_scrape")
    csv_run = storage.get_run("run_csv")

    assert scrape_run is not None
    assert csv_run is not None
    assert [review.review_id for review in storage.fetch_reviews_for_run(scrape_run)] == [
        scrape_review.review_id
    ]
    assert [review.review_id for review in storage.fetch_reviews_for_run(csv_run)] == [
        upload_review.review_id
    ]
