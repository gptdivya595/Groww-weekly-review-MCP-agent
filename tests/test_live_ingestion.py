from __future__ import annotations

import os
from pathlib import Path

import pytest

from agent.config import Settings, get_product_by_slug
from agent.ingestion.pipeline import IngestionService, build_appstore_client, build_playstore_client
from agent.storage import Storage
from agent.time_utils import current_iso_week, resolve_iso_week_window


@pytest.mark.skipif(
    os.getenv("PULSE_ENABLE_LIVE_NETWORK_TESTS") != "1",
    reason="Set PULSE_ENABLE_LIVE_NETWORK_TESTS=1 to run live ingestion smoke tests.",
)
def test_live_groww_ingest_smoke(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    settings = Settings(
        db_path=tmp_path / "pulse.sqlite",
        products_file=repo_root / "products.yaml",
        raw_data_dir=tmp_path / "raw",
        log_level="INFO",
        timezone="Asia/Kolkata",
        max_run_cost_usd=5.0,
        confirm_send=False,
        http_timeout_seconds=15.0,
        appstore_max_pages=10,
        playstore_page_size=200,
        playstore_max_pages=5,
    )
    storage = Storage(settings.db_path)
    storage.initialize()
    service = IngestionService(
        settings=settings,
        storage=storage,
        appstore_client=build_appstore_client(settings),
        playstore_client=build_playstore_client(settings),
    )
    product = get_product_by_slug("groww", settings)
    iso_week = current_iso_week(settings.timezone)
    window = resolve_iso_week_window(iso_week, 8, settings.timezone)

    result = service.run(product=product, window=window, run_id="run_live_groww_smoke")

    assert result.total_reviews >= 100
    assert result.snapshot_path.exists()
    assert result.upsert.inserted == result.total_reviews
