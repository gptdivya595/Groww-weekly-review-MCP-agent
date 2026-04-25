from __future__ import annotations

import sqlite3
from pathlib import Path

from agent.pulse_types import ProductConfig
from agent.storage import Storage


def test_initialize_creates_expected_tables(tmp_path: Path) -> None:
    storage = Storage(tmp_path / "pulse.sqlite")

    storage.initialize()

    tables = set(storage.list_tables())
    assert {
        "clusters",
        "deliveries",
        "products",
        "review_embeddings",
        "reviews",
        "runs",
        "themes",
    } <= tables


def test_seed_products_upserts_rows(tmp_path: Path) -> None:
    storage = Storage(tmp_path / "pulse.sqlite")
    storage.initialize()

    products = [
        ProductConfig(
            slug="groww",
            display_name="Groww",
            app_store_app_id="1404871703",
            google_play_package="com.nextbillion.groww",
            google_doc_id="replace-with-google-doc-id",
            stakeholder_emails=["product-team@example.com"],
            default_lookback_weeks=10,
            country="in",
            lang="en",
            active=True,
        )
    ]

    storage.seed_products(products)

    with storage.connect() as connection:
        row = connection.execute(
            "SELECT slug, display_name, google_play_package FROM products WHERE slug = ?",
            ("groww",),
        ).fetchone()

    assert row is not None
    assert row["slug"] == "groww"
    assert row["display_name"] == "Groww"
    assert row["google_play_package"] == "com.nextbillion.groww"


def test_initialize_migrates_theme_columns(tmp_path: Path) -> None:
    db_path = tmp_path / "pulse.sqlite"
    with sqlite3.connect(db_path) as connection:
        connection.execute(
            """
            CREATE TABLE themes (
                theme_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                cluster_id TEXT,
                name TEXT,
                summary TEXT,
                keyphrases_json TEXT,
                medoid_review_id TEXT,
                created_at TEXT NOT NULL
            )
            """
        )

    storage = Storage(db_path)
    storage.initialize()

    with storage.connect() as connection:
        columns = {
            row["name"] for row in connection.execute("PRAGMA table_info(themes)").fetchall()
        }

    assert {
        "quote_review_id",
        "quote_text",
        "action_ideas_json",
        "representative_review_ids_json",
        "coverage_count",
        "average_rating",
        "rating_stddev",
        "model_provider",
        "model_name",
        "low_coverage",
    } <= columns


def test_seed_products_preserves_resolved_google_doc_id(tmp_path: Path) -> None:
    storage = Storage(tmp_path / "pulse.sqlite")
    storage.initialize()

    storage.seed_products(
        [
            ProductConfig(
                slug="groww",
                display_name="Groww",
                app_store_app_id="1404871703",
                google_play_package="com.nextbillion.groww",
                google_doc_id=None,
                stakeholder_emails=["product-team@example.com"],
                default_lookback_weeks=10,
                country="in",
                lang="en",
                active=True,
            )
        ]
    )
    storage.update_product_google_doc_id("groww", "doc-resolved")
    storage.seed_products(
        [
            ProductConfig(
                slug="groww",
                display_name="Groww",
                app_store_app_id="1404871703",
                google_play_package="com.nextbillion.groww",
                google_doc_id="replace-with-google-doc-id",
                stakeholder_emails=["product-team@example.com"],
                default_lookback_weeks=10,
                country="in",
                lang="en",
                active=True,
            )
        ]
    )

    assert storage.get_product_google_doc_id("groww") == "doc-resolved"
