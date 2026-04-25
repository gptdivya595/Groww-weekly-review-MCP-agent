from __future__ import annotations

import json
import sqlite3
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from agent.clustering.models import PersistedCluster, PersistedEmbedding, ReviewDocument
from agent.ingestion.models import RawReview, ReviewUpsertStats
from agent.pulse_types import ProductConfig, StoredDeliveryRecord, StoredRunRecord
from agent.summarization.models import SummarizedTheme

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS products (
    slug TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    app_store_app_id TEXT,
    google_play_package TEXT,
    google_doc_id TEXT,
    stakeholder_emails_json TEXT NOT NULL DEFAULT '[]',
    default_lookback_weeks INTEGER NOT NULL DEFAULT 10,
    country TEXT NOT NULL DEFAULT 'in',
    lang TEXT NOT NULL DEFAULT 'en',
    active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS reviews (
    review_id TEXT PRIMARY KEY,
    product_slug TEXT NOT NULL,
    source TEXT NOT NULL,
    external_id TEXT NOT NULL,
    rating INTEGER,
    title TEXT,
    body TEXT,
    author_alias TEXT,
    review_created_at TEXT,
    review_updated_at TEXT,
    locale TEXT,
    app_version TEXT,
    source_url TEXT,
    raw_payload_json TEXT,
    pii_scrubbed_body TEXT,
    body_hash TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(source, external_id)
);

CREATE TABLE IF NOT EXISTS review_embeddings (
    review_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    embedding_provider TEXT NOT NULL,
    embedding_model TEXT NOT NULL,
    vector_json TEXT,
    created_at TEXT NOT NULL,
    PRIMARY KEY (review_id, run_id, embedding_model)
);

CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    product_slug TEXT NOT NULL,
    iso_week TEXT NOT NULL,
    stage TEXT NOT NULL,
    status TEXT NOT NULL,
    lookback_weeks INTEGER NOT NULL,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    week_start TEXT NOT NULL,
    week_end TEXT NOT NULL,
    lookback_start TEXT NOT NULL,
    metadata_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS themes (
    theme_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    cluster_id TEXT,
    name TEXT,
    summary TEXT,
    keyphrases_json TEXT,
    medoid_review_id TEXT,
    quote_review_id TEXT,
    quote_text TEXT,
    action_ideas_json TEXT NOT NULL DEFAULT '[]',
    representative_review_ids_json TEXT NOT NULL DEFAULT '[]',
    coverage_count INTEGER,
    average_rating REAL,
    rating_stddev REAL,
    model_provider TEXT,
    model_name TEXT,
    low_coverage INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS clusters (
    cluster_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    label INTEGER NOT NULL,
    size INTEGER NOT NULL,
    review_ids_json TEXT NOT NULL,
    representative_review_ids_json TEXT NOT NULL,
    keyphrases_json TEXT NOT NULL,
    medoid_review_id TEXT NOT NULL,
    average_rating REAL,
    rating_stddev REAL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS deliveries (
    delivery_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    target TEXT NOT NULL,
    status TEXT NOT NULL,
    external_id TEXT,
    external_link TEXT,
    payload_hash TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
"""


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


REVIEW_COMPARE_COLUMNS = (
    "product_slug",
    "source",
    "external_id",
    "rating",
    "title",
    "body",
    "author_alias",
    "review_created_at",
    "review_updated_at",
    "locale",
    "app_version",
    "source_url",
    "raw_payload_json",
    "pii_scrubbed_body",
    "body_hash",
)

THEME_COLUMN_DEFINITIONS = {
    "quote_review_id": "TEXT",
    "quote_text": "TEXT",
    "action_ideas_json": "TEXT NOT NULL DEFAULT '[]'",
    "representative_review_ids_json": "TEXT NOT NULL DEFAULT '[]'",
    "coverage_count": "INTEGER",
    "average_rating": "REAL",
    "rating_stddev": "REAL",
    "model_provider": "TEXT",
    "model_name": "TEXT",
    "low_coverage": "INTEGER NOT NULL DEFAULT 0",
}


class Storage:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path

    def connect(self) -> sqlite3.Connection:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        return connection

    def initialize(self) -> None:
        with self.connect() as connection:
            connection.executescript(SCHEMA_SQL)
            self._ensure_table_columns(
                connection,
                table_name="themes",
                required_columns=THEME_COLUMN_DEFINITIONS,
            )

    def list_tables(self) -> list[str]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT name
                FROM sqlite_master
                WHERE type = 'table'
                ORDER BY name
                """
            ).fetchall()
        return [row["name"] for row in rows]

    def list_products(self) -> list[ProductConfig]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT
                    slug,
                    display_name,
                    app_store_app_id,
                    google_play_package,
                    google_doc_id,
                    stakeholder_emails_json,
                    default_lookback_weeks,
                    country,
                    lang,
                    active
                FROM products
                ORDER BY display_name ASC, slug ASC
                """
            ).fetchall()

        products: list[ProductConfig] = []
        for row in rows:
            products.append(
                ProductConfig(
                    slug=row["slug"],
                    display_name=row["display_name"],
                    app_store_app_id=row["app_store_app_id"],
                    google_play_package=row["google_play_package"],
                    google_doc_id=self._normalize_google_doc_id(row["google_doc_id"]),
                    stakeholder_emails=json.loads(row["stakeholder_emails_json"] or "[]"),
                    default_lookback_weeks=row["default_lookback_weeks"],
                    country=row["country"],
                    lang=row["lang"],
                    active=bool(row["active"]),
                )
            )
        return products

    def list_runs(
        self,
        *,
        limit: int = 50,
        product_slug: str | None = None,
    ) -> list[StoredRunRecord]:
        query = """
            SELECT
                run_id,
                product_slug,
                stage,
                status,
                iso_week,
                lookback_weeks,
                started_at,
                completed_at,
                week_start,
                week_end,
                lookback_start,
                metadata_json
            FROM runs
        """
        parameters: list[object] = []
        if product_slug is not None:
            query += " WHERE product_slug = ?"
            parameters.append(product_slug)
        query += " ORDER BY started_at DESC LIMIT ?"
        parameters.append(limit)

        with self.connect() as connection:
            rows = connection.execute(query, parameters).fetchall()

        return [self._build_run_record(row) for row in rows]

    def get_run(self, run_id: str) -> StoredRunRecord | None:
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT
                    run_id,
                    product_slug,
                    stage,
                    status,
                    iso_week,
                    lookback_weeks,
                    started_at,
                    completed_at,
                    week_start,
                    week_end,
                    lookback_start,
                    metadata_json
                FROM runs
                WHERE run_id = ?
                """,
                (run_id,),
            ).fetchone()

        if row is None:
            return None

        return self._build_run_record(row)

    def get_latest_run_for_product_week(
        self,
        product_slug: str,
        iso_week: str,
    ) -> StoredRunRecord | None:
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT
                    run_id,
                    product_slug,
                    stage,
                    status,
                    iso_week,
                    lookback_weeks,
                    started_at,
                    completed_at,
                    week_start,
                    week_end,
                    lookback_start,
                    metadata_json
                FROM runs
                WHERE product_slug = ? AND iso_week = ?
                ORDER BY started_at DESC
                LIMIT 1
                """,
                (product_slug, iso_week),
            ).fetchone()

        if row is None:
            return None

        return self._build_run_record(row)

    def seed_products(self, products: list[ProductConfig]) -> None:
        timestamp = utc_now_iso()
        with self.connect() as connection:
            for product in products:
                google_doc_id = self._normalize_google_doc_id(product.google_doc_id)
                connection.execute(
                    """
                    INSERT INTO products (
                        slug,
                        display_name,
                        app_store_app_id,
                        google_play_package,
                        google_doc_id,
                        stakeholder_emails_json,
                        default_lookback_weeks,
                        country,
                        lang,
                        active,
                        created_at,
                        updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(slug) DO UPDATE SET
                        display_name = excluded.display_name,
                        app_store_app_id = excluded.app_store_app_id,
                        google_play_package = excluded.google_play_package,
                        google_doc_id = COALESCE(excluded.google_doc_id, products.google_doc_id),
                        stakeholder_emails_json = excluded.stakeholder_emails_json,
                        default_lookback_weeks = excluded.default_lookback_weeks,
                        country = excluded.country,
                        lang = excluded.lang,
                        active = excluded.active,
                        updated_at = excluded.updated_at
                    """,
                    (
                        product.slug,
                        product.display_name,
                        product.app_store_app_id,
                        product.google_play_package,
                        google_doc_id,
                        json.dumps(product.stakeholder_emails),
                        product.default_lookback_weeks,
                        product.country,
                        product.lang,
                        int(product.active),
                        timestamp,
                        timestamp,
                    ),
                )

    def get_product_google_doc_id(self, product_slug: str) -> str | None:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT google_doc_id FROM products WHERE slug = ?",
                (product_slug,),
            ).fetchone()
        if row is None:
            return None
        return self._normalize_google_doc_id(row["google_doc_id"])

    def update_product_google_doc_id(self, product_slug: str, google_doc_id: str) -> None:
        normalized = self._normalize_google_doc_id(google_doc_id)
        if normalized is None:
            raise ValueError("google_doc_id must be a non-placeholder value.")

        with self.connect() as connection:
            connection.execute(
                """
                UPDATE products
                SET google_doc_id = ?, updated_at = ?
                WHERE slug = ?
                """,
                (normalized, utc_now_iso(), product_slug),
            )

    def upsert_run(
        self,
        *,
        run_id: str,
        product_slug: str,
        iso_week: str,
        stage: str,
        status: str,
        lookback_weeks: int,
        week_start: str,
        week_end: str,
        lookback_start: str,
        metadata: dict[str, object] | None = None,
    ) -> None:
        timestamp = utc_now_iso()
        payload = json.dumps(metadata or {})
        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO runs (
                    run_id,
                    product_slug,
                    iso_week,
                    stage,
                    status,
                    lookback_weeks,
                    started_at,
                    week_start,
                    week_end,
                    lookback_start,
                    metadata_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(run_id) DO UPDATE SET
                    stage = excluded.stage,
                    status = excluded.status,
                    lookback_weeks = excluded.lookback_weeks,
                    week_start = excluded.week_start,
                    week_end = excluded.week_end,
                    lookback_start = excluded.lookback_start,
                    metadata_json = excluded.metadata_json
                """,
                (
                    run_id,
                    product_slug,
                    iso_week,
                    stage,
                    status,
                    lookback_weeks,
                    timestamp,
                    week_start,
                    week_end,
                    lookback_start,
                    payload,
                ),
            )

    def update_run_status(
        self,
        run_id: str,
        *,
        status: str,
        stage: str | None = None,
        metadata: dict[str, Any] | None = None,
        completed: bool = False,
    ) -> None:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT metadata_json FROM runs WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            current_metadata = (
                json.loads(row["metadata_json"]) if row and row["metadata_json"] else {}
            )
            merged_metadata = current_metadata | (metadata or {})
            merged_payload = json.dumps(merged_metadata, sort_keys=True)

            if completed:
                connection.execute(
                    """
                    UPDATE runs
                    SET stage = COALESCE(?, stage), status = ?, completed_at = ?, metadata_json = ?
                    WHERE run_id = ?
                    """,
                    (stage, status, utc_now_iso(), merged_payload, run_id),
                )
                return

            connection.execute(
                """
                UPDATE runs
                SET stage = COALESCE(?, stage), status = ?, metadata_json = ?
                WHERE run_id = ?
                """,
                (stage, status, merged_payload, run_id),
            )

    def get_delivery(self, run_id: str, target: str) -> StoredDeliveryRecord | None:
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT
                    delivery_id,
                    run_id,
                    target,
                    status,
                    external_id,
                    external_link,
                    payload_hash,
                    created_at,
                    updated_at
                FROM deliveries
                WHERE run_id = ? AND target = ?
                ORDER BY updated_at DESC, created_at DESC
                LIMIT 1
                """,
                (run_id, target),
            ).fetchone()

        if row is None:
            return None

        return StoredDeliveryRecord(
            delivery_id=row["delivery_id"],
            run_id=row["run_id"],
            target=row["target"],
            status=row["status"],
            external_id=row["external_id"],
            external_link=row["external_link"],
            payload_hash=row["payload_hash"],
            created_at=datetime.fromisoformat(row["created_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"]),
        )

    def list_deliveries_for_run(self, run_id: str) -> list[StoredDeliveryRecord]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT
                    delivery_id,
                    run_id,
                    target,
                    status,
                    external_id,
                    external_link,
                    payload_hash,
                    created_at,
                    updated_at
                FROM deliveries
                WHERE run_id = ?
                ORDER BY created_at ASC, delivery_id ASC
                """,
                (run_id,),
            ).fetchall()

        deliveries: list[StoredDeliveryRecord] = []
        for row in rows:
            deliveries.append(
                StoredDeliveryRecord(
                    delivery_id=row["delivery_id"],
                    run_id=row["run_id"],
                    target=row["target"],
                    status=row["status"],
                    external_id=row["external_id"],
                    external_link=row["external_link"],
                    payload_hash=row["payload_hash"],
                    created_at=datetime.fromisoformat(row["created_at"]),
                    updated_at=datetime.fromisoformat(row["updated_at"]),
                )
            )
        return deliveries

    def upsert_delivery(
        self,
        *,
        run_id: str,
        target: str,
        status: str,
        external_id: str | None = None,
        external_link: str | None = None,
        payload_hash: str | None = None,
    ) -> str:
        delivery_id = f"{run_id}:{target}"
        timestamp = utc_now_iso()
        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO deliveries (
                    delivery_id,
                    run_id,
                    target,
                    status,
                    external_id,
                    external_link,
                    payload_hash,
                    created_at,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(delivery_id) DO UPDATE SET
                    status = excluded.status,
                    external_id = excluded.external_id,
                    external_link = excluded.external_link,
                    payload_hash = excluded.payload_hash,
                    updated_at = excluded.updated_at
                """,
                (
                    delivery_id,
                    run_id,
                    target,
                    status,
                    external_id,
                    external_link,
                    payload_hash,
                    timestamp,
                    timestamp,
                ),
            )
        return delivery_id

    def upsert_reviews(
        self,
        product_slug: str,
        reviews: Sequence[RawReview],
    ) -> ReviewUpsertStats:
        timestamp = utc_now_iso()
        stats = ReviewUpsertStats()

        with self.connect() as connection:
            for review in reviews:
                record = review.as_db_record(product_slug)
                existing = connection.execute(
                    """
                    SELECT
                        product_slug,
                        source,
                        external_id,
                        rating,
                        title,
                        body,
                        author_alias,
                        review_created_at,
                        review_updated_at,
                        locale,
                        app_version,
                        source_url,
                        raw_payload_json,
                        pii_scrubbed_body,
                        body_hash
                    FROM reviews
                    WHERE review_id = ?
                    """,
                    (record["review_id"],),
                ).fetchone()

                if existing is None:
                    connection.execute(
                        """
                        INSERT INTO reviews (
                            review_id,
                            product_slug,
                            source,
                            external_id,
                            rating,
                            title,
                            body,
                            author_alias,
                            review_created_at,
                            review_updated_at,
                            locale,
                            app_version,
                            source_url,
                            raw_payload_json,
                            pii_scrubbed_body,
                            body_hash,
                            created_at,
                            updated_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            record["review_id"],
                            record["product_slug"],
                            record["source"],
                            record["external_id"],
                            record["rating"],
                            record["title"],
                            record["body"],
                            record["author_alias"],
                            record["review_created_at"],
                            record["review_updated_at"],
                            record["locale"],
                            record["app_version"],
                            record["source_url"],
                            record["raw_payload_json"],
                            record["pii_scrubbed_body"],
                            record["body_hash"],
                            timestamp,
                            timestamp,
                        ),
                    )
                    stats.inserted += 1
                    continue

                if self._review_row_changed(existing, record):
                    connection.execute(
                        """
                        UPDATE reviews
                        SET
                            product_slug = ?,
                            source = ?,
                            external_id = ?,
                            rating = ?,
                            title = ?,
                            body = ?,
                            author_alias = ?,
                            review_created_at = ?,
                            review_updated_at = ?,
                            locale = ?,
                            app_version = ?,
                            source_url = ?,
                            raw_payload_json = ?,
                            pii_scrubbed_body = ?,
                            body_hash = ?,
                            updated_at = ?
                        WHERE review_id = ?
                        """,
                        (
                            record["product_slug"],
                            record["source"],
                            record["external_id"],
                            record["rating"],
                            record["title"],
                            record["body"],
                            record["author_alias"],
                            record["review_created_at"],
                            record["review_updated_at"],
                            record["locale"],
                            record["app_version"],
                            record["source_url"],
                            record["raw_payload_json"],
                            record["pii_scrubbed_body"],
                            record["body_hash"],
                            timestamp,
                            record["review_id"],
                        ),
                    )
                    stats.updated += 1
                    continue

                stats.unchanged += 1

        return stats

    def fetch_reviews_for_run(self, run_record: StoredRunRecord) -> list[ReviewDocument]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT
                    review_id,
                    product_slug,
                    source,
                    rating,
                    title,
                    body,
                    pii_scrubbed_body,
                    locale,
                    body_hash,
                    review_created_at,
                    review_updated_at
                FROM reviews
                WHERE product_slug = ?
                  AND COALESCE(review_updated_at, review_created_at) >= ?
                  AND COALESCE(review_updated_at, review_created_at) <= ?
                ORDER BY review_id
                """,
                (
                    run_record.product_slug,
                    run_record.lookback_start.isoformat(),
                    run_record.week_end.isoformat(),
                ),
            ).fetchall()

        return [self._build_review_document(row) for row in rows]

    def fetch_reviews_by_ids(self, review_ids: Sequence[str]) -> list[ReviewDocument]:
        if not review_ids:
            return []

        ordered_ids = list(dict.fromkeys(review_ids))
        placeholders = ", ".join("?" for _ in ordered_ids)
        with self.connect() as connection:
            rows = connection.execute(
                f"""
                SELECT
                    review_id,
                    product_slug,
                    source,
                    rating,
                    title,
                    body,
                    pii_scrubbed_body,
                    locale,
                    body_hash,
                    review_created_at,
                    review_updated_at
                FROM reviews
                WHERE review_id IN ({placeholders})
                """,
                ordered_ids,
            ).fetchall()

        by_id = {row["review_id"]: self._build_review_document(row) for row in rows}
        return [by_id[review_id] for review_id in ordered_ids if review_id in by_id]

    def replace_review_embeddings(
        self,
        *,
        run_id: str,
        provider_name: str,
        model_name: str,
        embeddings: Sequence[PersistedEmbedding],
    ) -> None:
        timestamp = utc_now_iso()
        with self.connect() as connection:
            connection.execute(
                """
                DELETE FROM review_embeddings
                WHERE run_id = ? AND embedding_provider = ? AND embedding_model = ?
                """,
                (run_id, provider_name, model_name),
            )
            for embedding in embeddings:
                connection.execute(
                    """
                    INSERT INTO review_embeddings (
                        review_id,
                        run_id,
                        embedding_provider,
                        embedding_model,
                        vector_json,
                        created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        embedding.review_id,
                        run_id,
                        provider_name,
                        model_name,
                        json.dumps(embedding.vector),
                        timestamp,
                    ),
                )

    def fetch_clusters_for_run(self, run_id: str) -> list[PersistedCluster]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT
                    cluster_id,
                    run_id,
                    label,
                    size,
                    review_ids_json,
                    representative_review_ids_json,
                    keyphrases_json,
                    medoid_review_id,
                    average_rating,
                    rating_stddev
                FROM clusters
                WHERE run_id = ?
                ORDER BY size DESC, cluster_id
                """,
                (run_id,),
            ).fetchall()

        return [
            PersistedCluster(
                cluster_id=row["cluster_id"],
                run_id=row["run_id"],
                label=row["label"],
                size=row["size"],
                review_ids=json.loads(row["review_ids_json"]),
                representative_review_ids=json.loads(row["representative_review_ids_json"]),
                keyphrases=json.loads(row["keyphrases_json"]),
                medoid_review_id=row["medoid_review_id"],
                average_rating=row["average_rating"],
                rating_stddev=row["rating_stddev"],
            )
            for row in rows
        ]

    def replace_clusters(
        self,
        *,
        run_id: str,
        clusters: Sequence[PersistedCluster],
    ) -> None:
        timestamp = utc_now_iso()
        with self.connect() as connection:
            connection.execute("DELETE FROM clusters WHERE run_id = ?", (run_id,))
            for cluster in clusters:
                connection.execute(
                    """
                    INSERT INTO clusters (
                        cluster_id,
                        run_id,
                        label,
                        size,
                        review_ids_json,
                        representative_review_ids_json,
                        keyphrases_json,
                        medoid_review_id,
                        average_rating,
                        rating_stddev,
                        created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        cluster.cluster_id,
                        run_id,
                        cluster.label,
                        cluster.size,
                        json.dumps(cluster.review_ids),
                        json.dumps(cluster.representative_review_ids),
                        json.dumps(cluster.keyphrases),
                        cluster.medoid_review_id,
                        cluster.average_rating,
                        cluster.rating_stddev,
                        timestamp,
                    ),
                )

    def replace_themes(
        self,
        *,
        run_id: str,
        themes: Sequence[SummarizedTheme],
    ) -> None:
        timestamp = utc_now_iso()
        with self.connect() as connection:
            connection.execute("DELETE FROM themes WHERE run_id = ?", (run_id,))
            for theme in themes:
                connection.execute(
                    """
                    INSERT INTO themes (
                        theme_id,
                        run_id,
                        cluster_id,
                        name,
                        summary,
                        keyphrases_json,
                        medoid_review_id,
                        quote_review_id,
                        quote_text,
                        action_ideas_json,
                        representative_review_ids_json,
                        coverage_count,
                        average_rating,
                        rating_stddev,
                        model_provider,
                        model_name,
                        low_coverage,
                        created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        theme.theme_id,
                        run_id,
                        theme.cluster_id,
                        theme.name,
                        theme.summary,
                        json.dumps(theme.keyphrases),
                        theme.medoid_review_id,
                        theme.quote_review_id,
                        theme.quote_text,
                        json.dumps(theme.action_ideas),
                        json.dumps(theme.representative_review_ids),
                        theme.coverage_count,
                        theme.average_rating,
                        theme.rating_stddev,
                        theme.model_provider,
                        theme.model_name,
                        int(theme.low_coverage),
                        timestamp,
                    ),
                )

    def fetch_themes_for_run(self, run_id: str) -> list[SummarizedTheme]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT
                    theme_id,
                    run_id,
                    cluster_id,
                    name,
                    summary,
                    keyphrases_json,
                    medoid_review_id,
                    quote_review_id,
                    quote_text,
                    action_ideas_json,
                    representative_review_ids_json,
                    coverage_count,
                    average_rating,
                    rating_stddev,
                    model_provider,
                    model_name,
                    low_coverage
                FROM themes
                WHERE run_id = ?
                ORDER BY coverage_count DESC, theme_id
                """,
                (run_id,),
            ).fetchall()

        themes: list[SummarizedTheme] = []
        for row in rows:
            themes.append(
                SummarizedTheme(
                    theme_id=row["theme_id"],
                    run_id=row["run_id"],
                    cluster_id=row["cluster_id"],
                    name=row["name"],
                    summary=row["summary"],
                    keyphrases=json.loads(row["keyphrases_json"] or "[]"),
                    medoid_review_id=row["medoid_review_id"],
                    quote_review_id=row["quote_review_id"],
                    quote_text=row["quote_text"],
                    action_ideas=json.loads(row["action_ideas_json"] or "[]"),
                    representative_review_ids=json.loads(
                        row["representative_review_ids_json"] or "[]"
                    ),
                    coverage_count=row["coverage_count"] or 0,
                    average_rating=row["average_rating"],
                    rating_stddev=row["rating_stddev"],
                    model_provider=row["model_provider"] or "",
                    model_name=row["model_name"] or "",
                    low_coverage=bool(row["low_coverage"]),
                )
            )
        return themes

    @staticmethod
    def _review_row_changed(
        existing: sqlite3.Row,
        record: dict[str, object | None],
    ) -> bool:
        return any(existing[column] != record[column] for column in REVIEW_COMPARE_COLUMNS)

    @staticmethod
    def _build_review_document(row: sqlite3.Row) -> ReviewDocument:
        return ReviewDocument(
            review_id=row["review_id"],
            product_slug=row["product_slug"],
            source=row["source"],
            rating=row["rating"],
            title=row["title"],
            body=row["body"],
            pii_scrubbed_body=row["pii_scrubbed_body"],
            locale=row["locale"],
            body_hash=row["body_hash"],
            review_created_at=datetime.fromisoformat(row["review_created_at"])
            if row["review_created_at"]
            else None,
            review_updated_at=datetime.fromisoformat(row["review_updated_at"])
            if row["review_updated_at"]
            else None,
        )

    @staticmethod
    def _build_run_record(row: sqlite3.Row) -> StoredRunRecord:
        return StoredRunRecord(
            run_id=row["run_id"],
            product_slug=row["product_slug"],
            stage=row["stage"],
            status=row["status"],
            iso_week=row["iso_week"],
            lookback_weeks=row["lookback_weeks"],
            started_at=datetime.fromisoformat(row["started_at"]),
            completed_at=datetime.fromisoformat(row["completed_at"])
            if row["completed_at"]
            else None,
            week_start=datetime.fromisoformat(row["week_start"]),
            week_end=datetime.fromisoformat(row["week_end"]),
            lookback_start=datetime.fromisoformat(row["lookback_start"]),
            metadata=json.loads(row["metadata_json"]) if row["metadata_json"] else {},
        )

    @staticmethod
    def _ensure_table_columns(
        connection: sqlite3.Connection,
        *,
        table_name: str,
        required_columns: dict[str, str],
    ) -> None:
        existing_columns = {
            row["name"]
            for row in connection.execute(f"PRAGMA table_info({table_name})").fetchall()
        }
        for column_name, definition in required_columns.items():
            if column_name in existing_columns:
                continue
            connection.execute(
                f"ALTER TABLE {table_name} ADD COLUMN {column_name} {definition}"
            )

    @staticmethod
    def _normalize_google_doc_id(value: str | None) -> str | None:
        if value is None:
            return None
        cleaned = value.strip()
        if not cleaned:
            return None
        if cleaned.lower().startswith("replace-with-"):
            return None
        return cleaned
