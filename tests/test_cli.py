from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from typer.testing import CliRunner

import agent.clustering.pipeline as clustering_pipeline
import agent.ingestion.pipeline as ingestion_pipeline
import agent.publish.docs_pipeline as docs_publish_pipeline
import agent.publish.gmail_pipeline as gmail_publish_pipeline
from agent.__main__ import app
from agent.clustering.models import PersistedCluster
from agent.ingestion.appstore import AppStoreReviewClient
from agent.ingestion.models import RawReview, ReviewSource
from agent.ingestion.playstore import PlayStoreReviewClient
from agent.mcp_client.docs_client import DocsAppendResult, DocsDocument, DocsSection
from agent.mcp_client.gmail_client import GmailDraftResult, GmailSendResult
from agent.rendering.models import DOC_SECTION_URL_PLACEHOLDER
from agent.storage import Storage
from agent.summarization.models import SummarizedTheme

runner = CliRunner()
FIXTURES = Path(__file__).parent / "fixtures"

SAMPLE_PRODUCTS_YAML = """
- slug: groww
  display_name: Groww
  app_store_app_id: "1404871703"
  google_play_package: "com.nextbillion.groww"
  google_doc_id: "replace-with-google-doc-id"
  stakeholder_emails:
    - product-team@example.com
  default_lookback_weeks: 10
  country: in
  lang: en
  active: true
"""


def test_help_shows_core_commands() -> None:
    result = runner.invoke(app, ["--help"])

    assert result.exit_code == 0
    for command in (
        "init-db",
        "ingest",
        "cluster",
        "summarize",
        "render",
        "publish",
        "run",
        "run-weekly",
        "audit-run",
    ):
        assert command in result.stdout


def test_init_db_creates_sqlite_file(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "pulse.sqlite"
    products_path = tmp_path / "products.yaml"
    products_path.write_text(SAMPLE_PRODUCTS_YAML.strip(), encoding="utf-8")

    monkeypatch.setenv("PULSE_DB_PATH", str(db_path))
    monkeypatch.setenv("PULSE_PRODUCTS_FILE", str(products_path))

    result = runner.invoke(app, ["init-db"])

    assert result.exit_code == 0, result.stdout
    assert db_path.exists()


def test_ingest_records_planned_run(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "pulse.sqlite"
    products_path = tmp_path / "products.yaml"
    raw_data_dir = tmp_path / "raw"
    products_path.write_text(SAMPLE_PRODUCTS_YAML.strip(), encoding="utf-8")

    monkeypatch.setenv("PULSE_DB_PATH", str(db_path))
    monkeypatch.setenv("PULSE_PRODUCTS_FILE", str(products_path))
    monkeypatch.setenv("PULSE_RAW_DATA_DIR", str(raw_data_dir))

    init_result = runner.invoke(app, ["init-db"])
    assert init_result.exit_code == 0, init_result.stdout

    appstore_client = AppStoreReviewClient(
        max_pages=2,
        fetch_page=lambda app_id, country, page: {
            1: (FIXTURES / "appstore_page1.xml").read_text(encoding="utf-8"),
            2: (FIXTURES / "appstore_page2.xml").read_text(encoding="utf-8"),
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
            return (
                json.loads((FIXTURES / "playstore_page1.json").read_text(encoding="utf-8")),
                "page-2",
            )
        return json.loads((FIXTURES / "playstore_page2.json").read_text(encoding="utf-8")), None

    playstore_client = PlayStoreReviewClient(
        page_size=3,
        max_pages=2,
        fetch_page=fake_playstore_fetch,
    )

    monkeypatch.setattr(
        ingestion_pipeline,
        "build_appstore_client",
        lambda settings: appstore_client,
    )
    monkeypatch.setattr(
        ingestion_pipeline,
        "build_playstore_client",
        lambda settings: playstore_client,
    )

    result = runner.invoke(
        app,
        ["ingest", "--product", "groww", "--iso-week", "2026-W17", "--weeks", "8"],
    )

    assert result.exit_code == 0, result.stdout
    assert "Ingested 4 reviews for groww" in result.stdout

    storage = Storage(db_path)
    with storage.connect() as connection:
        row = connection.execute(
            """
            SELECT product_slug, iso_week, stage, status, lookback_weeks, metadata_json
            FROM runs
            ORDER BY started_at DESC
            LIMIT 1
            """
        ).fetchone()
        review_count = connection.execute("SELECT COUNT(*) AS count FROM reviews").fetchone()

    assert row is not None
    assert row["product_slug"] == "groww"
    assert row["iso_week"] == "2026-W17"
    assert row["stage"] == "ingest"
    assert row["status"] == "completed"
    assert row["lookback_weeks"] == 8
    assert review_count is not None
    assert review_count["count"] == 4

    metadata = json.loads(row["metadata_json"])
    snapshot_path = Path(metadata["snapshot_path"])
    assert metadata["phase"] == "phase-1"
    assert metadata["total_reviews"] == 4
    assert metadata["upsert"] == {"inserted": 4, "updated": 0, "unchanged": 0}
    assert snapshot_path.exists()


def test_cluster_persists_clusters_for_existing_run(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "pulse.sqlite"
    products_path = tmp_path / "products.yaml"
    raw_data_dir = tmp_path / "raw"
    cache_dir = tmp_path / "cache"
    products_path.write_text(SAMPLE_PRODUCTS_YAML.strip(), encoding="utf-8")

    monkeypatch.setenv("PULSE_DB_PATH", str(db_path))
    monkeypatch.setenv("PULSE_PRODUCTS_FILE", str(products_path))
    monkeypatch.setenv("PULSE_RAW_DATA_DIR", str(raw_data_dir))
    monkeypatch.setenv("PULSE_EMBEDDING_CACHE_DIR", str(cache_dir))
    monkeypatch.setenv("PULSE_CLUSTER_MIN_CLUSTER_SIZE", "2")

    init_result = runner.invoke(app, ["init-db"])
    assert init_result.exit_code == 0, init_result.stdout

    appstore_client = AppStoreReviewClient(
        max_pages=2,
        fetch_page=lambda app_id, country, page: {
            1: (FIXTURES / "appstore_page1.xml").read_text(encoding="utf-8"),
            2: (FIXTURES / "appstore_page2.xml").read_text(encoding="utf-8"),
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
            return (
                json.loads((FIXTURES / "playstore_page1.json").read_text(encoding="utf-8")),
                "page-2",
            )
        return json.loads((FIXTURES / "playstore_page2.json").read_text(encoding="utf-8")), None

    monkeypatch.setattr(
        ingestion_pipeline,
        "build_appstore_client",
        lambda settings: appstore_client,
    )
    monkeypatch.setattr(
        ingestion_pipeline,
        "build_playstore_client",
        lambda settings: PlayStoreReviewClient(
            page_size=3,
            max_pages=2,
            fetch_page=fake_playstore_fetch,
        ),
    )

    ingest_result = runner.invoke(
        app,
        ["ingest", "--product", "groww", "--iso-week", "2026-W17", "--weeks", "8"],
    )
    assert ingest_result.exit_code == 0, ingest_result.stdout

    storage = Storage(db_path)
    storage.upsert_reviews(
        "groww",
        [
            RawReview(
                source=ReviewSource.PLAYSTORE,
                external_id="extra-support",
                rating=2,
                title="Extra support",
                body="Support ticket tracking is confusing and the response is very slow.",
                author_alias="extra-support",
                review_created_at=datetime(2026, 4, 22, 12, 0, tzinfo=UTC),
                review_updated_at=datetime(2026, 4, 22, 12, 0, tzinfo=UTC),
                locale="en-in",
                app_version="1.0.0",
                source_url="https://example.com/extra-support",
                raw_payload={"fixture": "extra-support"},
            ),
            RawReview(
                source=ReviewSource.PLAYSTORE,
                external_id="extra-crash",
                rating=1,
                title="Extra crash",
                body="The app freezes and crash behavior appears during portfolio refresh.",
                author_alias="extra-crash",
                review_created_at=datetime(2026, 4, 21, 12, 0, tzinfo=UTC),
                review_updated_at=datetime(2026, 4, 21, 12, 0, tzinfo=UTC),
                locale="en-in",
                app_version="1.0.0",
                source_url="https://example.com/extra-crash",
                raw_payload={"fixture": "extra-crash"},
            ),
        ],
    )
    with storage.connect() as connection:
        latest_run = connection.execute(
            "SELECT run_id FROM runs ORDER BY started_at DESC LIMIT 1"
        ).fetchone()

    assert latest_run is not None
    run_id = latest_run["run_id"]

    class SmallSyntheticProvider:
        provider_name = "synthetic"
        model_name = "synthetic-cli"

        def embed_texts(self, texts: list[str]) -> object:
            import numpy as np

            vectors = []
            for text in texts:
                lowered = text.lower()
                if "support" in lowered:
                    vectors.append([0.0, 5.0, 0.0])
                elif "frustrating" in lowered or "freezes" in lowered or "crash" in lowered:
                    vectors.append([5.0, 0.0, 0.0])
                else:
                    vectors.append([1.0, 1.0, 1.0])
            return np.asarray(vectors, dtype=np.float32)

    monkeypatch.setattr(
        clustering_pipeline,
        "build_embedding_provider",
        lambda settings: SmallSyntheticProvider(),
    )
    monkeypatch.setattr(
        clustering_pipeline.ClusteringService,
        "_reduce_dimensions",
        lambda self, embeddings: embeddings,
    )

    def deterministic_small_sample_labels(self, embeddings):  # type: ignore[no-untyped-def]
        import numpy as np

        labels = np.full(shape=(len(embeddings),), fill_value=-1, dtype=int)
        grouped_indices: dict[tuple[float, ...], list[int]] = {}
        for index, vector in enumerate(np.asarray(embeddings, dtype=np.float32).tolist()):
            grouped_indices.setdefault(tuple(float(value) for value in vector), []).append(index)

        next_label = 0
        for indices in grouped_indices.values():
            if len(indices) < self.settings.cluster_min_cluster_size:
                continue
            for index in indices:
                labels[index] = next_label
            next_label += 1

        return labels

    monkeypatch.setattr(
        clustering_pipeline.ClusteringService,
        "_cluster_embeddings",
        deterministic_small_sample_labels,
    )

    cluster_result = runner.invoke(app, ["cluster", "--run", run_id])
    assert cluster_result.exit_code == 0, cluster_result.stdout
    assert "Clustered" in cluster_result.stdout

    with storage.connect() as connection:
        run_state = connection.execute(
            "SELECT stage, status, metadata_json FROM runs WHERE run_id = ?",
            (run_id,),
        ).fetchone()
        embedding_count = connection.execute(
            "SELECT COUNT(*) AS count FROM review_embeddings WHERE run_id = ?",
            (run_id,),
        ).fetchone()
        cluster_count = connection.execute(
            "SELECT COUNT(*) AS count FROM clusters WHERE run_id = ?",
            (run_id,),
        ).fetchone()

    assert run_state is not None
    assert run_state["stage"] == "cluster"
    assert run_state["status"] == "completed"
    metadata = json.loads(run_state["metadata_json"])
    assert metadata["phase"] == "phase-2"
    assert embedding_count is not None
    assert embedding_count["count"] >= 1
    assert cluster_count is not None
    assert cluster_count["count"] >= 1


def test_summarize_persists_themes_for_existing_run(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "pulse.sqlite"
    products_path = tmp_path / "products.yaml"
    products_path.write_text(SAMPLE_PRODUCTS_YAML.strip(), encoding="utf-8")

    monkeypatch.setenv("PULSE_DB_PATH", str(db_path))
    monkeypatch.setenv("PULSE_PRODUCTS_FILE", str(products_path))
    monkeypatch.setenv("PULSE_SUMMARIZATION_LOW_COVERAGE_THRESHOLD", "4")

    init_result = runner.invoke(app, ["init-db"])
    assert init_result.exit_code == 0, init_result.stdout

    storage = Storage(db_path)
    run_id = "run_phase3_cli"
    storage.upsert_run(
        run_id=run_id,
        product_slug="groww",
        iso_week="2026-W17",
        stage="cluster",
        status="completed",
        lookback_weeks=8,
        week_start=datetime(2026, 4, 20, 0, 0, tzinfo=UTC).isoformat(),
        week_end=datetime(2026, 4, 26, 23, 59, tzinfo=UTC).isoformat(),
        lookback_start=datetime(2026, 3, 2, 0, 0, tzinfo=UTC).isoformat(),
        metadata={"phase": "phase-2"},
    )

    reviews = [
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
            external_id="support-1",
            rating=2,
            title="Slow support",
            body="Support takes days to reply and ticket status is never clear.",
            author_alias="support-1",
            review_created_at=datetime(2026, 4, 22, 9, 0, tzinfo=UTC),
            review_updated_at=datetime(2026, 4, 22, 9, 0, tzinfo=UTC),
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
            review_created_at=datetime(2026, 4, 23, 9, 0, tzinfo=UTC),
            review_updated_at=datetime(2026, 4, 23, 9, 0, tzinfo=UTC),
            locale="en-in",
            app_version="1.0.0",
            source_url="https://example.com/support-2",
            raw_payload={"fixture": "support-2"},
        ),
    ]
    storage.upsert_reviews("groww", reviews)
    storage.replace_clusters(
        run_id=run_id,
        clusters=[
            PersistedCluster(
                cluster_id=f"{run_id}_cluster_01",
                run_id=run_id,
                label=0,
                size=2,
                review_ids=[reviews[0].review_id, reviews[1].review_id],
                representative_review_ids=[reviews[0].review_id],
                keyphrases=["freeze", "lag", "refresh"],
                medoid_review_id=reviews[0].review_id,
                average_rating=1.5,
                rating_stddev=0.5,
            ),
            PersistedCluster(
                cluster_id=f"{run_id}_cluster_02",
                run_id=run_id,
                label=1,
                size=2,
                review_ids=[reviews[2].review_id, reviews[3].review_id],
                representative_review_ids=[reviews[2].review_id],
                keyphrases=["support", "ticket", "response"],
                medoid_review_id=reviews[2].review_id,
                average_rating=2.0,
                rating_stddev=0.0,
            ),
        ],
    )

    result = runner.invoke(app, ["summarize", "--run", run_id])

    assert result.exit_code == 0, result.stdout
    assert "Summarized 2 themes from 2 clusters" in result.stdout

    with storage.connect() as connection:
        run_state = connection.execute(
            "SELECT stage, status, metadata_json FROM runs WHERE run_id = ?",
            (run_id,),
        ).fetchone()
        theme_count = connection.execute(
            "SELECT COUNT(*) AS count FROM themes WHERE run_id = ?",
            (run_id,),
        ).fetchone()

    assert run_state is not None
    assert run_state["stage"] == "summarize"
    assert run_state["status"] == "completed"
    metadata = json.loads(run_state["metadata_json"])
    assert metadata["phase"] == "phase-3"
    assert metadata["theme_count"] == 2
    assert theme_count is not None
    assert theme_count["count"] == 2


def test_render_persists_artifact_for_existing_run(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "pulse.sqlite"
    products_path = tmp_path / "products.yaml"
    artifacts_dir = tmp_path / "artifacts"
    products_path.write_text(SAMPLE_PRODUCTS_YAML.strip(), encoding="utf-8")

    monkeypatch.setenv("PULSE_DB_PATH", str(db_path))
    monkeypatch.setenv("PULSE_PRODUCTS_FILE", str(products_path))
    monkeypatch.setenv("PULSE_ARTIFACTS_DIR", str(artifacts_dir))
    monkeypatch.setenv("PULSE_RENDER_MAX_THEMES", "3")

    init_result = runner.invoke(app, ["init-db"])
    assert init_result.exit_code == 0, init_result.stdout

    storage = Storage(db_path)
    run_id = "run_phase4_cli"
    storage.upsert_run(
        run_id=run_id,
        product_slug="groww",
        iso_week="2026-W17",
        stage="summarize",
        status="completed",
        lookback_weeks=8,
        week_start=datetime(2026, 4, 20, 0, 0, tzinfo=UTC).isoformat(),
        week_end=datetime(2026, 4, 26, 23, 59, tzinfo=UTC).isoformat(),
        lookback_start=datetime(2026, 3, 2, 0, 0, tzinfo=UTC).isoformat(),
        metadata={"phase": "phase-3"},
    )
    storage.replace_themes(
        run_id=run_id,
        themes=[
            SummarizedTheme(
                theme_id=f"{run_id}_theme_01",
                run_id=run_id,
                cluster_id=f"{run_id}_cluster_01",
                name="App Performance & Stability",
                summary="Reviews repeatedly mention freezes during market open across 12 reviews.",
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
                quote_text="Support takes days to reply and ticket status is never clear.",
                action_ideas=["Expose clearer ticket status and expected response times."],
                representative_review_ids=["review-2"],
                coverage_count=10,
                average_rating=2.0,
                rating_stddev=0.4,
                model_provider="heuristic",
                model_name="heuristic-v1",
                low_coverage=False,
            ),
        ],
    )

    result = runner.invoke(app, ["render", "--run", run_id])

    assert result.exit_code == 0, result.stdout
    assert "Rendered 2 themes, 2 quotes, and 2 action ideas." in result.stdout

    with storage.connect() as connection:
        run_state = connection.execute(
            "SELECT stage, status, metadata_json FROM runs WHERE run_id = ?",
            (run_id,),
        ).fetchone()

    assert run_state is not None
    assert run_state["stage"] == "render"
    assert run_state["status"] == "completed"
    metadata = json.loads(run_state["metadata_json"])
    artifact_path = Path(metadata["render_artifact_path"])
    assert metadata["phase"] == "phase-4"
    assert metadata["anchor_key"] == "pulse-groww-2026-w17"
    assert artifact_path.exists()

    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
    assert artifact["docs_request_tree"]["anchor_key"] == "pulse-groww-2026-w17"
    assert artifact["email_teaser"]["docs_link_placeholder"] == DOC_SECTION_URL_PLACEHOLDER


def test_publish_docs_updates_run_and_delivery_for_existing_render(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "pulse.sqlite"
    products_path = tmp_path / "products.yaml"
    artifacts_dir = tmp_path / "artifacts"
    products_path.write_text(SAMPLE_PRODUCTS_YAML.strip(), encoding="utf-8")

    monkeypatch.setenv("PULSE_DB_PATH", str(db_path))
    monkeypatch.setenv("PULSE_PRODUCTS_FILE", str(products_path))
    monkeypatch.setenv("PULSE_ARTIFACTS_DIR", str(artifacts_dir))

    init_result = runner.invoke(app, ["init-db"])
    assert init_result.exit_code == 0, init_result.stdout

    storage = Storage(db_path)
    run_id = "run_phase5_cli"
    storage.upsert_run(
        run_id=run_id,
        product_slug="groww",
        iso_week="2026-W17",
        stage="summarize",
        status="completed",
        lookback_weeks=8,
        week_start=datetime(2026, 4, 20, 0, 0, tzinfo=UTC).isoformat(),
        week_end=datetime(2026, 4, 26, 23, 59, tzinfo=UTC).isoformat(),
        lookback_start=datetime(2026, 3, 2, 0, 0, tzinfo=UTC).isoformat(),
        metadata={"phase": "phase-3"},
    )
    storage.replace_themes(
        run_id=run_id,
        themes=[
            SummarizedTheme(
                theme_id=f"{run_id}_theme_01",
                run_id=run_id,
                cluster_id=f"{run_id}_cluster_01",
                name="App Performance & Stability",
                summary="Reviews repeatedly mention freezes during market open across 12 reviews.",
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
            )
        ],
    )

    render_result = runner.invoke(app, ["render", "--run", run_id])
    assert render_result.exit_code == 0, render_result.stdout

    class FakeDocsClient:
        def __init__(self) -> None:
            self.document_id = "doc-cli"
            self.document_url = "https://docs.google.com/document/d/doc-cli/edit"
            self.sections: list[DocsSection] = []

        def ensure_document(
            self,
            *,
            preferred_document_id: str | None,
            title: str,
        ) -> DocsDocument:
            return self.get_document(preferred_document_id or self.document_id)

        def get_document(self, document_id: str) -> DocsDocument:
            return DocsDocument(
                document_id=document_id,
                title="Weekly Review Pulse - Groww",
                document_url=self.document_url,
                text_content="\n\n".join(section.text_content for section in self.sections),
                sections=list(self.sections),
            )

        def append_section(
            self,
            *,
            document_id: str,
            request_tree: object,
        ) -> DocsAppendResult:
            heading_id = "h.cli"
            deep_link = f"{self.document_url}#heading={heading_id}"
            self.sections.append(
                DocsSection(
                    heading=request_tree.section_heading,
                    heading_id=heading_id,
                    deep_link=deep_link,
                    text_content=request_tree.markdown,
                    machine_key_line=request_tree.machine_key_line,
                )
            )
            return DocsAppendResult(
                document_id=document_id,
                heading_id=heading_id,
                deep_link=deep_link,
                document_url=self.document_url,
            )

        def close(self) -> None:
            return None

    fake_docs = FakeDocsClient()
    monkeypatch.setattr(docs_publish_pipeline, "build_docs_client", lambda settings: fake_docs)

    publish_result = runner.invoke(app, ["publish", "--run", run_id, "--target", "docs"])

    assert publish_result.exit_code == 0, publish_result.stdout
    assert "Published Docs section" in publish_result.stdout

    with storage.connect() as connection:
        run_state = connection.execute(
            "SELECT stage, status, metadata_json FROM runs WHERE run_id = ?",
            (run_id,),
        ).fetchone()
        delivery = connection.execute(
            """
            SELECT target, status, external_id, external_link, payload_hash
            FROM deliveries
            WHERE run_id = ? AND target = 'docs'
            """,
            (run_id,),
        ).fetchone()

    assert run_state is not None
    assert run_state["stage"] == "publish"
    assert run_state["status"] == "completed"
    metadata = json.loads(run_state["metadata_json"])
    assert metadata["phase"] == "phase-5"
    assert metadata["gdoc_id"] == "doc-cli"
    assert metadata["gdoc_heading_id"] == "h.cli"
    assert delivery is not None
    assert delivery["external_id"] == "doc-cli#h.cli"


def test_publish_gmail_creates_draft_after_docs_publish(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "pulse.sqlite"
    products_path = tmp_path / "products.yaml"
    artifacts_dir = tmp_path / "artifacts"
    products_path.write_text(SAMPLE_PRODUCTS_YAML.strip(), encoding="utf-8")

    monkeypatch.setenv("PULSE_DB_PATH", str(db_path))
    monkeypatch.setenv("PULSE_PRODUCTS_FILE", str(products_path))
    monkeypatch.setenv("PULSE_ARTIFACTS_DIR", str(artifacts_dir))

    init_result = runner.invoke(app, ["init-db"])
    assert init_result.exit_code == 0, init_result.stdout

    storage = Storage(db_path)
    run_id = "run_phase6_cli"
    storage.upsert_run(
        run_id=run_id,
        product_slug="groww",
        iso_week="2026-W17",
        stage="summarize",
        status="completed",
        lookback_weeks=8,
        week_start=datetime(2026, 4, 20, 0, 0, tzinfo=UTC).isoformat(),
        week_end=datetime(2026, 4, 26, 23, 59, tzinfo=UTC).isoformat(),
        lookback_start=datetime(2026, 3, 2, 0, 0, tzinfo=UTC).isoformat(),
        metadata={"phase": "phase-3"},
    )
    storage.replace_themes(
        run_id=run_id,
        themes=[
            SummarizedTheme(
                theme_id=f"{run_id}_theme_01",
                run_id=run_id,
                cluster_id=f"{run_id}_cluster_01",
                name="App Performance & Stability",
                summary="Reviews repeatedly mention freezes during market open across 12 reviews.",
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
            )
        ],
    )

    render_result = runner.invoke(app, ["render", "--run", run_id])
    assert render_result.exit_code == 0, render_result.stdout

    class FakeDocsClient:
        def __init__(self) -> None:
            self.document_id = "doc-cli-gmail"
            self.document_url = "https://docs.google.com/document/d/doc-cli-gmail/edit"
            self.sections: list[DocsSection] = []

        def ensure_document(
            self,
            *,
            preferred_document_id: str | None,
            title: str,
        ) -> DocsDocument:
            return self.get_document(preferred_document_id or self.document_id)

        def get_document(self, document_id: str) -> DocsDocument:
            return DocsDocument(
                document_id=document_id,
                title="Weekly Review Pulse - Groww",
                document_url=self.document_url,
                text_content="\n\n".join(section.text_content for section in self.sections),
                sections=list(self.sections),
            )

        def append_section(
            self,
            *,
            document_id: str,
            request_tree: object,
        ) -> DocsAppendResult:
            heading_id = "h.gmail.cli"
            deep_link = f"{self.document_url}#heading={heading_id}"
            self.sections.append(
                DocsSection(
                    heading=request_tree.section_heading,
                    heading_id=heading_id,
                    deep_link=deep_link,
                    text_content=request_tree.markdown,
                    machine_key_line=request_tree.machine_key_line,
                )
            )
            return DocsAppendResult(
                document_id=document_id,
                heading_id=heading_id,
                deep_link=deep_link,
                document_url=self.document_url,
            )

        def close(self) -> None:
            return None

    class FakeGmailClient:
        def create_draft(
            self,
            *,
            to: list[str],
            subject: str,
            plain_text_body: str,
            html_body: str,
            idempotency_key: str,
        ) -> GmailDraftResult:
            assert "doc-cli-gmail" in plain_text_body
            return GmailDraftResult(
                draft_id="draft-cli",
                thread_id="thread-cli",
                thread_link="https://mail.google.com/mail/u/0/#inbox/thread-cli",
            )

        def update_draft(
            self,
            *,
            draft_id: str,
            to: list[str],
            subject: str,
            plain_text_body: str,
            html_body: str,
            idempotency_key: str,
            thread_id: str | None = None,
        ) -> GmailDraftResult:
            return GmailDraftResult(
                draft_id=draft_id,
                thread_id=thread_id or "thread-cli",
                thread_link="https://mail.google.com/mail/u/0/#inbox/thread-cli",
            )

        def send_draft(self, *, draft_id: str) -> GmailSendResult:
            return GmailSendResult(
                message_id="msg-cli",
                draft_id=draft_id,
                thread_id="thread-cli",
                thread_link="https://mail.google.com/mail/u/0/#inbox/thread-cli",
            )

        def close(self) -> None:
            return None

    monkeypatch.setattr(
        docs_publish_pipeline,
        "build_docs_client",
        lambda settings: FakeDocsClient(),
    )
    monkeypatch.setattr(
        gmail_publish_pipeline,
        "build_gmail_client",
        lambda settings: FakeGmailClient(),
    )

    publish_result = runner.invoke(app, ["publish", "--run", run_id, "--target", "gmail"])

    assert publish_result.exit_code == 0, publish_result.stdout
    assert "Created Gmail draft" in publish_result.stdout

    with storage.connect() as connection:
        run_state = connection.execute(
            "SELECT stage, status, metadata_json FROM runs WHERE run_id = ?",
            (run_id,),
        ).fetchone()
        delivery = connection.execute(
            """
            SELECT target, status, external_id, external_link, payload_hash
            FROM deliveries
            WHERE run_id = ? AND target = 'gmail'
            """,
            (run_id,),
        ).fetchone()

    assert run_state is not None
    assert run_state["stage"] == "publish"
    assert run_state["status"] == "completed"
    metadata = json.loads(run_state["metadata_json"])
    assert metadata["phase"] == "phase-6"
    assert metadata["gmail_draft_id"] == "draft-cli"
    assert metadata["gmail_thread_id"] == "thread-cli"
    assert metadata["gdoc_id"] == "doc-cli-gmail"
    assert delivery is not None
    assert delivery["status"] == "drafted"
    assert delivery["external_id"] == "draft-cli"
