from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from agent.api import create_app
from agent.orchestration.models import PipelineRunResult
from agent.storage import Storage


def test_overview_exposes_dashboard_control_tower_fields(
    tmp_path: Path,
    monkeypatch,
) -> None:
    products_path = tmp_path / "products.yaml"
    products_path.write_text(
        """
- slug: groww
  display_name: Groww
  app_store_app_id: "1404871703"
  google_play_package: "com.nextbillion.groww"
  google_doc_id: null
  stakeholder_emails:
    - ops@example.com
  default_lookback_weeks: 10
  country: in
  lang: en
  active: true
        """.strip(),
        encoding="utf-8",
    )

    monkeypatch.setenv("PULSE_DB_PATH", str(tmp_path / "pulse.sqlite"))
    monkeypatch.setenv("PULSE_PRODUCTS_FILE", str(products_path))
    monkeypatch.setenv("PULSE_RAW_DATA_DIR", str(tmp_path / "raw"))
    monkeypatch.setenv("PULSE_EMBEDDING_CACHE_DIR", str(tmp_path / "cache"))
    monkeypatch.setenv("PULSE_ARTIFACTS_DIR", str(tmp_path / "artifacts"))
    monkeypatch.setenv("PULSE_LOCKS_DIR", str(tmp_path / "locks"))
    monkeypatch.setenv("PULSE_SCHEDULER_ENABLED", "true")
    monkeypatch.setenv("PULSE_SCHEDULER_MODE", "external-cron")
    monkeypatch.setenv("PULSE_SCHEDULER_ISO_WEEKDAY", "1")
    monkeypatch.setenv("PULSE_SCHEDULER_HOUR", "9")
    monkeypatch.setenv("PULSE_SCHEDULER_MINUTE", "0")

    app = create_app()
    with TestClient(app) as client:
        overview = client.get("/api/overview")
        services = client.get("/api/services")
        scheduler = client.get("/api/scheduler")
        issues = client.get("/api/issues")

    assert overview.status_code == 200
    assert services.status_code == 200
    assert scheduler.status_code == 200
    assert issues.status_code == 200

    payload = overview.json()
    assert {"checked_at", "stats", "scheduler", "services", "issues", "locks"} <= set(
        payload
    )
    assert payload["scheduler"]["enabled"] is True
    assert any(service["key"] == "api_backend" for service in payload["services"])
    assert isinstance(payload["issues"], list)


def test_services_report_missing_google_mcp_token_before_launching_probe(
    tmp_path: Path,
    monkeypatch,
) -> None:
    products_path = tmp_path / "products.yaml"
    products_path.write_text(
        """
- slug: groww
  display_name: Groww
  app_store_app_id: "1404871703"
  google_play_package: "com.nextbillion.groww"
  google_doc_id: null
  stakeholder_emails:
    - ops@example.com
  default_lookback_weeks: 10
  country: in
  lang: en
  active: true
        """.strip(),
        encoding="utf-8",
    )

    monkeypatch.setenv("PULSE_DB_PATH", str(tmp_path / "pulse.sqlite"))
    monkeypatch.setenv("PULSE_PRODUCTS_FILE", str(products_path))
    monkeypatch.setenv("PULSE_RAW_DATA_DIR", str(tmp_path / "raw"))
    monkeypatch.setenv("PULSE_EMBEDDING_CACHE_DIR", str(tmp_path / "cache"))
    monkeypatch.setenv("PULSE_ARTIFACTS_DIR", str(tmp_path / "artifacts"))
    monkeypatch.setenv("PULSE_LOCKS_DIR", str(tmp_path / "locks"))
    monkeypatch.setenv("PULSE_DOCS_MCP_COMMAND", "google-docs-mcp")
    monkeypatch.setenv("PULSE_GMAIL_MCP_COMMAND", "google-docs-mcp")
    monkeypatch.setenv("GOOGLE_CLIENT_ID", "test-client-id")
    monkeypatch.setenv("GOOGLE_CLIENT_SECRET", "test-client-secret")
    monkeypatch.setenv("GOOGLE_MCP_PROFILE", "pulse")
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.delenv("XDG_CONFIG_HOME", raising=False)
    monkeypatch.setenv("GOOGLE_MCP_TOKEN_JSON", "")
    monkeypatch.setenv("GOOGLE_MCP_TOKEN_B64", "")

    app = create_app()
    with TestClient(app) as client:
        response = client.get("/api/services")

    assert response.status_code == 200
    services = {service["key"]: service for service in response.json()}
    assert services["docs_mcp"]["status"] == "failed"
    assert "Google MCP auth token not found" in services["docs_mcp"]["detail"]
    assert services["gmail_mcp"]["status"] == "failed"
    assert "Google MCP auth token not found" in services["gmail_mcp"]["detail"]


def test_upload_csv_trigger_creates_csv_scoped_run(
    tmp_path: Path,
    monkeypatch,
) -> None:
    products_path = tmp_path / "products.yaml"
    products_path.write_text(
        """
- slug: groww
  display_name: Groww
  app_store_app_id: "1404871703"
  google_play_package: "com.nextbillion.groww"
  google_doc_id: null
  stakeholder_emails:
    - ops@example.com
  default_lookback_weeks: 10
  country: in
  lang: en
  active: true
        """.strip(),
        encoding="utf-8",
    )

    monkeypatch.setenv("PULSE_DB_PATH", str(tmp_path / "pulse.sqlite"))
    monkeypatch.setenv("PULSE_PRODUCTS_FILE", str(products_path))
    monkeypatch.setenv("PULSE_RAW_DATA_DIR", str(tmp_path / "raw"))
    monkeypatch.setenv("PULSE_EMBEDDING_CACHE_DIR", str(tmp_path / "cache"))
    monkeypatch.setenv("PULSE_ARTIFACTS_DIR", str(tmp_path / "artifacts"))
    monkeypatch.setenv("PULSE_LOCKS_DIR", str(tmp_path / "locks"))
    monkeypatch.setenv("GOOGLE_MCP_TOKEN_JSON", "")
    monkeypatch.setenv("GOOGLE_MCP_TOKEN_B64", "")

    def fake_run_pipeline_for_product(**kwargs) -> PipelineRunResult:  # type: ignore[no-untyped-def]
        run_id = kwargs["preferred_run_id"]
        return PipelineRunResult(
            run_id=run_id,
            product_slug=kwargs["product"].slug,
            iso_week=kwargs["iso_week"],
            target=kwargs["target"],
            status="completed",
            resumed=True,
            summary_path=tmp_path / "artifacts" / f"{run_id}.json",
            warning=None,
        )

    monkeypatch.setattr("agent.api.run_pipeline_for_product", fake_run_pipeline_for_product)

    app = create_app()
    payload = {
        "product_slug": "groww",
        "filename": "groww-upload.csv",
        "weeks": 8,
        "target": "all",
        "csv_text": (
            "source,review_id,body,rating,review_created_at\n"
            "google play,gp-upload-1,The app freezes at market open.,1,2026-04-22T08:00:00Z\n"
            "app store,ap-upload-1,Support replies too slowly.,2,2026-04-23T09:00:00Z\n"
        ),
    }

    with TestClient(app) as client:
        response = client.post("/api/triggers/upload-csv", json=payload)

    assert response.status_code == 202, response.text
    job = response.json()
    assert job["kind"] == "csv-upload"
    assert job["run_id"] is not None

    storage_path = tmp_path / "pulse.sqlite"
    from agent.storage import Storage

    storage = Storage(storage_path)
    run_record = storage.get_run(job["run_id"])

    assert run_record is not None
    assert run_record.metadata["input_mode"] == "csv_upload"
    assert run_record.metadata["upload_filename"] == "groww-upload.csv"
    assert run_record.metadata["upload_rows_in_window"] == 2
    reviews = storage.fetch_reviews_for_run(run_record)
    assert len(reviews) == 2
    assert {review.source for review in reviews} == {"playstore", "appstore"}


def test_trigger_run_returns_warning_when_same_week_delivery_is_already_satisfied(
    tmp_path: Path,
    monkeypatch,
) -> None:
    products_path = tmp_path / "products.yaml"
    products_path.write_text(
        """
- slug: groww
  display_name: Groww
  app_store_app_id: "1404871703"
  google_play_package: "com.nextbillion.groww"
  google_doc_id: doc-groww
  stakeholder_emails:
    - ops@example.com
  default_lookback_weeks: 10
  country: in
  lang: en
  active: true
        """.strip(),
        encoding="utf-8",
    )

    db_path = tmp_path / "pulse.sqlite"
    monkeypatch.setenv("PULSE_DB_PATH", str(db_path))
    monkeypatch.setenv("PULSE_PRODUCTS_FILE", str(products_path))
    monkeypatch.setenv("PULSE_RAW_DATA_DIR", str(tmp_path / "raw"))
    monkeypatch.setenv("PULSE_EMBEDDING_CACHE_DIR", str(tmp_path / "cache"))
    monkeypatch.setenv("PULSE_ARTIFACTS_DIR", str(tmp_path / "artifacts"))
    monkeypatch.setenv("PULSE_LOCKS_DIR", str(tmp_path / "locks"))
    monkeypatch.setenv("GOOGLE_MCP_TOKEN_JSON", "")
    monkeypatch.setenv("GOOGLE_MCP_TOKEN_B64", "")
    monkeypatch.setenv("PULSE_CONFIRM_SEND", "true")

    def fail_run_pipeline_for_product(**kwargs) -> PipelineRunResult:  # type: ignore[no-untyped-def]
        raise AssertionError("submit_run should short-circuit instead of launching the pipeline")

    monkeypatch.setattr("agent.api.run_pipeline_for_product", fail_run_pipeline_for_product)

    storage = Storage(db_path)
    storage.initialize()
    storage.upsert_run(
        run_id="run-existing-groww",
        product_slug="groww",
        iso_week="2026-W17",
        stage="run",
        status="completed",
        lookback_weeks=10,
        week_start="2026-04-20T00:00:00+00:00",
        week_end="2026-04-26T23:59:59+00:00",
        lookback_start="2026-02-09T00:00:00+00:00",
        metadata={
            "input_mode": "scrape",
            "render_artifact_path": str(tmp_path / "artifacts" / "render" / "groww.json"),
            "gdoc_id": "doc-groww",
            "gdoc_heading_id": "h.groww",
            "gdoc_deep_link": "https://docs.google.com/document/d/doc-groww/edit#heading=h.groww",
            "gdoc_document_url": "https://docs.google.com/document/d/doc-groww/edit",
            "docs_payload_hash": "docs-hash",
            "gmail_message_id": "msg-groww",
            "gmail_thread_id": "thread-groww",
            "gmail_thread_link": "https://mail.google.com/mail/u/0/#inbox/thread-groww",
            "gmail_payload_hash": "gmail-hash",
        },
    )
    storage.upsert_delivery(
        run_id="run-existing-groww",
        target="docs",
        status="completed",
        external_id="doc-groww#h.groww",
        external_link="https://docs.google.com/document/d/doc-groww/edit#heading=h.groww",
        payload_hash="docs-hash",
    )
    storage.upsert_delivery(
        run_id="run-existing-groww",
        target="gmail",
        status="sent",
        external_id="msg-groww",
        external_link="https://mail.google.com/mail/u/0/#inbox/thread-groww",
        payload_hash="gmail-hash",
    )

    app = create_app()
    with TestClient(app) as client:
        response = client.post(
            "/api/triggers/run",
            json={
                "product_slug": "groww",
                "iso_week": "2026-W17",
                "target": "all",
            },
        )

    assert response.status_code == 202, response.text
    payload = response.json()
    assert payload["status"] == "completed"
    assert payload["run_id"] == "run-existing-groww"
    assert "idempotent" in payload["warning"]
    assert "duplicate Gmail delivery" in payload["warning"]

    updated_run = storage.get_run("run-existing-groww")
    assert updated_run is not None
    assert updated_run.metadata["warning"] == payload["warning"]
