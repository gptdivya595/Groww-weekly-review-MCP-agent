from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from agent.api import create_app


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
