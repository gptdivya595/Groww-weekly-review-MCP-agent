from __future__ import annotations

from typing import Any

from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from agent.config import Settings
from agent.telemetry import (
    bind_context,
    clear_context,
    configure_telemetry,
    record_mcp_call_latency_ms,
    record_publish_status,
    record_reviews_ingested,
    shutdown_telemetry,
    start_span,
)


def test_spans_and_ingestion_metrics_include_run_context() -> None:
    span_exporter = InMemorySpanExporter()
    metric_reader = InMemoryMetricReader()
    settings = Settings(otel_enabled=True, otel_console_fallback=False)

    try:
        configure_telemetry(
            settings,
            force=True,
            span_exporter=span_exporter,
            metric_reader=metric_reader,
        )
        bind_context(run_id="run-123", product="groww", iso_week="2026-W17")

        with start_span("orchestration.stage", {"stage": "ingest"}):
            record_reviews_ingested(source="appstore", count=42, status="ok")
    finally:
        clear_context()
        shutdown_telemetry()

    spans = span_exporter.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].attributes["run_id"] == "run-123"
    assert spans[0].attributes["product"] == "groww"
    assert spans[0].attributes["iso_week"] == "2026-W17"
    assert spans[0].attributes["stage"] == "ingest"

    data_points = _metric_data_points(metric_reader, "pulse.reviews_ingested")
    assert len(data_points) == 1
    assert data_points[0].attributes["run_id"] == "run-123"
    assert data_points[0].attributes["source"] == "appstore"
    assert data_points[0].attributes["status"] == "ok"
    assert data_points[0].value == 42


def test_mcp_and_publish_metrics_are_recorded() -> None:
    metric_reader = InMemoryMetricReader()
    settings = Settings(otel_enabled=True, otel_console_fallback=False)

    try:
        configure_telemetry(
            settings,
            force=True,
            metric_reader=metric_reader,
        )
        bind_context(run_id="run-456", product="kuvera", iso_week="2026-W18")

        record_mcp_call_latency_ms(
            duration_ms=125.5,
            method="tools/call",
            status="ok",
            tool_name="docs.append_section",
        )
        record_publish_status(
            target="gmail",
            status="completed",
            action="draft_created",
            mode="draft",
        )
    finally:
        clear_context()
        shutdown_telemetry()

    latency_points = _metric_data_points(metric_reader, "pulse.mcp_call_latency_ms")
    assert len(latency_points) == 1
    assert latency_points[0].attributes["run_id"] == "run-456"
    assert latency_points[0].attributes["tool_name"] == "docs.append_section"
    assert latency_points[0].count == 1
    assert latency_points[0].sum == 125.5

    publish_points = _metric_data_points(metric_reader, "pulse.publish_status")
    assert len(publish_points) == 1
    assert publish_points[0].attributes["target"] == "gmail"
    assert publish_points[0].attributes["action"] == "draft_created"
    assert publish_points[0].attributes["mode"] == "draft"
    assert publish_points[0].value == 1


def _metric_data_points(metric_reader: InMemoryMetricReader, metric_name: str) -> list[Any]:
    metrics_data = metric_reader.get_metrics_data()
    for resource_metric in metrics_data.resource_metrics:
        for scope_metric in resource_metric.scope_metrics:
            for metric in scope_metric.metrics:
                if metric.name == metric_name:
                    return list(metric.data.data_points)
    return []
