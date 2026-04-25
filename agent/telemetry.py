from __future__ import annotations

import atexit
import logging
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any

from opentelemetry import metrics, trace
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import (
    ConsoleMetricExporter,
    MetricReader,
    PeriodicExportingMetricReader,
)
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    ConsoleSpanExporter,
    SpanExporter,
)

from agent.config import Settings

_DEFAULT_SERVICE_NAME = "weekly-product-review-pulse"
_TELEMETRY_CONTEXT: ContextVar[dict[str, Any]] = ContextVar(
    "pulse_telemetry_context",
    default={},
)
_LOGGER = logging.getLogger("pulse.telemetry")


@dataclass(slots=True)
class TelemetryHandle:
    tracer: Any
    meter: Any
    reviews_ingested: Any
    review_average_rating: Any
    clusters_formed: Any
    themes_generated: Any
    llm_tokens: Any
    llm_cost_usd: Any
    llm_schema_failures: Any
    mcp_call_latency_ms: Any
    publish_status: Any
    stage_duration_ms: Any
    tracer_provider: TracerProvider | None = None
    meter_provider: MeterProvider | None = None

    def shutdown(self) -> None:
        if self.meter_provider is not None:
            with suppress(Exception):
                self.meter_provider.shutdown()
        if self.tracer_provider is not None:
            with suppress(Exception):
                self.tracer_provider.shutdown()


_configured_signature: tuple[Any, ...] | None = None
_atexit_registered = False
_llm_input_cost_per_million_usd = 0.0
_llm_output_cost_per_million_usd = 0.0


def configure_telemetry(
    settings: Settings,
    *,
    force: bool = False,
    span_exporter: SpanExporter | None = None,
    metric_reader: MetricReader | None = None,
) -> TelemetryHandle:
    global _configured_signature, _llm_input_cost_per_million_usd, _llm_output_cost_per_million_usd
    global _telemetry

    signature = (
        settings.otel_enabled,
        settings.otel_service_name,
        settings.otel_traces_endpoint,
        settings.otel_metrics_endpoint,
        settings.otel_console_fallback,
        settings.otel_export_interval_seconds,
        settings.llm_input_cost_per_million_usd,
        settings.llm_output_cost_per_million_usd,
    )
    if not force and _configured_signature == signature:
        return _telemetry

    shutdown_telemetry()
    _llm_input_cost_per_million_usd = settings.llm_input_cost_per_million_usd
    _llm_output_cost_per_million_usd = settings.llm_output_cost_per_million_usd

    if not settings.otel_enabled:
        _telemetry = _build_noop_handle(settings.otel_service_name)
        _configured_signature = signature
        _register_atexit_shutdown()
        return _telemetry

    resource = Resource.create({SERVICE_NAME: settings.otel_service_name})
    tracer_provider = TracerProvider(resource=resource)
    resolved_span_exporter = span_exporter or _build_span_exporter(settings)
    if resolved_span_exporter is not None:
        tracer_provider.add_span_processor(BatchSpanProcessor(resolved_span_exporter))

    metric_readers: list[MetricReader] = []
    resolved_metric_reader = metric_reader or _build_metric_reader(settings)
    if resolved_metric_reader is not None:
        metric_readers.append(resolved_metric_reader)
    meter_provider = MeterProvider(resource=resource, metric_readers=metric_readers)

    _telemetry = _build_handle(
        service_name=settings.otel_service_name,
        tracer_provider=tracer_provider,
        meter_provider=meter_provider,
    )
    _configured_signature = signature
    _register_atexit_shutdown()
    return _telemetry


def shutdown_telemetry() -> None:
    global _configured_signature, _llm_input_cost_per_million_usd, _llm_output_cost_per_million_usd
    global _telemetry
    _telemetry.shutdown()
    _telemetry = _build_noop_handle(_DEFAULT_SERVICE_NAME)
    _configured_signature = None
    _llm_input_cost_per_million_usd = 0.0
    _llm_output_cost_per_million_usd = 0.0


def bind_context(**attributes: Any) -> None:
    current = dict(_TELEMETRY_CONTEXT.get())
    for key, value in attributes.items():
        if value is None:
            continue
        current[key] = value
    _TELEMETRY_CONTEXT.set(current)


def clear_context() -> None:
    _TELEMETRY_CONTEXT.set({})


def current_attributes(extra: dict[str, Any] | None = None) -> dict[str, Any]:
    merged = dict(_TELEMETRY_CONTEXT.get())
    if extra:
        for key, value in extra.items():
            if value is None:
                continue
            merged[key] = value
    return merged


@contextmanager
def start_span(name: str, attributes: dict[str, Any] | None = None) -> Iterator[Any]:
    with _telemetry.tracer.start_as_current_span(
        name,
        attributes=current_attributes(attributes),
    ) as span:
        yield span


def record_reviews_ingested(*, source: str, count: int, status: str) -> None:
    _telemetry.reviews_ingested.add(
        count,
        current_attributes(
            {
                "source": source,
                "status": status,
            }
        ),
    )


def record_average_rating(*, source: str, value: float) -> None:
    _telemetry.review_average_rating.record(
        value,
        current_attributes(
            {
                "source": source,
            }
        ),
    )


def record_clusters_formed(*, count: int, embedding_provider: str, embedding_model: str) -> None:
    _telemetry.clusters_formed.add(
        count,
        current_attributes(
            {
                "embedding_provider": embedding_provider,
                "embedding_model": embedding_model,
            }
        ),
    )


def record_themes_generated(*, count: int, provider: str, model: str) -> None:
    _telemetry.themes_generated.add(
        count,
        current_attributes(
            {
                "provider": provider,
                "model": model,
            }
        ),
    )


def record_llm_tokens(
    *,
    provider: str,
    model: str,
    input_tokens: int = 0,
    output_tokens: int = 0,
) -> None:
    attributes = current_attributes({"provider": provider, "model": model})
    if input_tokens > 0:
        _telemetry.llm_tokens.add(
            input_tokens,
            attributes | {"direction": "input"},
        )
    if output_tokens > 0:
        _telemetry.llm_tokens.add(
            output_tokens,
            attributes | {"direction": "output"},
        )
    estimated_cost = (
        (input_tokens * _llm_input_cost_per_million_usd)
        + (output_tokens * _llm_output_cost_per_million_usd)
    ) / 1_000_000
    if estimated_cost > 0:
        _telemetry.llm_cost_usd.add(estimated_cost, attributes)


def record_llm_schema_failure(*, provider: str, model: str, reason: str) -> None:
    _telemetry.llm_schema_failures.add(
        1,
        current_attributes(
            {
                "provider": provider,
                "model": model,
                "reason": reason,
            }
        ),
    )


def record_mcp_call_latency_ms(
    *,
    duration_ms: float,
    method: str,
    status: str,
    tool_name: str | None = None,
) -> None:
    _telemetry.mcp_call_latency_ms.record(
        duration_ms,
        current_attributes(
            {
                "method": method,
                "status": status,
                "tool_name": tool_name or "",
            }
        ),
    )


def record_publish_status(
    *,
    target: str,
    status: str,
    action: str,
    mode: str | None = None,
) -> None:
    _telemetry.publish_status.add(
        1,
        current_attributes(
            {
                "target": target,
                "status": status,
                "action": action,
                "mode": mode or "",
            }
        ),
    )


def record_stage_duration(*, stage: str, duration_ms: int, status: str) -> None:
    _telemetry.stage_duration_ms.record(
        float(duration_ms),
        current_attributes(
            {
                "stage": stage,
                "status": status,
            }
        ),
    )


def _build_noop_handle(service_name: str) -> TelemetryHandle:
    return _build_handle(service_name=service_name, tracer_provider=None, meter_provider=None)


def _build_handle(
    *,
    service_name: str,
    tracer_provider: TracerProvider | None,
    meter_provider: MeterProvider | None,
) -> TelemetryHandle:
    tracer = (
        tracer_provider.get_tracer(service_name)
        if tracer_provider is not None
        else trace.get_tracer(service_name)
    )
    meter = (
        meter_provider.get_meter(service_name)
        if meter_provider is not None
        else metrics.get_meter(service_name)
    )
    return TelemetryHandle(
        tracer=tracer,
        meter=meter,
        reviews_ingested=meter.create_counter(
            "pulse.reviews_ingested",
            unit="1",
            description="Number of store reviews ingested.",
        ),
        review_average_rating=meter.create_histogram(
            "pulse.review_average_rating",
            unit="1",
            description="Average rating captured for the current review window.",
        ),
        clusters_formed=meter.create_counter(
            "pulse.clusters_formed",
            unit="1",
            description="Number of review clusters formed.",
        ),
        themes_generated=meter.create_counter(
            "pulse.themes_generated",
            unit="1",
            description="Number of summarized themes generated.",
        ),
        llm_tokens=meter.create_counter(
            "pulse.llm_tokens",
            unit="1",
            description="Tokens consumed by LLM requests.",
        ),
        llm_cost_usd=meter.create_counter(
            "pulse.llm_cost_usd",
            unit="USD",
            description="Estimated LLM cost recorded from token usage.",
        ),
        llm_schema_failures=meter.create_counter(
            "pulse.llm_schema_failures",
            unit="1",
            description="Structured-output failures raised during summarization.",
        ),
        mcp_call_latency_ms=meter.create_histogram(
            "pulse.mcp_call_latency_ms",
            unit="ms",
            description="Latency of MCP requests.",
        ),
        publish_status=meter.create_counter(
            "pulse.publish_status",
            unit="1",
            description="Publish outcomes for Docs and Gmail delivery.",
        ),
        stage_duration_ms=meter.create_histogram(
            "pulse.stage_duration_ms",
            unit="ms",
            description="Duration of orchestration stages.",
        ),
        tracer_provider=tracer_provider,
        meter_provider=meter_provider,
    )


def _build_span_exporter(settings: Settings) -> SpanExporter | None:
    try:
        if settings.otel_traces_endpoint:
            return OTLPSpanExporter(endpoint=settings.otel_traces_endpoint)
        if settings.otel_console_fallback:
            return ConsoleSpanExporter()
    except Exception as exc:
        _LOGGER.warning(
            "telemetry_trace_exporter_failed endpoint=%s error=%s",
            settings.otel_traces_endpoint,
            exc,
        )
        if settings.otel_console_fallback:
            return ConsoleSpanExporter()
    return None


def _build_metric_reader(settings: Settings) -> MetricReader | None:
    export_interval_millis = max(1000, int(settings.otel_export_interval_seconds * 1000))
    try:
        if settings.otel_metrics_endpoint:
            return PeriodicExportingMetricReader(
                OTLPMetricExporter(endpoint=settings.otel_metrics_endpoint),
                export_interval_millis=export_interval_millis,
            )
        if settings.otel_console_fallback:
            return PeriodicExportingMetricReader(
                ConsoleMetricExporter(),
                export_interval_millis=export_interval_millis,
            )
    except Exception as exc:
        _LOGGER.warning(
            "telemetry_metric_exporter_failed endpoint=%s error=%s",
            settings.otel_metrics_endpoint,
            exc,
        )
        if settings.otel_console_fallback:
            return PeriodicExportingMetricReader(
                ConsoleMetricExporter(),
                export_interval_millis=export_interval_millis,
            )
    return None


_telemetry = _build_noop_handle(_DEFAULT_SERVICE_NAME)


def _register_atexit_shutdown() -> None:
    global _atexit_registered
    if _atexit_registered:
        return
    atexit.register(shutdown_telemetry)
    _atexit_registered = True
