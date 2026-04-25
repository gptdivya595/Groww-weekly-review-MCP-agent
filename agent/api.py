from __future__ import annotations

import json
import os
from collections.abc import AsyncIterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager, suppress
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Lock
from time import perf_counter
from typing import Any, cast
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Query, Request, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from agent.config import get_product_by_slug, get_settings, load_products
from agent.logging import configure_logging, get_logger
from agent.mcp_client.docs_client import DocsMcpClient
from agent.mcp_client.gmail_client import GmailMcpClient
from agent.orchestration.models import WeeklyBatchResult
from agent.orchestration.pipeline import (
    build_run_audit_payload,
    run_pipeline_for_product,
    run_weekly_for_products,
)
from agent.pulse_types import DeliveryTarget, ProductConfig, StoredDeliveryRecord, StoredRunRecord
from agent.storage import Storage
from agent.telemetry import configure_telemetry
from agent.time_utils import current_iso_week, next_weekly_schedule_time


class ApiDeliverySummary(BaseModel):
    target: str
    status: str
    external_id: str | None = None
    external_link: str | None = None
    payload_hash: str | None = None
    updated_at: datetime


class ApiRunSummary(BaseModel):
    run_id: str
    product_slug: str
    iso_week: str
    stage: str
    status: str
    lookback_weeks: int
    started_at: datetime
    completed_at: datetime | None = None
    docs_status: str | None = None
    gmail_status: str | None = None
    warning: str | None = None
    summary_path: str | None = None


class ApiRunDetail(BaseModel):
    run: ApiRunSummary
    deliveries: list[ApiDeliverySummary]
    audit: dict[str, Any]


class ApiReadinessCheck(BaseModel):
    key: str
    label: str
    status: str
    detail: str


class ApiProductStatus(BaseModel):
    slug: str
    display_name: str
    active: bool
    default_lookback_weeks: int
    app_store_configured: bool
    play_store_configured: bool
    stakeholder_count: int
    google_doc_configured: bool
    issues: list[str] = Field(default_factory=list)
    latest_run: ApiRunSummary | None = None


class ApiPhaseStatus(BaseModel):
    phase: str
    title: str
    implementation_status: str
    end_to_end_status: str
    notes: list[str] = Field(default_factory=list)


class ApiCompletionAudit(BaseModel):
    overall_status: str
    notes: list[str] = Field(default_factory=list)
    phases: list[ApiPhaseStatus]


class ApiServiceStatus(BaseModel):
    key: str
    label: str
    category: str
    status: str
    detail: str
    checked_at: datetime
    active: bool = False
    product_slug: str | None = None
    run_id: str | None = None
    latency_ms: int | None = None


class ApiIssueSnapshot(BaseModel):
    issue_id: str
    severity: str
    source: str
    title: str
    detail: str
    observed_at: datetime
    product_slug: str | None = None
    run_id: str | None = None


class ApiLockSnapshot(BaseModel):
    key: str
    status: str
    product_slug: str
    iso_week: str
    path: str
    acquired_at: datetime | None = None
    age_seconds: int | None = None
    pid: int | None = None
    detail: str


class ApiSchedulerStatus(BaseModel):
    enabled: bool
    mode: str
    status: str
    timezone: str
    cadence: str
    detail: str
    next_run_at: datetime | None = None
    last_started_at: datetime | None = None
    last_success_at: datetime | None = None


class ApiDashboardStats(BaseModel):
    active_products: int
    active_services: int
    running_jobs: int
    recorded_deliveries: int
    ready_services: int
    warning_services: int
    failed_services: int
    open_issues: int
    active_locks: int
    failed_runs_last_24h: int


class ApiOverviewResponse(BaseModel):
    checked_at: datetime
    stats: ApiDashboardStats
    scheduler: ApiSchedulerStatus
    services: list[ApiServiceStatus]
    issues: list[ApiIssueSnapshot]
    locks: list[ApiLockSnapshot]
    readiness: list[ApiReadinessCheck]
    completion: ApiCompletionAudit
    products: list[ApiProductStatus]
    recent_runs: list[ApiRunSummary]
    jobs: list[ApiJobSnapshot]


class TriggerRunRequest(BaseModel):
    product_slug: str
    iso_week: str | None = None
    weeks: int | None = Field(default=None, ge=1)
    target: DeliveryTarget = DeliveryTarget.ALL


class TriggerWeeklyRequest(BaseModel):
    iso_week: str | None = None
    weeks: int | None = Field(default=None, ge=1)
    target: DeliveryTarget = DeliveryTarget.ALL


class ApiJobItem(BaseModel):
    product_slug: str
    status: str
    run_id: str | None = None
    summary_path: str | None = None
    error: str | None = None


class ApiJobSnapshot(BaseModel):
    job_id: str
    kind: str
    status: str
    submitted_at: datetime
    started_at: datetime | None = None
    completed_at: datetime | None = None
    iso_week: str | None = None
    product_slug: str | None = None
    target: str
    run_id: str | None = None
    summary_path: str | None = None
    error: str | None = None
    items: list[ApiJobItem] = Field(default_factory=list)


ApiOverviewResponse.model_rebuild()


class ApiRuntime:
    def __init__(self) -> None:
        self.settings = get_settings()
        configure_logging(self.settings.log_level)
        configure_telemetry(self.settings)
        self.logger = get_logger("pulse.api")
        self.storage = Storage(self.settings.db_path)
        self.storage.initialize()
        self.executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="pulse-api")
        self._jobs: dict[str, ApiJobSnapshot] = {}
        self._jobs_lock = Lock()
        self._service_probe_lock = Lock()
        self._service_probe_cache: dict[str, tuple[float, ApiServiceStatus]] = {}
        self.refresh_products()

    def refresh_products(self) -> list[ProductConfig]:
        products = load_products(self.settings)
        self.storage.seed_products(products)
        return products

    def list_jobs(self) -> list[ApiJobSnapshot]:
        with self._jobs_lock:
            jobs = list(self._jobs.values())
        return sorted(jobs, key=lambda item: item.submitted_at, reverse=True)

    def submit_run(self, request: TriggerRunRequest) -> ApiJobSnapshot:
        products = self.refresh_products()
        if request.product_slug not in {product.slug for product in products}:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Unknown product slug: {request.product_slug}",
            )

        resolved_iso_week = request.iso_week or current_iso_week(self.settings.timezone)
        job = ApiJobSnapshot(
            job_id=uuid4().hex,
            kind="single-run",
            status="queued",
            submitted_at=datetime.now(UTC),
            iso_week=resolved_iso_week,
            product_slug=request.product_slug,
            target=request.target.value,
        )
        self._store_job(job)
        self.executor.submit(self._execute_single_run, job.job_id, request, resolved_iso_week)

        discovered_run = self._wait_for_run_id(request.product_slug, resolved_iso_week)
        if discovered_run is not None:
            self._update_job(job.job_id, run_id=discovered_run.run_id, status="running")
        return self._get_job(job.job_id)

    def submit_weekly(self, request: TriggerWeeklyRequest) -> ApiJobSnapshot:
        products = self.refresh_products()
        resolved_iso_week = request.iso_week or current_iso_week(self.settings.timezone)
        job = ApiJobSnapshot(
            job_id=uuid4().hex,
            kind="weekly-batch",
            status="queued",
            submitted_at=datetime.now(UTC),
            iso_week=resolved_iso_week,
            target=request.target.value,
            items=[
                ApiJobItem(product_slug=product.slug, status="queued")
                for product in products
                if product.active
            ],
        )
        self._store_job(job)
        self.executor.submit(self._execute_weekly_run, job.job_id, request, resolved_iso_week)
        return self._get_job(job.job_id)

    def build_overview(self, *, limit: int = 20) -> ApiOverviewResponse:
        products = self.refresh_products()
        run_records = self.storage.list_runs(limit=max(200, limit, len(products) * 20))
        jobs = self.list_jobs()
        latest_run_by_product: dict[str, ApiRunSummary] = {}
        serialized_runs: list[ApiRunSummary] = []
        for run_record in run_records[:limit]:
            serialized = self._serialize_run(run_record)
            serialized_runs.append(serialized)
            latest_run_by_product.setdefault(run_record.product_slug, serialized)

        product_statuses = [
            self._build_product_status(product, latest_run_by_product.get(product.slug))
            for product in products
        ]
        readiness = self._build_readiness_checks(product_statuses)
        completion = self._build_completion_audit(readiness)
        locks = self._build_lock_snapshots()
        scheduler = self._build_scheduler_status(run_records=run_records, jobs=jobs)
        services = self._build_service_statuses(
            products=product_statuses,
            readiness=readiness,
            run_records=run_records,
            jobs=jobs,
            scheduler=scheduler,
            locks=locks,
        )
        issues = self._build_issue_feed(
            products=product_statuses,
            readiness=readiness,
            recent_runs=serialized_runs,
            jobs=jobs,
            scheduler=scheduler,
            locks=locks,
        )
        stats = self._build_dashboard_stats(
            products=product_statuses,
            jobs=jobs,
            services=services,
            issues=issues,
            locks=locks,
            run_records=run_records,
        )

        return ApiOverviewResponse(
            checked_at=datetime.now(UTC),
            stats=stats,
            scheduler=scheduler,
            services=services,
            issues=issues,
            locks=locks,
            readiness=readiness,
            completion=completion,
            products=product_statuses,
            recent_runs=serialized_runs,
            jobs=jobs,
        )

    def build_run_detail(self, run_id: str) -> ApiRunDetail:
        run_record = self.storage.get_run(run_id)
        if run_record is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Unknown run id: {run_id}",
            )
        deliveries = self.storage.list_deliveries_for_run(run_id)
        return ApiRunDetail(
            run=self._serialize_run(run_record, deliveries=deliveries),
            deliveries=[self._serialize_delivery(item) for item in deliveries],
            audit=build_run_audit_payload(storage=self.storage, run_record=run_record),
        )

    def shutdown(self) -> None:
        self.executor.shutdown(wait=False, cancel_futures=False)

    def _execute_single_run(
        self,
        job_id: str,
        request: TriggerRunRequest,
        resolved_iso_week: str,
    ) -> None:
        self._update_job(job_id, status="running", started_at=datetime.now(UTC))
        try:
            product = get_product_by_slug(request.product_slug, self.settings)
            result = run_pipeline_for_product(
                settings=self.settings,
                storage=self.storage,
                product=product,
                iso_week=resolved_iso_week,
                lookback_weeks=request.weeks,
                target=request.target,
            )
            self._update_job(
                job_id,
                status="completed",
                completed_at=datetime.now(UTC),
                run_id=result.run_id,
                summary_path=str(result.summary_path),
            )
        except Exception as exc:
            latest_run = self.storage.get_latest_run_for_product_week(
                request.product_slug,
                resolved_iso_week,
            )
            self.logger.exception(
                "api_single_run_failed",
                job_id=job_id,
                product=request.product_slug,
                iso_week=resolved_iso_week,
                error=str(exc),
            )
            self._update_job(
                job_id,
                status="failed",
                completed_at=datetime.now(UTC),
                run_id=latest_run.run_id if latest_run is not None else None,
                summary_path=(
                    str(self._summary_path(latest_run))
                    if latest_run is not None and self._summary_path(latest_run) is not None
                    else None
                ),
                error=str(exc),
            )

    def _execute_weekly_run(
        self,
        job_id: str,
        request: TriggerWeeklyRequest,
        resolved_iso_week: str,
    ) -> None:
        self._update_job(job_id, status="running", started_at=datetime.now(UTC))
        try:
            products = self.refresh_products()
            result = run_weekly_for_products(
                settings=self.settings,
                storage=self.storage,
                products=products,
                iso_week=resolved_iso_week,
                lookback_weeks=request.weeks,
                target=request.target,
            )
            self._update_job(
                job_id,
                status="completed" if result.failed_count == 0 else "failed",
                completed_at=datetime.now(UTC),
                items=self._serialize_weekly_items(result),
                error=(
                    None
                    if result.failed_count == 0
                    else f"{result.failed_count} product runs failed."
                ),
            )
        except Exception as exc:
            self.logger.exception(
                "api_weekly_run_failed",
                job_id=job_id,
                iso_week=resolved_iso_week,
                error=str(exc),
            )
            self._update_job(
                job_id,
                status="failed",
                completed_at=datetime.now(UTC),
                error=str(exc),
            )

    def _wait_for_run_id(
        self,
        product_slug: str,
        iso_week: str,
        *,
        attempts: int = 25,
        delay_seconds: float = 0.2,
    ) -> StoredRunRecord | None:
        for _ in range(attempts):
            run_record = self.storage.get_latest_run_for_product_week(product_slug, iso_week)
            if run_record is not None:
                return run_record
            import time

            time.sleep(delay_seconds)
        return None

    def _serialize_run(
        self,
        run_record: StoredRunRecord,
        *,
        deliveries: list[StoredDeliveryRecord] | None = None,
    ) -> ApiRunSummary:
        resolved_deliveries = deliveries or self.storage.list_deliveries_for_run(run_record.run_id)
        by_target = {item.target: item for item in resolved_deliveries}
        docs_delivery = by_target.get("docs")
        gmail_delivery = by_target.get("gmail")
        return ApiRunSummary(
            run_id=run_record.run_id,
            product_slug=run_record.product_slug,
            iso_week=run_record.iso_week,
            stage=run_record.stage,
            status=run_record.status,
            lookback_weeks=run_record.lookback_weeks,
            started_at=run_record.started_at,
            completed_at=run_record.completed_at,
            docs_status=docs_delivery.status if docs_delivery is not None else None,
            gmail_status=gmail_delivery.status if gmail_delivery is not None else None,
            warning=_warning_from_metadata(run_record.metadata),
            summary_path=(
                str(summary_path)
                if (summary_path := self._summary_path(run_record)) is not None
                else None
            ),
        )

    @staticmethod
    def _serialize_delivery(delivery: StoredDeliveryRecord) -> ApiDeliverySummary:
        return ApiDeliverySummary(
            target=delivery.target,
            status=delivery.status,
            external_id=delivery.external_id,
            external_link=delivery.external_link,
            payload_hash=delivery.payload_hash,
            updated_at=delivery.updated_at,
        )

    def _build_product_status(
        self,
        product: ProductConfig,
        latest_run: ApiRunSummary | None,
    ) -> ApiProductStatus:
        issues: list[str] = []
        if not _is_identifier_configured(product.app_store_app_id):
            issues.append("App Store app id is missing or placeholder.")
        if not _is_identifier_configured(product.google_play_package):
            issues.append("Google Play package is missing or placeholder.")
        if not _has_real_stakeholders(product.stakeholder_emails):
            issues.append("Stakeholder emails are missing or still placeholder values.")
        if product.google_doc_id is None:
            issues.append(
                "Google Doc id is not set; Docs MCP must create the document on first "
                "publish."
            )

        return ApiProductStatus(
            slug=product.slug,
            display_name=product.display_name,
            active=product.active,
            default_lookback_weeks=product.default_lookback_weeks,
            app_store_configured=_is_identifier_configured(product.app_store_app_id),
            play_store_configured=_is_identifier_configured(product.google_play_package),
            stakeholder_count=len(product.stakeholder_emails),
            google_doc_configured=product.google_doc_id is not None,
            issues=issues,
            latest_run=latest_run,
        )

    def _build_readiness_checks(
        self,
        products: list[ApiProductStatus],
    ) -> list[ApiReadinessCheck]:
        checks = [
            ApiReadinessCheck(
                key="docs_mcp",
                label="Docs MCP configuration",
                status=(
                    "ready"
                    if _is_command_configured(self.settings.docs_mcp_command)
                    else "missing"
                ),
                detail=(
                    "Docs MCP command is configured."
                    if _is_command_configured(self.settings.docs_mcp_command)
                    else "Set PULSE_DOCS_MCP_COMMAND and related args before live Docs publish."
                ),
            ),
            ApiReadinessCheck(
                key="gmail_mcp",
                label="Gmail MCP configuration",
                status=(
                    "ready"
                    if _is_command_configured(self.settings.gmail_mcp_command)
                    else "missing"
                ),
                detail=(
                    "Gmail MCP command is configured."
                    if _is_command_configured(self.settings.gmail_mcp_command)
                    else "Set PULSE_GMAIL_MCP_COMMAND and related args before live Gmail publish."
                ),
            ),
            ApiReadinessCheck(
                key="openai_api_key",
                label="OpenAI API key",
                status=(
                    "ready"
                    if os.getenv("OPENAI_API_KEY")
                    else "info"
                    if self.settings.summarization_provider == "heuristic"
                    else "missing"
                ),
                detail=(
                    "OPENAI_API_KEY is present."
                    if os.getenv("OPENAI_API_KEY")
                    else (
                        "Heuristic summarization is active, so OPENAI_API_KEY is "
                        "optional right now."
                    )
                    if self.settings.summarization_provider == "heuristic"
                    else (
                        "OPENAI_API_KEY is required when using the OpenAI "
                        "summarization provider."
                    )
                ),
            ),
        ]

        products_with_issues = [product.slug for product in products if product.issues]
        checks.append(
            ApiReadinessCheck(
                key="products",
                label="Product configuration",
                status="ready" if not products_with_issues else "warning",
                detail=(
                    "All configured products have the required IDs and stakeholder metadata."
                    if not products_with_issues
                    else "Products still needing cleanup: " + ", ".join(products_with_issues)
                ),
            )
        )

        docs_deliveries = any(
            delivery.target == "docs"
            for run_record in self.storage.list_runs(limit=100)
            for delivery in self.storage.list_deliveries_for_run(run_record.run_id)
        )
        gmail_deliveries = any(
            delivery.target == "gmail"
            for run_record in self.storage.list_runs(limit=100)
            for delivery in self.storage.list_deliveries_for_run(run_record.run_id)
        )
        checks.append(
            ApiReadinessCheck(
                key="live_docs_validation",
                label="Live Docs MCP validation",
                status="ready" if docs_deliveries else "warning",
                detail=(
                    "At least one Docs delivery has been persisted in this workspace."
                    if docs_deliveries
                    else "No live Docs delivery has been recorded yet in this workspace."
                ),
            )
        )
        checks.append(
            ApiReadinessCheck(
                key="live_gmail_validation",
                label="Live Gmail MCP validation",
                status="ready" if gmail_deliveries else "warning",
                detail=(
                    "At least one Gmail delivery has been persisted in this workspace."
                    if gmail_deliveries
                    else "No live Gmail delivery has been recorded yet in this workspace."
                ),
            )
        )
        return checks

    def _build_completion_audit(
        self,
        readiness: list[ApiReadinessCheck],
    ) -> ApiCompletionAudit:
        readiness_map = {item.key: item for item in readiness}
        live_docs_ready = readiness_map["live_docs_validation"].status == "ready"
        live_gmail_ready = readiness_map["live_gmail_validation"].status == "ready"
        docs_configured = readiness_map["docs_mcp"].status == "ready"
        gmail_configured = readiness_map["gmail_mcp"].status == "ready"

        phases = [
            ApiPhaseStatus(
                phase="phase-0",
                title="Foundations",
                implementation_status="complete",
                end_to_end_status="complete",
                notes=["CLI, storage, config, logging, and CI scaffolding are in place."],
            ),
            ApiPhaseStatus(
                phase="phase-1",
                title="Ingestion",
                implementation_status="complete",
                end_to_end_status="complete",
                notes=["App Store and Google Play ingestion are implemented and tested locally."],
            ),
            ApiPhaseStatus(
                phase="phase-2",
                title="Clustering",
                implementation_status="complete",
                end_to_end_status="complete",
                notes=["Embeddings, clustering, caching, and persisted cluster artifacts exist."],
            ),
            ApiPhaseStatus(
                phase="phase-3",
                title="Summarization",
                implementation_status="complete",
                end_to_end_status="complete",
                notes=[
                    "Grounded theme generation, quote validation, and persistence are "
                    "implemented."
                ],
            ),
            ApiPhaseStatus(
                phase="phase-4",
                title="Render",
                implementation_status="complete",
                end_to_end_status="complete",
                notes=["Deterministic Docs and Gmail render artifacts are generated."],
            ),
            ApiPhaseStatus(
                phase="phase-5",
                title="Docs MCP",
                implementation_status="complete",
                end_to_end_status=(
                    "complete"
                    if docs_configured and live_docs_ready
                    else "pending-live-validation"
                ),
                notes=[
                    "Docs MCP publish logic is implemented and covered by local tests.",
                    "A real Docs MCP server still has to be configured and exercised "
                    "in this workspace."
                    if not (docs_configured and live_docs_ready)
                    else "This workspace has recorded at least one Docs delivery.",
                ],
            ),
            ApiPhaseStatus(
                phase="phase-6",
                title="Gmail MCP",
                implementation_status="complete",
                end_to_end_status="complete"
                if gmail_configured and live_gmail_ready
                else "pending-live-validation",
                notes=[
                    "Gmail MCP draft/send logic is implemented and covered by local tests.",
                    "A real Gmail MCP server still has to be configured and exercised "
                    "in this workspace."
                    if not (gmail_configured and live_gmail_ready)
                    else "This workspace has recorded at least one Gmail delivery.",
                ],
            ),
            ApiPhaseStatus(
                phase="phase-7",
                title="Orchestration",
                implementation_status="complete",
                end_to_end_status="complete"
                if docs_configured and gmail_configured and live_docs_ready and live_gmail_ready
                else "pending-live-validation",
                notes=[
                    "Recovery, locking, audit output, alerts, and telemetry are implemented.",
                    "End-to-end completion depends on a live Docs plus Gmail MCP "
                    "backed run."
                    if not (
                        docs_configured
                        and gmail_configured
                        and live_docs_ready
                        and live_gmail_ready
                    )
                    else "A live end-to-end MCP-backed run has been observed in this workspace.",
                ],
            ),
        ]

        pending_phases = [phase.phase for phase in phases if phase.end_to_end_status != "complete"]
        notes = (
            ["All documented phases are complete end to end in this workspace."]
            if not pending_phases
            else [
                "The codebase implements all documented phases, but live end-to-end "
                "validation is still pending for "
                + ", ".join(pending_phases)
                + "."
            ]
        )

        return ApiCompletionAudit(
            overall_status="complete" if not pending_phases else "pending-live-validation",
            notes=notes,
            phases=phases,
        )

    def _build_scheduler_status(
        self,
        *,
        run_records: list[StoredRunRecord],
        jobs: list[ApiJobSnapshot],
    ) -> ApiSchedulerStatus:
        cadence = _scheduler_cadence_label(
            iso_weekday=self.settings.scheduler_iso_weekday,
            hour=self.settings.scheduler_hour,
            minute=self.settings.scheduler_minute,
            timezone=self.settings.timezone,
        )
        last_started_at = max((run.started_at for run in run_records), default=None)
        last_success_at = max(
            (
                run.completed_at
                for run in run_records
                if run.status == "completed" and run.completed_at is not None
            ),
            default=None,
        )
        weekly_job_active = any(
            job.kind == "weekly-batch" and job.status in {"queued", "running"}
            for job in jobs
        )

        if weekly_job_active:
            return ApiSchedulerStatus(
                enabled=self.settings.scheduler_enabled,
                mode=self.settings.scheduler_mode,
                status="running",
                timezone=self.settings.timezone,
                cadence=cadence,
                detail="A weekly batch trigger is currently active.",
                next_run_at=(
                    self._safe_next_scheduler_run()
                    if self.settings.scheduler_enabled
                    else None
                ),
                last_started_at=last_started_at,
                last_success_at=last_success_at,
            )

        if not self.settings.scheduler_enabled:
            return ApiSchedulerStatus(
                enabled=False,
                mode=self.settings.scheduler_mode,
                status="warning",
                timezone=self.settings.timezone,
                cadence=cadence,
                detail=(
                    "Automatic scheduling is disabled. Operators must trigger runs "
                    "manually or via an external cron."
                ),
                next_run_at=None,
                last_started_at=last_started_at,
                last_success_at=last_success_at,
            )

        next_run_at = self._safe_next_scheduler_run()
        if next_run_at is None:
            return ApiSchedulerStatus(
                enabled=True,
                mode=self.settings.scheduler_mode,
                status="failed",
                timezone=self.settings.timezone,
                cadence=cadence,
                detail=(
                    "Scheduler timing is enabled, but the configured weekday or time "
                    "is invalid."
                ),
                next_run_at=None,
                last_started_at=last_started_at,
                last_success_at=last_success_at,
            )
        overdue = (
            last_success_at is not None
            and datetime.now(UTC) - last_success_at > timedelta(days=8)
        )
        return ApiSchedulerStatus(
            enabled=True,
            mode=self.settings.scheduler_mode,
            status="warning" if overdue else "ready",
            timezone=self.settings.timezone,
            cadence=cadence,
            detail=(
                "The last successful run is older than 8 days; check scheduler "
                "wiring or trigger a one-shot run."
                if overdue
                else (
                    "Scheduler timing is configured. The next run shown here is a "
                    "forecast, which is especially useful when an external cron "
                    "owns execution."
                )
            ),
            next_run_at=next_run_at,
            last_started_at=last_started_at,
            last_success_at=last_success_at,
        )

    def _build_service_statuses(
        self,
        *,
        products: list[ApiProductStatus],
        readiness: list[ApiReadinessCheck],
        run_records: list[StoredRunRecord],
        jobs: list[ApiJobSnapshot],
        scheduler: ApiSchedulerStatus,
        locks: list[ApiLockSnapshot],
    ) -> list[ApiServiceStatus]:
        checked_at = datetime.now(UTC)
        running_runs = [run for run in run_records if run.status == "running"]
        active_products = [product for product in products if product.active]
        configured_ingestion_products = sum(
            1
            for product in active_products
            if product.app_store_configured and product.play_store_configured
        )
        active_lock_count = sum(lock.status == "running" for lock in locks)
        running_job_count = sum(job.status in {"queued", "running"} for job in jobs)
        readiness_map = {item.key: item for item in readiness}

        services = [
            ApiServiceStatus(
                key="api_backend",
                label="Control API",
                category="platform",
                status="ready",
                detail="FastAPI control plane is serving dashboard and trigger requests.",
                checked_at=checked_at,
                active=True,
            ),
            self._build_storage_service_status(checked_at=checked_at),
            ApiServiceStatus(
                key="orchestrator",
                label="Orchestrator",
                category="pipeline",
                status="running" if running_job_count or active_lock_count else "ready",
                detail=(
                    f"{running_job_count} queued or running jobs and "
                    f"{active_lock_count} active run locks are being tracked."
                    if running_job_count or active_lock_count
                    else (
                        "The orchestrator is idle and ready for a one-shot or "
                        "scheduled trigger."
                    )
                ),
                checked_at=checked_at,
                active=bool(running_job_count or active_lock_count),
            ),
            self._build_stage_service_status(
                checked_at=checked_at,
                label="Ingestion Agent",
                key="ingestion_agent",
                stage="ingest",
                run_records=running_runs,
                fallback_status=(
                    "warning"
                    if not active_products
                    else
                    "ready"
                    if configured_ingestion_products == len(active_products)
                    else "warning"
                ),
                fallback_detail=(
                    f"{configured_ingestion_products}/{len(active_products)} active "
                    "products have both App Store and Play Store identifiers "
                    "configured."
                    if active_products
                    else "No active products are configured."
                ),
            ),
            self._build_stage_service_status(
                checked_at=checked_at,
                label="Clustering Agent",
                key="clustering_agent",
                stage="cluster",
                run_records=running_runs,
                fallback_status="ready",
                fallback_detail=(
                    f"Embedding provider {self.settings.cluster_embedding_provider} "
                    f"is configured with model {self.settings.cluster_embedding_model}."
                ),
            ),
            self._build_stage_service_status(
                checked_at=checked_at,
                label="Summarization Agent",
                key="summarization_agent",
                stage="summarize",
                run_records=running_runs,
                fallback_status=(
                    "warning"
                    if self.settings.summarization_provider != "heuristic"
                    and "OPENAI_API_KEY" not in os.environ
                    else "ready"
                ),
                fallback_detail=(
                    f"Summarization provider {self.settings.summarization_provider} "
                    f"is configured with model {self.settings.summarization_model}."
                ),
            ),
            self._build_stage_service_status(
                checked_at=checked_at,
                label="Render Agent",
                key="render_agent",
                stage="render",
                run_records=running_runs,
                fallback_status="ready",
                fallback_detail=(
                    "Render templates are loaded and ready to produce Docs and "
                    "Gmail artifacts."
                ),
            ),
            self._probe_docs_mcp_status(
                checked_at=checked_at,
                live_validation=readiness_map["live_docs_validation"].status == "ready",
            ),
            self._probe_gmail_mcp_status(
                checked_at=checked_at,
                live_validation=readiness_map["live_gmail_validation"].status == "ready",
            ),
            ApiServiceStatus(
                key="scheduler",
                label="Scheduler",
                category="ops",
                status=scheduler.status,
                detail=scheduler.detail,
                checked_at=checked_at,
                active=scheduler.status == "running",
            ),
            ApiServiceStatus(
                key="telemetry",
                label="Telemetry",
                category="ops",
                status="ready" if self.settings.otel_enabled else "info",
                detail=(
                    f"OpenTelemetry export is enabled for {self.settings.otel_service_name}."
                    if self.settings.otel_enabled
                    else "Telemetry export is disabled; local logs remain the primary signal."
                ),
                checked_at=checked_at,
                active=self.settings.otel_enabled,
            ),
        ]
        return services

    def _build_storage_service_status(self, *, checked_at: datetime) -> ApiServiceStatus:
        try:
            tables = set(self.storage.list_tables())
            expected_tables = {
                "clusters",
                "deliveries",
                "products",
                "review_embeddings",
                "reviews",
                "runs",
                "themes",
            }
            missing_tables = sorted(expected_tables - tables)
            if missing_tables:
                return ApiServiceStatus(
                    key="sqlite_storage",
                    label="SQLite Storage",
                    category="platform",
                    status="failed",
                    detail=(
                        "Storage is reachable, but required tables are missing: "
                        + ", ".join(missing_tables)
                    ),
                    checked_at=checked_at,
                    active=True,
                )
            return ApiServiceStatus(
                key="sqlite_storage",
                label="SQLite Storage",
                category="platform",
                status="ready",
                detail=(
                    f"SQLite is reachable at {self.settings.db_path}. {len(tables)} "
                    "tables are available."
                ),
                checked_at=checked_at,
                active=True,
            )
        except Exception as exc:
            return ApiServiceStatus(
                key="sqlite_storage",
                label="SQLite Storage",
                category="platform",
                status="failed",
                detail=f"SQLite health check failed: {exc}",
                checked_at=checked_at,
                active=False,
            )

    def _build_stage_service_status(
        self,
        *,
        checked_at: datetime,
        label: str,
        key: str,
        stage: str,
        run_records: list[StoredRunRecord],
        fallback_status: str,
        fallback_detail: str,
    ) -> ApiServiceStatus:
        active_run = next((run for run in run_records if run.stage == stage), None)
        if active_run is not None:
            return ApiServiceStatus(
                key=key,
                label=label,
                category="pipeline",
                status="running",
                detail=(
                    f"{label} is actively processing {active_run.product_slug} "
                    f"({active_run.run_id})."
                ),
                checked_at=checked_at,
                active=True,
                product_slug=active_run.product_slug,
                run_id=active_run.run_id,
            )

        return ApiServiceStatus(
            key=key,
            label=label,
            category="pipeline",
            status=fallback_status,
            detail=fallback_detail,
            checked_at=checked_at,
            active=False,
        )

    def _probe_docs_mcp_status(
        self,
        *,
        checked_at: datetime,
        live_validation: bool,
    ) -> ApiServiceStatus:
        if not _is_command_configured(self.settings.docs_mcp_command):
            return ApiServiceStatus(
                key="docs_mcp",
                label="Docs MCP",
                category="mcp",
                status="missing",
                detail="Docs MCP command is not configured for the backend runtime.",
                checked_at=checked_at,
                active=False,
            )
        return self._probe_mcp_service(
            key="docs_mcp",
            label="Docs MCP",
            checked_at=checked_at,
            live_validation=live_validation,
        )

    def _probe_gmail_mcp_status(
        self,
        *,
        checked_at: datetime,
        live_validation: bool,
    ) -> ApiServiceStatus:
        if not _is_command_configured(self.settings.gmail_mcp_command):
            return ApiServiceStatus(
                key="gmail_mcp",
                label="Gmail MCP",
                category="mcp",
                status="missing",
                detail="Gmail MCP command is not configured for the backend runtime.",
                checked_at=checked_at,
                active=False,
            )
        return self._probe_mcp_service(
            key="gmail_mcp",
            label="Gmail MCP",
            checked_at=checked_at,
            live_validation=live_validation,
        )

    def _probe_mcp_service(
        self,
        *,
        key: str,
        label: str,
        checked_at: datetime,
        live_validation: bool,
    ) -> ApiServiceStatus:
        cache_ttl_seconds = 20.0
        now_monotonic = perf_counter()
        with self._service_probe_lock:
            cached = self._service_probe_cache.get(key)
            if cached is not None and now_monotonic - cached[0] < cache_ttl_seconds:
                return cached[1]

        status = "ready"
        latency_ms: int | None = None
        detail = "MCP server responded and exposed the required tools."
        client: DocsMcpClient | GmailMcpClient | None = None
        start = perf_counter()
        try:
            if key == "docs_mcp":
                probe_settings = self.settings.model_copy(
                    update={
                        "docs_mcp_timeout_seconds": min(
                            self.settings.docs_mcp_timeout_seconds,
                            10.0,
                        )
                    }
                )
                client = DocsMcpClient.from_settings(probe_settings)
            else:
                probe_settings = self.settings.model_copy(
                    update={
                        "gmail_mcp_timeout_seconds": min(
                            self.settings.gmail_mcp_timeout_seconds,
                            10.0,
                        )
                    }
                )
                client = GmailMcpClient.from_settings(probe_settings)
            client.start()
            latency_ms = int((perf_counter() - start) * 1000)
            if not live_validation:
                detail += (
                    " The service is reachable, but this workspace has not yet "
                    "recorded a live delivery."
                )
        except Exception as exc:
            status = "failed"
            detail = f"Health probe failed: {exc}"
            latency_ms = int((perf_counter() - start) * 1000)
        finally:
            if client is not None:
                with suppress(Exception):
                    client.close()

        snapshot = ApiServiceStatus(
            key=key,
            label=label,
            category="mcp",
            status=status,
            detail=detail,
            checked_at=checked_at,
            active=False,
            latency_ms=latency_ms,
        )
        with self._service_probe_lock:
            self._service_probe_cache[key] = (now_monotonic, snapshot)
        return snapshot

    def _build_lock_snapshots(self) -> list[ApiLockSnapshot]:
        if not self.settings.locks_dir.exists():
            return []

        snapshots: list[ApiLockSnapshot] = []
        now = datetime.now(UTC)
        for lock_path in sorted(self.settings.locks_dir.glob("*.lock")):
            fallback_product_slug, fallback_iso_week = _parse_lock_name(lock_path.stem)
            try:
                payload = json.loads(lock_path.read_text(encoding="utf-8"))
                raw_acquired_at = payload.get("acquired_at")
                acquired_at = (
                    datetime.fromisoformat(raw_acquired_at)
                    if isinstance(raw_acquired_at, str)
                    else None
                )
                age_seconds = (
                    int((now - acquired_at).total_seconds())
                    if acquired_at is not None
                    else None
                )
                stale = (
                    age_seconds is not None
                    and age_seconds > self.settings.run_lock_stale_seconds
                )
                snapshots.append(
                    ApiLockSnapshot(
                        key=lock_path.stem,
                        status="warning" if stale else "running",
                        product_slug=_string_or_fallback(
                            payload.get("product_slug"),
                            fallback_product_slug,
                        ),
                        iso_week=_string_or_fallback(payload.get("iso_week"), fallback_iso_week),
                        path=str(lock_path),
                        acquired_at=acquired_at,
                        age_seconds=age_seconds,
                        pid=payload.get("pid") if isinstance(payload.get("pid"), int) else None,
                        detail=(
                            "Run lock is stale and should be investigated."
                            if stale
                            else "Run lock is active."
                        ),
                    )
                )
            except Exception as exc:
                snapshots.append(
                    ApiLockSnapshot(
                        key=lock_path.stem,
                        status="warning",
                        product_slug=fallback_product_slug,
                        iso_week=fallback_iso_week,
                        path=str(lock_path),
                        acquired_at=None,
                        age_seconds=None,
                        pid=None,
                        detail=f"Unable to parse lock file: {exc}",
                    )
                )
        return sorted(
            snapshots,
            key=lambda item: (
                0 if item.status == "running" else 1,
                -(item.age_seconds or 0),
                item.key,
            ),
        )

    def _build_issue_feed(
        self,
        *,
        products: list[ApiProductStatus],
        readiness: list[ApiReadinessCheck],
        recent_runs: list[ApiRunSummary],
        jobs: list[ApiJobSnapshot],
        scheduler: ApiSchedulerStatus,
        locks: list[ApiLockSnapshot],
    ) -> list[ApiIssueSnapshot]:
        now = datetime.now(UTC)
        issues: list[ApiIssueSnapshot] = []

        for check in readiness:
            if check.status == "ready":
                continue
            issues.append(
                ApiIssueSnapshot(
                    issue_id=f"readiness:{check.key}",
                    severity=_severity_from_status(check.status),
                    source="readiness",
                    title=check.label,
                    detail=check.detail,
                    observed_at=now,
                )
            )

        for product in products:
            for index, issue in enumerate(product.issues):
                issues.append(
                    ApiIssueSnapshot(
                        issue_id=f"product:{product.slug}:{index}",
                        severity="warning",
                        source="product",
                        title=f"{product.display_name} configuration",
                        detail=issue,
                        observed_at=(
                            product.latest_run.started_at
                            if product.latest_run is not None
                            else now
                        ),
                        product_slug=product.slug,
                    )
                )

        for run in recent_runs:
            if run.status == "failed":
                issues.append(
                    ApiIssueSnapshot(
                        issue_id=f"run:{run.run_id}",
                        severity="error",
                        source="run",
                        title=f"Run failed for {humanize_slug(run.product_slug)}",
                        detail=(
                            run.warning
                            or "The pipeline run failed. Inspect the run detail "
                            "payload for the exact stage and error."
                        ),
                        observed_at=run.completed_at or run.started_at,
                        product_slug=run.product_slug,
                        run_id=run.run_id,
                    )
                )
            elif run.warning:
                issues.append(
                    ApiIssueSnapshot(
                        issue_id=f"run-warning:{run.run_id}",
                        severity="warning",
                        source="run",
                        title=f"Run warning for {humanize_slug(run.product_slug)}",
                        detail=run.warning,
                        observed_at=run.completed_at or run.started_at,
                        product_slug=run.product_slug,
                        run_id=run.run_id,
                    )
                )

        for job in jobs:
            if job.status != "failed":
                continue
            issues.append(
                ApiIssueSnapshot(
                    issue_id=f"job:{job.job_id}",
                    severity="error",
                    source="job",
                    title="Triggered job failed",
                    detail=job.error or "A background trigger failed.",
                    observed_at=job.completed_at or job.started_at or job.submitted_at,
                    product_slug=job.product_slug,
                    run_id=job.run_id,
                )
            )

        if scheduler.status in {"warning", "failed"}:
            issues.append(
                ApiIssueSnapshot(
                    issue_id="scheduler:status",
                    severity="warning" if scheduler.status == "warning" else "error",
                    source="scheduler",
                    title="Scheduler needs attention",
                    detail=scheduler.detail,
                    observed_at=now,
                )
            )

        for lock in locks:
            if lock.status == "running":
                continue
            issues.append(
                ApiIssueSnapshot(
                    issue_id=f"lock:{lock.key}",
                    severity="warning",
                    source="lock",
                    title="Run lock needs attention",
                    detail=lock.detail,
                    observed_at=lock.acquired_at or now,
                    product_slug=lock.product_slug,
                )
            )

        issues.sort(
            key=lambda item: (
                _severity_rank(item.severity),
                item.observed_at,
            ),
            reverse=True,
        )
        return issues[:16]

    def _build_dashboard_stats(
        self,
        *,
        products: list[ApiProductStatus],
        jobs: list[ApiJobSnapshot],
        services: list[ApiServiceStatus],
        issues: list[ApiIssueSnapshot],
        locks: list[ApiLockSnapshot],
        run_records: list[StoredRunRecord],
    ) -> ApiDashboardStats:
        service_statuses = [service.status for service in services]
        active_products = sum(product.active for product in products)
        active_services = sum(service.active for service in services)
        running_jobs = sum(job.status in {"queued", "running"} for job in jobs)
        ready_services = sum(status in {"ready", "running"} for status in service_statuses)
        warning_services = sum(status in {"warning", "info"} for status in service_statuses)
        failed_services = sum(status in {"failed", "missing"} for status in service_statuses)
        active_locks = sum(lock.status == "running" for lock in locks)
        failed_runs_last_24h = sum(
            run.status == "failed" and run.started_at >= datetime.now(UTC) - timedelta(days=1)
            for run in run_records
        )

        with self.storage.connect() as connection:
            delivery_row = connection.execute(
                "SELECT COUNT(*) AS delivery_count FROM deliveries"
            ).fetchone()
        recorded_deliveries = int(delivery_row["delivery_count"]) if delivery_row is not None else 0

        return ApiDashboardStats(
            active_products=active_products,
            active_services=active_services,
            running_jobs=running_jobs,
            recorded_deliveries=recorded_deliveries,
            ready_services=ready_services,
            warning_services=warning_services,
            failed_services=failed_services,
            open_issues=sum(issue.severity in {"warning", "error"} for issue in issues),
            active_locks=active_locks,
            failed_runs_last_24h=failed_runs_last_24h,
        )

    def _safe_next_scheduler_run(self) -> datetime | None:
        try:
            return next_weekly_schedule_time(
                tz_name=self.settings.timezone,
                iso_weekday=self.settings.scheduler_iso_weekday,
                hour=self.settings.scheduler_hour,
                minute=self.settings.scheduler_minute,
            )
        except ValueError as exc:
            self.logger.warning("scheduler_configuration_invalid", error=str(exc))
            return None

    def _summary_path(self, run_record: StoredRunRecord) -> Path | None:
        candidate = (
            self.settings.artifacts_dir
            / "orchestration"
            / run_record.product_slug
            / f"{run_record.run_id}.json"
        )
        return candidate if candidate.exists() else None

    def _store_job(self, job: ApiJobSnapshot) -> None:
        with self._jobs_lock:
            self._jobs[job.job_id] = job

    def _update_job(self, job_id: str, **updates: Any) -> None:
        with self._jobs_lock:
            current = self._jobs[job_id]
            self._jobs[job_id] = current.model_copy(update=updates)

    def _get_job(self, job_id: str) -> ApiJobSnapshot:
        with self._jobs_lock:
            return self._jobs[job_id]

    @staticmethod
    def _serialize_weekly_items(result: WeeklyBatchResult) -> list[ApiJobItem]:
        return [
            ApiJobItem(
                product_slug=item.product_slug,
                status=item.status,
                run_id=item.run_id,
                summary_path=str(item.summary_path) if item.summary_path is not None else None,
                error=item.error,
            )
            for item in result.items
        ]


def create_app() -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        runtime = ApiRuntime()
        app.state.runtime = runtime
        yield
        runtime.shutdown()

    app = FastAPI(
        title="Weekly Product Review Pulse API",
        version="0.1.0",
        lifespan=lifespan,
    )

    settings = get_settings()
    app.add_middleware(
        CORSMiddleware,
        allow_origins=_parse_cors_origins(settings.api_cors_origins),
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/api/overview", response_model=ApiOverviewResponse)
    def get_overview(
        request: Request,
        limit: int = Query(default=20, ge=1, le=100),
    ) -> ApiOverviewResponse:
        return _runtime(request).build_overview(limit=limit)

    @app.get("/api/completion", response_model=ApiCompletionAudit)
    def get_completion(request: Request) -> ApiCompletionAudit:
        overview = _runtime(request).build_overview(limit=10)
        return overview.completion

    @app.get("/api/services", response_model=list[ApiServiceStatus])
    def get_services(request: Request) -> list[ApiServiceStatus]:
        overview = _runtime(request).build_overview(limit=10)
        return overview.services

    @app.get("/api/issues", response_model=list[ApiIssueSnapshot])
    def get_issues(request: Request) -> list[ApiIssueSnapshot]:
        overview = _runtime(request).build_overview(limit=10)
        return overview.issues

    @app.get("/api/scheduler", response_model=ApiSchedulerStatus)
    def get_scheduler(request: Request) -> ApiSchedulerStatus:
        overview = _runtime(request).build_overview(limit=10)
        return overview.scheduler

    @app.get("/api/products", response_model=list[ApiProductStatus])
    def get_products(request: Request) -> list[ApiProductStatus]:
        overview = _runtime(request).build_overview(limit=10)
        return overview.products

    @app.get("/api/runs", response_model=list[ApiRunSummary])
    def get_runs(
        request: Request,
        limit: int = Query(default=20, ge=1, le=100),
        product_slug: str | None = Query(default=None),
    ) -> list[ApiRunSummary]:
        runtime = _runtime(request)
        return [
            runtime._serialize_run(run_record)  # noqa: SLF001
            for run_record in runtime.storage.list_runs(limit=limit, product_slug=product_slug)
        ]

    @app.get("/api/runs/{run_id}", response_model=ApiRunDetail)
    def get_run_detail(request: Request, run_id: str) -> ApiRunDetail:
        return _runtime(request).build_run_detail(run_id)

    @app.get("/api/jobs", response_model=list[ApiJobSnapshot])
    def get_jobs(request: Request) -> list[ApiJobSnapshot]:
        return _runtime(request).list_jobs()

    @app.post(
        "/api/triggers/run",
        response_model=ApiJobSnapshot,
        status_code=status.HTTP_202_ACCEPTED,
    )
    def trigger_run(request: Request, payload: TriggerRunRequest) -> ApiJobSnapshot:
        return _runtime(request).submit_run(payload)

    @app.post(
        "/api/triggers/weekly",
        response_model=ApiJobSnapshot,
        status_code=status.HTTP_202_ACCEPTED,
    )
    def trigger_weekly(request: Request, payload: TriggerWeeklyRequest) -> ApiJobSnapshot:
        return _runtime(request).submit_weekly(payload)

    return app


def _runtime(request: Request) -> ApiRuntime:
    return cast(ApiRuntime, request.app.state.runtime)


def _parse_cors_origins(raw_value: str) -> list[str]:
    normalized = raw_value.strip()
    if not normalized or normalized == "*":
        return ["*"]
    if normalized.startswith("[") and normalized.endswith("]"):
        import json

        parsed = json.loads(normalized)
        if isinstance(parsed, list):
            return [str(item).strip() for item in parsed if str(item).strip()]
    return [item.strip() for item in normalized.split(",") if item.strip()]


def _is_command_configured(value: str | None) -> bool:
    return bool(value and value.strip())


def _is_identifier_configured(value: str | None) -> bool:
    if value is None:
        return False
    cleaned = value.strip().lower()
    return bool(cleaned and not cleaned.startswith("replace"))


def _has_real_stakeholders(emails: list[str]) -> bool:
    return any("example.com" not in email for email in emails)


def _warning_from_metadata(metadata: dict[str, Any]) -> str | None:
    for key in ("warning", "error"):
        value = metadata.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _scheduler_cadence_label(
    *,
    iso_weekday: int,
    hour: int,
    minute: int,
    timezone: str,
) -> str:
    weekday_name = _ISO_WEEKDAY_NAMES.get(iso_weekday, f"Day {iso_weekday}")
    return f"Every {weekday_name} at {hour:02d}:{minute:02d} ({timezone})"


def _severity_from_status(status: str) -> str:
    if status in {"failed", "missing"}:
        return "error"
    if status in {"warning"}:
        return "warning"
    return "info"


def _severity_rank(severity: str) -> int:
    if severity == "error":
        return 3
    if severity == "warning":
        return 2
    return 1


def _string_or_fallback(value: object, fallback: str) -> str:
    if isinstance(value, str) and value.strip():
        return value.strip()
    return fallback


def _parse_lock_name(lock_stem: str) -> tuple[str, str]:
    parts = lock_stem.split("-")
    if len(parts) >= 3 and parts[-2].isdigit() and parts[-1].startswith("w"):
        product_slug = "-".join(parts[:-2]) or lock_stem
        return product_slug, f"{parts[-2]}-{parts[-1].upper()}"
    return lock_stem, "unknown"


def humanize_slug(value: str) -> str:
    return value.replace("-", " ").title()


_ISO_WEEKDAY_NAMES = {
    1: "Monday",
    2: "Tuesday",
    3: "Wednesday",
    4: "Thursday",
    5: "Friday",
    6: "Saturday",
    7: "Sunday",
}


app = create_app()
