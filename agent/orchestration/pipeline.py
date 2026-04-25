from __future__ import annotations

import json
from pathlib import Path
from time import perf_counter
from typing import Any

from agent.clustering.pipeline import run_clustering_for_run
from agent.config import Settings
from agent.ingestion.pipeline import run_ingestion_for_run
from agent.logging import bind_run_context, clear_run_context, get_logger
from agent.orchestration.locks import ProductWeekRunLock
from agent.orchestration.models import (
    PipelineRunResult,
    StageExecution,
    WeeklyBatchItem,
    WeeklyBatchResult,
)
from agent.publish.docs_pipeline import hydrate_docs_publish_result, run_docs_publish_for_run
from agent.publish.gmail_pipeline import run_gmail_publish_for_run
from agent.pulse_types import DeliveryTarget, ProductConfig, RunWindow, Stage, StoredRunRecord
from agent.rendering.pipeline import run_render_for_run
from agent.storage import Storage
from agent.summarization.pipeline import run_summarization_for_run
from agent.telemetry import record_stage_duration, start_span
from agent.time_utils import current_iso_week, generate_run_id, resolve_iso_week_window


def run_pipeline_for_product(
    *,
    settings: Settings,
    storage: Storage,
    product: ProductConfig,
    iso_week: str | None = None,
    lookback_weeks: int | None = None,
    target: DeliveryTarget = DeliveryTarget.ALL,
) -> PipelineRunResult:
    service = OrchestrationService(settings=settings, storage=storage)
    return service.run(
        product=product,
        iso_week=iso_week,
        lookback_weeks=lookback_weeks,
        target=target,
    )


def run_weekly_for_products(
    *,
    settings: Settings,
    storage: Storage,
    products: list[ProductConfig],
    iso_week: str | None = None,
    lookback_weeks: int | None = None,
    target: DeliveryTarget = DeliveryTarget.ALL,
) -> WeeklyBatchResult:
    service = OrchestrationService(settings=settings, storage=storage)
    return service.run_weekly(
        products=products,
        iso_week=iso_week,
        lookback_weeks=lookback_weeks,
        target=target,
    )


def build_run_audit_payload(
    *,
    storage: Storage,
    run_record: StoredRunRecord,
) -> dict[str, Any]:
    deliveries = storage.list_deliveries_for_run(run_record.run_id)
    return {
        "run": {
            "run_id": run_record.run_id,
            "product_slug": run_record.product_slug,
            "iso_week": run_record.iso_week,
            "stage": run_record.stage,
            "status": run_record.status,
            "lookback_weeks": run_record.lookback_weeks,
            "started_at": run_record.started_at.isoformat(),
            "completed_at": run_record.completed_at.isoformat()
            if run_record.completed_at is not None
            else None,
            "week_start": run_record.week_start.isoformat(),
            "week_end": run_record.week_end.isoformat(),
            "lookback_start": run_record.lookback_start.isoformat(),
            "metadata": run_record.metadata,
        },
        "deliveries": [delivery.model_dump(mode="json") for delivery in deliveries],
    }


class OrchestrationService:
    def __init__(self, *, settings: Settings, storage: Storage) -> None:
        self.settings = settings
        self.storage = storage
        self.logger = get_logger("pulse.orchestration")

    def run(
        self,
        *,
        product: ProductConfig,
        iso_week: str | None,
        lookback_weeks: int | None,
        target: DeliveryTarget,
    ) -> PipelineRunResult:
        resolved_iso_week = iso_week or current_iso_week(self.settings.timezone)
        latest_run = self.storage.get_latest_run_for_product_week(product.slug, resolved_iso_week)
        resumed = latest_run is not None

        if latest_run is not None and _target_satisfied(
            storage=self.storage,
            run_record=latest_run,
            target=target,
            confirm_send=self.settings.confirm_send,
        ):
            result = PipelineRunResult(
                run_id=latest_run.run_id,
                product_slug=product.slug,
                iso_week=resolved_iso_week,
                target=target,
                status="completed",
                resumed=True,
                stages=_build_already_satisfied_stages(target),
                summary_path=self._summary_path(product.slug, latest_run.run_id),
                warning="Run already satisfies the requested delivery target.",
            )
            self.storage.update_run_status(
                latest_run.run_id,
                status="completed",
                stage=Stage.RUN.value,
                metadata=result.to_metadata(),
                completed=True,
            )
            refreshed = self._refresh_run(latest_run.run_id)
            self._write_summary(run_record=refreshed, result=result)
            return result

        run_record: StoredRunRecord
        if latest_run is not None:
            run_record = latest_run
        else:
            resolved_lookback = lookback_weeks or product.default_lookback_weeks
            window = resolve_iso_week_window(
                resolved_iso_week,
                resolved_lookback,
                self.settings.timezone,
            )
            run_id = generate_run_id()
            self.storage.upsert_run(
                run_id=run_id,
                product_slug=product.slug,
                iso_week=resolved_iso_week,
                stage=Stage.RUN.value,
                status="planned",
                lookback_weeks=resolved_lookback,
                week_start=window.week_start.isoformat(),
                week_end=window.week_end.isoformat(),
                lookback_start=window.lookback_start.isoformat(),
                metadata={
                    "phase": "phase-7",
                    "placeholder": False,
                    "orchestration_target": target.value,
                },
            )
            created_run = self.storage.get_run(run_id)
            if created_run is None:
                raise RuntimeError(f"Failed to create run record {run_id}.")
            run_record = created_run
            resumed = False

        lock = ProductWeekRunLock(
            lock_path=self._lock_path(product.slug, resolved_iso_week),
            product_slug=product.slug,
            iso_week=resolved_iso_week,
            stale_after_seconds=self.settings.run_lock_stale_seconds,
        )
        with lock:
            bind_run_context(
                run_id=run_record.run_id,
                stage=Stage.RUN.value,
                product=product.slug,
                iso_week=run_record.iso_week,
                target=target.value,
            )
            self.storage.update_run_status(
                run_record.run_id,
                status="running",
                stage=Stage.RUN.value,
                metadata={
                    "phase": "phase-7",
                    "orchestration_target": target.value,
                    "orchestration_resumed": resumed,
                },
            )
            stages: list[StageExecution] = []
            try:
                with start_span(
                    "orchestration.run",
                    {
                        "product_slug": product.slug,
                        "iso_week": run_record.iso_week,
                        "target": target.value,
                        "resumed": resumed,
                    },
                ):
                    current_run = self._refresh_run(run_record.run_id)
                    window = resolve_iso_week_window(
                        current_run.iso_week,
                        current_run.lookback_weeks,
                        self.settings.timezone,
                    )

                    if _ingest_complete(current_run):
                        self._mark_stage_skipped(
                            stage_name="ingest",
                            detail="Existing ingestion snapshot found.",
                            stages=stages,
                        )
                    else:
                        self._run_ingest_stage(
                            run_record=current_run,
                            product=product,
                            window=window,
                            stages=stages,
                        )
                    current_run = self._refresh_run(run_record.run_id)

                    if _cluster_complete(current_run):
                        self._mark_stage_skipped(
                            stage_name="cluster",
                            detail="Existing cluster metadata found.",
                            stages=stages,
                        )
                    else:
                        self._run_cluster_stage(run_record=current_run, stages=stages)
                    current_run = self._refresh_run(run_record.run_id)

                    if _summarize_complete(current_run):
                        self._mark_stage_skipped(
                            stage_name="summarize",
                            detail="Existing theme summary metadata found.",
                            stages=stages,
                        )
                    else:
                        self._run_summarize_stage(run_record=current_run, stages=stages)
                    current_run = self._refresh_run(run_record.run_id)

                    if _render_complete(current_run):
                        self._mark_stage_skipped(
                            stage_name="render",
                            detail="Existing render artifact found.",
                            stages=stages,
                        )
                    else:
                        self._run_render_stage(
                            run_record=current_run,
                            product=product,
                            stages=stages,
                        )
                    current_run = self._refresh_run(run_record.run_id)

                    docs_result = None
                    if target in {DeliveryTarget.DOCS, DeliveryTarget.GMAIL, DeliveryTarget.ALL}:
                        docs_result = hydrate_docs_publish_result(
                            storage=self.storage,
                            run_record=current_run,
                        )
                        if docs_result is not None:
                            self._mark_stage_skipped(
                                stage_name="publish_docs",
                                detail="Existing Docs delivery metadata found.",
                                stages=stages,
                            )
                        else:
                            docs_result = self._run_docs_stage(
                                run_record=current_run,
                                product=product,
                                stages=stages,
                            )
                        current_run = self._refresh_run(run_record.run_id)

                    if target in {DeliveryTarget.GMAIL, DeliveryTarget.ALL}:
                        if _gmail_target_satisfied(
                            storage=self.storage,
                            run_record=current_run,
                            confirm_send=self.settings.confirm_send,
                        ):
                            detail = (
                                "Existing Gmail send metadata found."
                                if self.settings.confirm_send
                                else "Existing Gmail draft/send metadata found."
                            )
                            self._mark_stage_skipped(
                                stage_name="publish_gmail",
                                detail=detail,
                                stages=stages,
                            )
                        else:
                            self._run_gmail_stage(
                                run_record=current_run,
                                product=product,
                                docs_result=docs_result,
                                stages=stages,
                            )
                        current_run = self._refresh_run(run_record.run_id)

                    result = PipelineRunResult(
                        run_id=current_run.run_id,
                        product_slug=product.slug,
                        iso_week=current_run.iso_week,
                        target=target,
                        status="completed",
                        resumed=resumed,
                        stages=stages,
                        summary_path=self._summary_path(product.slug, current_run.run_id),
                        warning=None,
                    )
                    self.storage.update_run_status(
                        current_run.run_id,
                        status="completed",
                        stage=Stage.RUN.value,
                        metadata=result.to_metadata(),
                        completed=True,
                    )
                    refreshed_run = self._refresh_run(current_run.run_id)
                    self._write_summary(run_record=refreshed_run, result=result)
                    return result
            except Exception as exc:
                failed_run = self._refresh_run(run_record.run_id)
                result = PipelineRunResult(
                    run_id=failed_run.run_id,
                    product_slug=product.slug,
                    iso_week=failed_run.iso_week,
                    target=target,
                    status="failed",
                    resumed=resumed,
                    stages=stages,
                    summary_path=self._summary_path(product.slug, failed_run.run_id),
                    warning=str(exc),
                )
                self.storage.update_run_status(
                    failed_run.run_id,
                    status="failed",
                    stage=Stage.RUN.value,
                    metadata=result.to_metadata() | {"error": str(exc)},
                    completed=True,
                )
                refreshed_failed_run = self._refresh_run(failed_run.run_id)
                self._write_summary(
                    run_record=refreshed_failed_run,
                    result=result,
                    error=str(exc),
                )
                self.logger.exception(
                    "run_failed",
                    error=str(exc),
                    product=product.slug,
                    iso_week=failed_run.iso_week,
                    run_id=failed_run.run_id,
                )
                raise
            finally:
                clear_run_context()

    def run_weekly(
        self,
        *,
        products: list[ProductConfig],
        iso_week: str | None,
        lookback_weeks: int | None,
        target: DeliveryTarget,
    ) -> WeeklyBatchResult:
        resolved_iso_week = iso_week or current_iso_week(self.settings.timezone)
        items: list[WeeklyBatchItem] = []
        with start_span(
            "orchestration.weekly_batch",
            {
                "iso_week": resolved_iso_week,
                "target": target.value,
                "product_count": len([product for product in products if product.active]),
            },
        ):
            for product in products:
                if not product.active:
                    continue
                try:
                    result = self.run(
                        product=product,
                        iso_week=resolved_iso_week,
                        lookback_weeks=lookback_weeks,
                        target=target,
                    )
                    items.append(
                        WeeklyBatchItem(
                            product_slug=product.slug,
                            iso_week=resolved_iso_week,
                            status="completed",
                            run_id=result.run_id,
                            summary_path=result.summary_path,
                        )
                    )
                except Exception as exc:
                    latest_run = self.storage.get_latest_run_for_product_week(
                        product.slug,
                        resolved_iso_week,
                    )
                    items.append(
                        WeeklyBatchItem(
                            product_slug=product.slug,
                            iso_week=resolved_iso_week,
                            status="failed",
                            run_id=latest_run.run_id if latest_run is not None else None,
                            summary_path=self._summary_path(product.slug, latest_run.run_id)
                            if latest_run is not None
                            else None,
                            error=str(exc),
                        )
                    )
        return WeeklyBatchResult(
            iso_week=resolved_iso_week,
            target=target,
            items=items,
        )

    def _run_ingest_stage(
        self,
        *,
        run_record: StoredRunRecord,
        product: ProductConfig,
        window: RunWindow,
        stages: list[StageExecution],
    ) -> None:
        self.storage.update_run_status(
            run_record.run_id,
            status="running",
            stage=Stage.INGEST.value,
        )
        start = perf_counter()
        with start_span("orchestration.stage", {"stage": "ingest"}):
            try:
                result = run_ingestion_for_run(
                    settings=self.settings,
                    storage=self.storage,
                    product=product,
                    window=window,
                    run_id=run_record.run_id,
                )
                duration_ms = _duration_ms(start)
                record_stage_duration(stage="ingest", duration_ms=duration_ms, status="completed")
                self.storage.update_run_status(
                    run_record.run_id,
                    status="completed",
                    stage=Stage.INGEST.value,
                    metadata=result.to_metadata(),
                    completed=True,
                )
                stages.append(
                    StageExecution(
                        name="ingest",
                        status="completed",
                        duration_ms=duration_ms,
                        detail=f"Ingested {result.total_reviews} reviews.",
                    )
                )
            except Exception as exc:
                duration_ms = _duration_ms(start)
                record_stage_duration(stage="ingest", duration_ms=duration_ms, status="failed")
                stages.append(
                    StageExecution(
                        name="ingest",
                        status="failed",
                        duration_ms=duration_ms,
                        detail=str(exc),
                    )
                )
                raise

    def _run_cluster_stage(
        self,
        *,
        run_record: StoredRunRecord,
        stages: list[StageExecution],
    ) -> None:
        self.storage.update_run_status(
            run_record.run_id,
            status="running",
            stage=Stage.CLUSTER.value,
        )
        start = perf_counter()
        with start_span("orchestration.stage", {"stage": "cluster"}):
            try:
                result = run_clustering_for_run(
                    settings=self.settings,
                    storage=self.storage,
                    run_record=run_record,
                )
                duration_ms = _duration_ms(start)
                record_stage_duration(stage="cluster", duration_ms=duration_ms, status="completed")
                self.storage.update_run_status(
                    run_record.run_id,
                    status="completed",
                    stage=Stage.CLUSTER.value,
                    metadata=result.to_metadata(),
                    completed=True,
                )
                stages.append(
                    StageExecution(
                        name="cluster",
                        status="completed",
                        duration_ms=duration_ms,
                        detail=f"Persisted {result.cluster_count} clusters.",
                    )
                )
            except Exception as exc:
                duration_ms = _duration_ms(start)
                record_stage_duration(stage="cluster", duration_ms=duration_ms, status="failed")
                stages.append(
                    StageExecution(
                        name="cluster",
                        status="failed",
                        duration_ms=duration_ms,
                        detail=str(exc),
                    )
                )
                raise

    def _run_summarize_stage(
        self,
        *,
        run_record: StoredRunRecord,
        stages: list[StageExecution],
    ) -> None:
        self.storage.update_run_status(
            run_record.run_id,
            status="running",
            stage=Stage.SUMMARIZE.value,
        )
        start = perf_counter()
        with start_span("orchestration.stage", {"stage": "summarize"}):
            try:
                result = run_summarization_for_run(
                    settings=self.settings,
                    storage=self.storage,
                    run_record=run_record,
                )
                duration_ms = _duration_ms(start)
                record_stage_duration(
                    stage="summarize",
                    duration_ms=duration_ms,
                    status="completed",
                )
                self.storage.update_run_status(
                    run_record.run_id,
                    status="completed",
                    stage=Stage.SUMMARIZE.value,
                    metadata=result.to_metadata(),
                    completed=True,
                )
                stages.append(
                    StageExecution(
                        name="summarize",
                        status="completed",
                        duration_ms=duration_ms,
                        detail=f"Persisted {result.theme_count} themes.",
                    )
                )
            except Exception as exc:
                duration_ms = _duration_ms(start)
                record_stage_duration(
                    stage="summarize",
                    duration_ms=duration_ms,
                    status="failed",
                )
                stages.append(
                    StageExecution(
                        name="summarize",
                        status="failed",
                        duration_ms=duration_ms,
                        detail=str(exc),
                    )
                )
                raise

    def _run_render_stage(
        self,
        *,
        run_record: StoredRunRecord,
        product: ProductConfig,
        stages: list[StageExecution],
    ) -> None:
        self.storage.update_run_status(
            run_record.run_id,
            status="running",
            stage=Stage.RENDER.value,
        )
        start = perf_counter()
        with start_span("orchestration.stage", {"stage": "render"}):
            try:
                result = run_render_for_run(
                    settings=self.settings,
                    storage=self.storage,
                    run_record=run_record,
                    product=product,
                )
                duration_ms = _duration_ms(start)
                record_stage_duration(stage="render", duration_ms=duration_ms, status="completed")
                self.storage.update_run_status(
                    run_record.run_id,
                    status="completed",
                    stage=Stage.RENDER.value,
                    metadata=result.to_metadata(),
                    completed=True,
                )
                stages.append(
                    StageExecution(
                        name="render",
                        status="completed",
                        duration_ms=duration_ms,
                        detail=f"Rendered artifact {result.artifact_path.name}.",
                    )
                )
            except Exception as exc:
                duration_ms = _duration_ms(start)
                record_stage_duration(stage="render", duration_ms=duration_ms, status="failed")
                stages.append(
                    StageExecution(
                        name="render",
                        status="failed",
                        duration_ms=duration_ms,
                        detail=str(exc),
                    )
                )
                raise

    def _run_docs_stage(
        self,
        *,
        run_record: StoredRunRecord,
        product: ProductConfig,
        stages: list[StageExecution],
    ) -> Any:
        self.storage.update_run_status(
            run_record.run_id,
            status="running",
            stage=Stage.PUBLISH.value,
        )
        start = perf_counter()
        with start_span("orchestration.stage", {"stage": "publish_docs"}):
            try:
                result = run_docs_publish_for_run(
                    settings=self.settings,
                    storage=self.storage,
                    run_record=run_record,
                    product=product,
                )
                duration_ms = _duration_ms(start)
                record_stage_duration(
                    stage="publish_docs",
                    duration_ms=duration_ms,
                    status="completed",
                )
                self.storage.update_run_status(
                    run_record.run_id,
                    status="completed",
                    stage=Stage.PUBLISH.value,
                    metadata=result.to_metadata(),
                    completed=True,
                )
                stages.append(
                    StageExecution(
                        name="publish_docs",
                        status="completed",
                        duration_ms=duration_ms,
                        detail=result.publish_action,
                    )
                )
                return result
            except Exception as exc:
                duration_ms = _duration_ms(start)
                record_stage_duration(
                    stage="publish_docs",
                    duration_ms=duration_ms,
                    status="failed",
                )
                stages.append(
                    StageExecution(
                        name="publish_docs",
                        status="failed",
                        duration_ms=duration_ms,
                        detail=str(exc),
                    )
                )
                raise

    def _run_gmail_stage(
        self,
        *,
        run_record: StoredRunRecord,
        product: ProductConfig,
        docs_result: Any,
        stages: list[StageExecution],
    ) -> None:
        self.storage.update_run_status(
            run_record.run_id,
            status="running",
            stage=Stage.PUBLISH.value,
        )
        start = perf_counter()
        with start_span("orchestration.stage", {"stage": "publish_gmail"}):
            try:
                result = run_gmail_publish_for_run(
                    settings=self.settings,
                    storage=self.storage,
                    run_record=run_record,
                    product=product,
                    docs_result=docs_result,
                )
                duration_ms = _duration_ms(start)
                record_stage_duration(
                    stage="publish_gmail",
                    duration_ms=duration_ms,
                    status="completed",
                )
                self.storage.update_run_status(
                    run_record.run_id,
                    status="completed",
                    stage=Stage.PUBLISH.value,
                    metadata=result.to_metadata(),
                    completed=True,
                )
                stages.append(
                    StageExecution(
                        name="publish_gmail",
                        status="completed",
                        duration_ms=duration_ms,
                        detail=result.publish_action,
                    )
                )
            except Exception as exc:
                duration_ms = _duration_ms(start)
                record_stage_duration(
                    stage="publish_gmail",
                    duration_ms=duration_ms,
                    status="failed",
                )
                stages.append(
                    StageExecution(
                        name="publish_gmail",
                        status="failed",
                        duration_ms=duration_ms,
                        detail=str(exc),
                    )
                )
                raise

    def _refresh_run(self, run_id: str) -> StoredRunRecord:
        run_record = self.storage.get_run(run_id)
        if run_record is None:
            raise KeyError(f"Unknown run id: {run_id}")
        return run_record

    def _lock_path(self, product_slug: str, iso_week: str) -> Path:
        normalized_week = iso_week.lower()
        return self.settings.locks_dir / f"{product_slug}-{normalized_week}.lock"

    def _summary_path(self, product_slug: str, run_id: str) -> Path:
        return self.settings.artifacts_dir / "orchestration" / product_slug / f"{run_id}.json"

    def _mark_stage_skipped(
        self,
        *,
        stage_name: str,
        detail: str,
        stages: list[StageExecution],
    ) -> None:
        with start_span(
            "orchestration.stage",
            {
                "stage": stage_name,
                "status": "skipped",
            },
        ):
            record_stage_duration(stage=stage_name, duration_ms=0, status="skipped")
        stages.append(
            StageExecution(
                name=stage_name,
                status="skipped",
                detail=detail,
            )
        )

    def _write_summary(
        self,
        *,
        run_record: StoredRunRecord,
        result: PipelineRunResult,
        error: str | None = None,
    ) -> None:
        payload = build_run_audit_payload(storage=self.storage, run_record=run_record)
        payload["orchestration"] = {
            "target": result.target.value,
            "status": result.status,
            "resumed": result.resumed,
            "warning": result.warning,
            "stages": [stage.model_dump(mode="json") for stage in result.stages],
            "error": error,
        }
        result.summary_path.parent.mkdir(parents=True, exist_ok=True)
        result.summary_path.write_text(
            json.dumps(payload, indent=2, sort_keys=True),
            encoding="utf-8",
        )


def _duration_ms(start: float) -> int:
    return int((perf_counter() - start) * 1000)


def _ingest_complete(run_record: StoredRunRecord) -> bool:
    snapshot_path = _string_or_none(run_record.metadata.get("snapshot_path"))
    return snapshot_path is not None and Path(snapshot_path).exists()


def _cluster_complete(run_record: StoredRunRecord) -> bool:
    return "cluster_count" in run_record.metadata and "embedding_model" in run_record.metadata


def _summarize_complete(run_record: StoredRunRecord) -> bool:
    return "theme_count" in run_record.metadata and "summarization_model" in run_record.metadata


def _render_complete(run_record: StoredRunRecord) -> bool:
    artifact_path = _string_or_none(run_record.metadata.get("render_artifact_path"))
    return artifact_path is not None and Path(artifact_path).exists()


def _gmail_target_satisfied(
    *,
    storage: Storage,
    run_record: StoredRunRecord,
    confirm_send: bool,
) -> bool:
    if _string_or_none(run_record.metadata.get("gmail_message_id")) is not None:
        return True
    delivery = storage.get_delivery(run_record.run_id, DeliveryTarget.GMAIL.value)
    if delivery is not None and delivery.status == "sent":
        return True
    if confirm_send:
        return False
    if _string_or_none(run_record.metadata.get("gmail_draft_id")) is not None:
        return True
    return delivery is not None and delivery.status == "drafted"


def _target_satisfied(
    *,
    storage: Storage,
    run_record: StoredRunRecord,
    target: DeliveryTarget,
    confirm_send: bool,
) -> bool:
    if target is DeliveryTarget.DOCS:
        return hydrate_docs_publish_result(storage=storage, run_record=run_record) is not None
    if target is DeliveryTarget.GMAIL:
        return _gmail_target_satisfied(
            storage=storage,
            run_record=run_record,
            confirm_send=confirm_send,
        )
    return hydrate_docs_publish_result(storage=storage, run_record=run_record) is not None and (
        _gmail_target_satisfied(
            storage=storage,
            run_record=run_record,
            confirm_send=confirm_send,
        )
    )


def _build_already_satisfied_stages(target: DeliveryTarget) -> list[StageExecution]:
    stages = [
        StageExecution(name="ingest", status="skipped", detail="Run already satisfied."),
        StageExecution(name="cluster", status="skipped", detail="Run already satisfied."),
        StageExecution(name="summarize", status="skipped", detail="Run already satisfied."),
        StageExecution(name="render", status="skipped", detail="Run already satisfied."),
    ]
    if target in {DeliveryTarget.DOCS, DeliveryTarget.GMAIL, DeliveryTarget.ALL}:
        stages.append(
            StageExecution(name="publish_docs", status="skipped", detail="Run already satisfied.")
        )
    if target in {DeliveryTarget.GMAIL, DeliveryTarget.ALL}:
        stages.append(
            StageExecution(name="publish_gmail", status="skipped", detail="Run already satisfied.")
        )
    return stages


def _string_or_none(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None
