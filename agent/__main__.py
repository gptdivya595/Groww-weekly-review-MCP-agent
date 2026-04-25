from __future__ import annotations

import json
from typing import Annotated

import typer

from agent.clustering.pipeline import run_clustering_for_run
from agent.config import Settings, get_product_by_slug, get_settings, load_products
from agent.ingestion.pipeline import run_ingestion_for_run
from agent.logging import bind_run_context, clear_run_context, configure_logging, get_logger
from agent.orchestration.pipeline import (
    build_run_audit_payload,
    run_pipeline_for_product,
    run_weekly_for_products,
)
from agent.publish.docs_pipeline import run_docs_publish_for_run
from agent.publish.gmail_pipeline import run_gmail_publish_for_run
from agent.pulse_types import DeliveryTarget, ProductConfig, RunWindow, Stage, StoredRunRecord
from agent.rendering.pipeline import run_render_for_run
from agent.storage import Storage
from agent.summarization.pipeline import run_summarization_for_run
from agent.telemetry import configure_telemetry
from agent.time_utils import current_iso_week, generate_run_id, resolve_iso_week_window

app = typer.Typer(
    name="pulse",
    help="Weekly Product Review Pulse CLI.",
    no_args_is_help=True,
)


ProductOption = Annotated[
    str,
    typer.Option(help="Product slug from products.yaml."),
]
WeeksOption = Annotated[
    int | None,
    typer.Option(min=1, help="Lookback window in weeks."),
]
IsoWeekOption = Annotated[
    str | None,
    typer.Option(help="Target ISO week, e.g. 2026-W17."),
]
RunOption = Annotated[
    str,
    typer.Option(help="Run identifier."),
]
TargetOption = Annotated[
    DeliveryTarget,
    typer.Option(help="Publish target."),
]
HostOption = Annotated[
    str,
    typer.Option(help="Host interface to bind."),
]
PortOption = Annotated[
    int,
    typer.Option(min=1, max=65535, help="Port to bind."),
]


def _bootstrap() -> tuple[Settings, Storage]:
    settings = get_settings()
    configure_logging(settings.log_level)
    configure_telemetry(settings)
    return settings, Storage(settings.db_path)


def _prepare_run(
    *,
    product: str,
    stage: Stage,
    iso_week: str | None,
    lookback_weeks: int | None,
) -> tuple[Settings, Storage, ProductConfig, RunWindow, str]:
    settings, storage = _bootstrap()
    storage.initialize()
    storage.seed_products(load_products(settings))
    product_config = get_product_by_slug(product, settings)
    resolved_iso_week = iso_week or current_iso_week(settings.timezone)
    resolved_lookback = lookback_weeks or product_config.default_lookback_weeks
    window = resolve_iso_week_window(resolved_iso_week, resolved_lookback, settings.timezone)
    run_id = generate_run_id()
    bind_run_context(run_id=run_id, product=product, iso_week=resolved_iso_week, stage=stage.value)
    try:
        storage.upsert_run(
            run_id=run_id,
            product_slug=product,
            iso_week=resolved_iso_week,
            stage=stage.value,
            status="planned",
            lookback_weeks=resolved_lookback,
            week_start=window.week_start.isoformat(),
            week_end=window.week_end.isoformat(),
            lookback_start=window.lookback_start.isoformat(),
            metadata={"phase": "phase-0", "placeholder": True},
        )
    except Exception:
        clear_run_context()
        raise
    return settings, storage, product_config, window, run_id


def _load_existing_run(storage: Storage, run_id: str) -> StoredRunRecord:
    run_record = storage.get_run(run_id)
    if run_record is None:
        raise KeyError(f"Unknown run id: {run_id}")
    return run_record


@app.command("init-db")
def init_db() -> None:
    """Create the local SQLite database and seed products."""
    settings, storage = _bootstrap()
    logger = get_logger("pulse.init_db")
    storage.initialize()
    products = load_products(settings)
    storage.seed_products(products)
    logger.info(
        "database_initialized",
        db_path=str(settings.db_path),
        products_seeded=len(products),
        tables=storage.list_tables(),
    )
    typer.echo(f"Initialized database at {settings.db_path}")


@app.command()
def ingest(
    product: ProductOption,
    weeks: WeeksOption = None,
    iso_week: IsoWeekOption = None,
) -> None:
    """Fetch reviews, persist them locally, and write a raw audit snapshot."""
    settings, storage, product_config, window, run_id = _prepare_run(
        product=product,
        stage=Stage.INGEST,
        iso_week=iso_week,
        lookback_weeks=weeks,
    )
    logger = get_logger("pulse.ingest")
    try:
        result = run_ingestion_for_run(
            settings=settings,
            storage=storage,
            product=product_config,
            window=window,
            run_id=run_id,
        )
        storage.update_run_status(
            run_id,
            status="completed",
            stage=Stage.INGEST.value,
            metadata=result.to_metadata(),
            completed=True,
        )
        logger.info(
            "ingest_completed",
            total_reviews=result.total_reviews,
            inserted=result.upsert.inserted,
            updated=result.upsert.updated,
            unchanged=result.upsert.unchanged,
            snapshot_path=str(result.snapshot_path),
            degraded=result.degraded,
        )
        typer.echo(
            "Ingested "
            f"{result.total_reviews} reviews for {product_config.slug} "
            f"(inserted={result.upsert.inserted}, updated={result.upsert.updated}, "
            f"unchanged={result.upsert.unchanged}). "
            f"Snapshot: {result.snapshot_path}"
        )
    except Exception as exc:
        storage.update_run_status(
            run_id,
            status="failed",
            stage=Stage.INGEST.value,
            metadata={"phase": "phase-1", "error": str(exc)},
            completed=True,
        )
        logger.exception("ingest_failed", error=str(exc))
        typer.echo(f"Ingest failed for {product_config.slug}: {exc}", err=True)
        raise typer.Exit(code=1) from None
    finally:
        clear_run_context()


@app.command()
def cluster(
    run: RunOption,
) -> None:
    """Cluster stored reviews, persist embeddings, and write cluster artifacts."""
    settings, _ = _bootstrap()
    storage = Storage(settings.db_path)
    storage.initialize()
    try:
        run_record = _load_existing_run(storage, run)
    except KeyError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from None

    bind_run_context(
        run_id=run,
        stage=Stage.CLUSTER.value,
        product=run_record.product_slug,
        iso_week=run_record.iso_week,
    )
    try:
        storage.update_run_status(run, status="running", stage=Stage.CLUSTER.value)
        result = run_clustering_for_run(
            settings=settings,
            storage=storage,
            run_record=run_record,
        )
        storage.update_run_status(
            run,
            status="completed",
            stage=Stage.CLUSTER.value,
            metadata=result.to_metadata(),
            completed=True,
        )
        logger = get_logger("pulse.cluster")
        logger.info(
            "cluster_completed",
            clusters=result.cluster_count,
            eligible_reviews=result.eligible_reviews,
            total_reviews_window=result.total_reviews_window,
            noise_count=result.noise_count,
            cache_hits=result.embedding_stats.cache_hits,
            cache_misses=result.embedding_stats.cache_misses,
            warning=result.warning,
        )
        typer.echo(
            "Clustered "
            f"{result.eligible_reviews} eligible reviews into {result.cluster_count} clusters "
            f"(noise={result.noise_count}, cache_hits={result.embedding_stats.cache_hits}, "
            f"cache_misses={result.embedding_stats.cache_misses})."
        )
        if result.warning:
            typer.echo(f"Warning: {result.warning}")
    except Exception as exc:
        storage.update_run_status(
            run,
            status="failed",
            stage=Stage.CLUSTER.value,
            metadata={"phase": "phase-2", "error": str(exc)},
            completed=True,
        )
        logger = get_logger("pulse.cluster")
        logger.exception("cluster_failed", error=str(exc))
        typer.echo(f"Clustering failed for run {run}: {exc}", err=True)
        raise typer.Exit(code=1) from None
    finally:
        clear_run_context()


@app.command()
def summarize(
    run: RunOption,
) -> None:
    """Summarize stored clusters into grounded themes, quotes, and actions."""
    settings, _ = _bootstrap()
    storage = Storage(settings.db_path)
    storage.initialize()
    try:
        run_record = _load_existing_run(storage, run)
    except KeyError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from None

    bind_run_context(
        run_id=run,
        stage=Stage.SUMMARIZE.value,
        product=run_record.product_slug,
        iso_week=run_record.iso_week,
    )
    try:
        storage.update_run_status(run, status="running", stage=Stage.SUMMARIZE.value)
        result = run_summarization_for_run(
            settings=settings,
            storage=storage,
            run_record=run_record,
        )
        storage.update_run_status(
            run,
            status="completed",
            stage=Stage.SUMMARIZE.value,
            metadata=result.to_metadata(),
            completed=True,
        )
        logger = get_logger("pulse.summarize")
        logger.info(
            "summarize_completed",
            theme_count=result.theme_count,
            clusters_summarized=result.clusters_summarized,
            invalid_quote_count=result.invalid_quote_count,
            quote_omission_count=result.quote_omission_count,
            retry_count=result.retry_count,
            fallback_count=result.fallback_count,
            warning=result.warning,
        )
        typer.echo(
            "Summarized "
            f"{result.theme_count} themes from {result.clusters_summarized} clusters "
            f"(invalid_quotes={result.invalid_quote_count}, "
            f"omitted_quotes={result.quote_omission_count}, retries={result.retry_count})."
        )
        if result.warning:
            typer.echo(f"Warning: {result.warning}")
    except Exception as exc:
        storage.update_run_status(
            run,
            status="failed",
            stage=Stage.SUMMARIZE.value,
            metadata={"phase": "phase-3", "error": str(exc)},
            completed=True,
        )
        logger = get_logger("pulse.summarize")
        logger.exception("summarize_failed", error=str(exc))
        typer.echo(f"Summarization failed for run {run}: {exc}", err=True)
        raise typer.Exit(code=1) from None
    finally:
        clear_run_context()


@app.command()
def render(
    run: RunOption,
) -> None:
    """Render a deterministic Docs/email artifact from persisted summarized themes."""
    settings, _ = _bootstrap()
    storage = Storage(settings.db_path)
    storage.initialize()
    try:
        run_record = _load_existing_run(storage, run)
    except KeyError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from None

    try:
        product = get_product_by_slug(run_record.product_slug, settings)
    except KeyError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from None

    bind_run_context(
        run_id=run,
        stage=Stage.RENDER.value,
        product=run_record.product_slug,
        iso_week=run_record.iso_week,
    )
    try:
        storage.update_run_status(run, status="running", stage=Stage.RENDER.value)
        result = run_render_for_run(
            settings=settings,
            storage=storage,
            run_record=run_record,
            product=product,
        )
        storage.update_run_status(
            run,
            status="completed",
            stage=Stage.RENDER.value,
            metadata=result.to_metadata(),
            completed=True,
        )
        logger = get_logger("pulse.render")
        logger.info(
            "render_completed",
            rendered_theme_count=result.rendered_theme_count,
            quote_count=result.quote_count,
            action_count=result.action_count,
            artifact_path=str(result.artifact_path),
            anchor_key=result.anchor_key,
            warning=result.warning,
        )
        typer.echo(
            "Rendered "
            f"{result.rendered_theme_count} themes, {result.quote_count} quotes, "
            f"and {result.action_count} action ideas. "
            f"Artifact: {result.artifact_path}"
        )
        if result.warning:
            typer.echo(f"Warning: {result.warning}")
    except Exception as exc:
        storage.update_run_status(
            run,
            status="failed",
            stage=Stage.RENDER.value,
            metadata={"phase": "phase-4", "error": str(exc)},
            completed=True,
        )
        logger = get_logger("pulse.render")
        logger.exception("render_failed", error=str(exc))
        typer.echo(f"Render failed for run {run}: {exc}", err=True)
        raise typer.Exit(code=1) from None
    finally:
        clear_run_context()


@app.command()
def publish(
    run: RunOption,
    target: TargetOption,
) -> None:
    """Publish rendered artifacts to downstream delivery targets."""
    settings, _ = _bootstrap()
    storage = Storage(settings.db_path)
    storage.initialize()
    try:
        run_record = _load_existing_run(storage, run)
    except KeyError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from None

    try:
        product = get_product_by_slug(run_record.product_slug, settings)
    except KeyError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from None

    bind_run_context(
        run_id=run,
        stage=Stage.PUBLISH.value,
        product=run_record.product_slug,
        iso_week=run_record.iso_week,
        target=target.value,
    )
    try:
        storage.update_run_status(run, status="running", stage=Stage.PUBLISH.value)
        logger = get_logger("pulse.publish")
        docs_result = None
        if target in {DeliveryTarget.DOCS, DeliveryTarget.GMAIL, DeliveryTarget.ALL}:
            docs_result = run_docs_publish_for_run(
                settings=settings,
                storage=storage,
                run_record=run_record,
                product=product,
            )
            storage.update_run_status(
                run,
                status="completed",
                stage=Stage.PUBLISH.value,
                metadata=docs_result.to_metadata(),
                completed=True,
            )
            logger.info(
                "publish_docs_completed",
                target=target.value,
                action=docs_result.publish_action,
                document_id=docs_result.document_id,
                heading_id=docs_result.heading_id,
                deep_link=docs_result.deep_link,
                warning=docs_result.warning,
            )
            if target is DeliveryTarget.DOCS:
                if docs_result.published:
                    typer.echo(
                        "Published Docs section "
                        f"for run {run} into document {docs_result.document_id}. "
                        f"Link: {docs_result.deep_link}"
                    )
                else:
                    typer.echo(
                        "Docs section already exists "
                        f"for run {run} in document {docs_result.document_id}. "
                        f"Link: {docs_result.deep_link}"
                    )
                if docs_result.warning:
                    typer.echo(f"Warning: {docs_result.warning}")
                return

        if target not in {DeliveryTarget.GMAIL, DeliveryTarget.ALL}:
            typer.echo(f"Unsupported publish target: {target.value}", err=True)
            raise typer.Exit(code=1)

        gmail_result = run_gmail_publish_for_run(
            settings=settings,
            storage=storage,
            run_record=storage.get_run(run) or run_record,
            product=product,
            docs_result=docs_result,
        )
        storage.update_run_status(
            run,
            status="completed",
            stage=Stage.PUBLISH.value,
            metadata=gmail_result.to_metadata(),
            completed=True,
        )
        logger.info(
            "publish_gmail_completed",
            target=target.value,
            mode=gmail_result.publish_mode,
            action=gmail_result.publish_action,
            draft_id=gmail_result.draft_id,
            message_id=gmail_result.message_id,
            thread_id=gmail_result.thread_id,
            thread_link=gmail_result.thread_link,
            warning=gmail_result.warning,
        )

        assert docs_result is not None
        if target is DeliveryTarget.ALL:
            if docs_result.published:
                typer.echo(
                    "Published Docs section "
                    f"for run {run} into document {docs_result.document_id}. "
                    f"Link: {docs_result.deep_link}"
                )
            else:
                typer.echo(
                    "Docs section already exists "
                    f"for run {run} in document {docs_result.document_id}. "
                    f"Link: {docs_result.deep_link}"
                )

        if gmail_result.sent:
            typer.echo(
                "Sent stakeholder email "
                f"for run {run}. Thread: {gmail_result.thread_link or 'unavailable'}"
            )
        elif gmail_result.publish_action == "draft_reused":
            typer.echo(
                "Reused canonical Gmail draft "
                f"for run {run}. Thread: {gmail_result.thread_link or 'unavailable'}"
            )
        elif gmail_result.publish_action == "draft_updated":
            typer.echo(
                "Updated canonical Gmail draft "
                f"for run {run}. Thread: {gmail_result.thread_link or 'unavailable'}"
            )
        elif gmail_result.publish_action == "draft_created":
            typer.echo(
                "Created Gmail draft "
                f"for run {run}. Thread: {gmail_result.thread_link or 'unavailable'}"
            )
        else:
            typer.echo(
                "Stakeholder email was already sent "
                f"for run {run}. Thread: {gmail_result.thread_link or 'unavailable'}"
            )
        if docs_result.warning:
            typer.echo(f"Warning: {docs_result.warning}")
        if gmail_result.warning:
            typer.echo(f"Warning: {gmail_result.warning}")
    except Exception as exc:
        phase = "phase-5" if target is DeliveryTarget.DOCS else "phase-6"
        storage.update_run_status(
            run,
            status="failed",
            stage=Stage.PUBLISH.value,
            metadata={"phase": phase, "error": str(exc)},
            completed=True,
        )
        logger = get_logger("pulse.publish")
        logger.exception("publish_failed", error=str(exc), target=target.value)
        typer.echo(f"Publish failed for run {run}: {exc}", err=True)
        raise typer.Exit(code=1) from None
    finally:
        clear_run_context()


@app.command(name="run")
def run_pipeline(
    product: ProductOption,
    weeks: WeeksOption = None,
    iso_week: IsoWeekOption = None,
    target: TargetOption = DeliveryTarget.ALL,
) -> None:
    """Run the full weekly pulse with recovery and stage skipping."""
    settings, storage = _bootstrap()
    storage.initialize()
    storage.seed_products(load_products(settings))
    try:
        product_config = get_product_by_slug(product, settings)
    except KeyError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from None

    try:
        result = run_pipeline_for_product(
            settings=settings,
            storage=storage,
            product=product_config,
            iso_week=iso_week,
            lookback_weeks=weeks,
            target=target,
        )
        typer.echo(
            f"Run {result.run_id} completed for {result.product_slug} {result.iso_week} "
            f"(target={result.target.value}, resumed={str(result.resumed).lower()})."
        )
        typer.echo(f"Summary: {result.summary_path}")
    except Exception as exc:
        latest_run = storage.get_latest_run_for_product_week(
            product_config.slug,
            iso_week or current_iso_week(settings.timezone),
        )
        if latest_run is not None:
            typer.echo(
                f"Run {latest_run.run_id} failed for {product_config.slug}: {exc}",
                err=True,
            )
        else:
            typer.echo(f"Run failed for {product_config.slug}: {exc}", err=True)
        raise typer.Exit(code=1) from None


@app.command("run-weekly")
def run_weekly(
    weeks: WeeksOption = None,
    iso_week: IsoWeekOption = None,
    target: TargetOption = DeliveryTarget.ALL,
) -> None:
    """Run the weekly pulse for every active product."""
    settings, storage = _bootstrap()
    storage.initialize()
    products = load_products(settings)
    storage.seed_products(products)

    result = run_weekly_for_products(
        settings=settings,
        storage=storage,
        products=products,
        iso_week=iso_week,
        lookback_weeks=weeks,
        target=target,
    )
    for item in result.items:
        if item.status == "completed":
            typer.echo(
                f"{item.product_slug}\tcompleted\trun={item.run_id}\tsummary={item.summary_path}"
            )
        else:
            typer.echo(
                f"{item.product_slug}\tfailed\trun={item.run_id}\terror={item.error}",
                err=True,
            )

    if result.failed_count:
        raise typer.Exit(code=1)


@app.command("audit-run")
def audit_run(
    run: RunOption,
) -> None:
    """Print the stored audit payload for a run as JSON."""
    _, storage = _bootstrap()
    storage.initialize()
    try:
        run_record = _load_existing_run(storage, run)
    except KeyError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from None

    payload = build_run_audit_payload(storage=storage, run_record=run_record)
    typer.echo(json.dumps(payload, indent=2, sort_keys=True))


@app.command("list-products")
def list_products() -> None:
    """Print configured products."""
    settings, _ = _bootstrap()
    products = load_products(settings)
    for product in products:
        typer.echo(f"{product.slug}\t{product.display_name}")


@app.command("serve")
def serve(
    host: HostOption = "127.0.0.1",
    port: PortOption = 8000,
    reload: bool = False,
) -> None:
    """Serve the operator API used by the frontend dashboard."""
    import uvicorn

    uvicorn.run("agent.api:app", host=host, port=port, reload=reload)


def main() -> None:
    app()


if __name__ == "__main__":
    main()
