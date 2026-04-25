from __future__ import annotations

from pathlib import Path
from typing import Protocol

from agent.config import Settings
from agent.logging import get_logger
from agent.mcp_client.docs_client import DocsAppendResult, DocsDocument, DocsMcpClient
from agent.publish.models import DocsPublishResult
from agent.pulse_types import DeliveryTarget, ProductConfig, StoredRunRecord
from agent.rendering.models import DocsRequestTree, RenderArtifact
from agent.storage import Storage
from agent.telemetry import record_publish_status, start_span


class DocsPublisherClient(Protocol):
    def ensure_document(
        self,
        *,
        preferred_document_id: str | None,
        title: str,
    ) -> DocsDocument: ...

    def get_document(self, document_id: str) -> DocsDocument: ...

    def append_section(
        self,
        *,
        document_id: str,
        request_tree: DocsRequestTree,
    ) -> DocsAppendResult: ...

    def close(self) -> None: ...


def build_docs_client(settings: Settings) -> DocsMcpClient:
    return DocsMcpClient.from_settings(settings)


def run_docs_publish_for_run(
    *,
    settings: Settings,
    storage: Storage,
    run_record: StoredRunRecord,
    product: ProductConfig,
    docs_client: DocsPublisherClient | None = None,
) -> DocsPublishResult:
    service = DocsPublishService(settings=settings, storage=storage)
    return service.run(run_record=run_record, product=product, docs_client=docs_client)


def hydrate_docs_publish_result(
    *,
    storage: Storage,
    run_record: StoredRunRecord,
) -> DocsPublishResult | None:
    metadata = run_record.metadata
    artifact_path_raw = metadata.get("render_artifact_path")
    if not isinstance(artifact_path_raw, str) or not artifact_path_raw.strip():
        return None

    artifact_path = Path(artifact_path_raw)
    document_id = _string_or_none(metadata.get("gdoc_id"))
    deep_link = _string_or_none(metadata.get("gdoc_deep_link"))
    heading_id = _string_or_none(metadata.get("gdoc_heading_id"))
    document_url = _string_or_none(metadata.get("gdoc_document_url"))
    payload_hash = _string_or_none(metadata.get("docs_payload_hash"))
    delivery = storage.get_delivery(run_record.run_id, DeliveryTarget.DOCS.value)

    if delivery is not None:
        if document_id is None and delivery.external_id:
            document_id = delivery.external_id.split("#", maxsplit=1)[0]
        if heading_id is None and delivery.external_id and "#" in delivery.external_id:
            heading_id = delivery.external_id.split("#", maxsplit=1)[1]
        if deep_link is None:
            deep_link = delivery.external_link
        if payload_hash is None:
            payload_hash = delivery.payload_hash

    if document_id is None or deep_link is None or payload_hash is None:
        return None

    return DocsPublishResult(
        run_id=run_record.run_id,
        product_slug=run_record.product_slug,
        iso_week=run_record.iso_week,
        delivery_id=(delivery.delivery_id if delivery is not None else f"{run_record.run_id}:docs"),
        document_id=document_id,
        heading_id=heading_id,
        deep_link=deep_link,
        document_url=document_url or _build_document_url(document_id),
        payload_hash=payload_hash,
        artifact_path=artifact_path,
        publish_action="already_exists",
        published=False,
        warning=None,
    )


class DocsPublishService:
    def __init__(self, *, settings: Settings, storage: Storage) -> None:
        self.settings = settings
        self.storage = storage
        self.logger = get_logger("pulse.publish.docs")

    def run(
        self,
        *,
        run_record: StoredRunRecord,
        product: ProductConfig,
        docs_client: DocsPublisherClient | None = None,
    ) -> DocsPublishResult:
        try:
            with start_span(
                "publish.docs",
                {
                    "product_slug": product.slug,
                    "iso_week": run_record.iso_week,
                },
            ):
                artifact, artifact_path = self._load_render_artifact(run_record)
                existing_delivery = self.storage.get_delivery(
                    run_record.run_id,
                    DeliveryTarget.DOCS.value,
                )

                owned_client = docs_client is None
                client = docs_client or build_docs_client(self.settings)
                try:
                    preferred_document_id = self._resolve_preferred_document_id(run_record, product)
                    document = client.ensure_document(
                        preferred_document_id=preferred_document_id,
                        title=(
                            f"{self.settings.docs_document_title_prefix} - "
                            f"{product.display_name}"
                        ),
                    )
                    self._persist_product_doc_id(product.slug, document.document_id)

                    existing_section = document.find_section(
                        machine_key_line=artifact.machine_key_line,
                        section_heading=artifact.section_heading,
                    )
                    if existing_section is not None:
                        heading_id = existing_section.heading_id or (
                            existing_delivery.external_id.split("#", maxsplit=1)[1]
                            if existing_delivery
                            and existing_delivery.external_id
                            and "#" in existing_delivery.external_id
                            else None
                        )
                        document_url = (
                            document.document_url
                            or run_record.metadata.get("gdoc_document_url")
                            or _build_document_url(document.document_id)
                        )
                        deep_link = (
                            existing_section.deep_link
                            or (existing_delivery.external_link if existing_delivery else None)
                            or document_url
                        )
                        delivery_id = self.storage.upsert_delivery(
                            run_id=run_record.run_id,
                            target=DeliveryTarget.DOCS.value,
                            status="completed",
                            external_id=_build_external_id(document.document_id, heading_id),
                            external_link=deep_link,
                            payload_hash=artifact.docs_payload_hash,
                        )
                        result = DocsPublishResult(
                            run_id=run_record.run_id,
                            product_slug=product.slug,
                            iso_week=run_record.iso_week,
                            delivery_id=delivery_id,
                            document_id=document.document_id,
                            heading_id=heading_id,
                            deep_link=deep_link,
                            document_url=document_url,
                            payload_hash=artifact.docs_payload_hash,
                            artifact_path=artifact_path,
                            publish_action="already_exists",
                            published=False,
                            warning=(
                                None
                                if heading_id
                                else "Existing section found but heading ID was unavailable."
                            ),
                        )
                        record_publish_status(
                            target=DeliveryTarget.DOCS.value,
                            status="completed",
                            action=result.publish_action,
                        )
                        self.logger.info(
                            "docs_publish_reused",
                            run_id=run_record.run_id,
                            document_id=document.document_id,
                            deep_link=deep_link,
                        )
                        return result

                    append_result = client.append_section(
                        document_id=document.document_id,
                        request_tree=artifact.docs_request_tree,
                    )
                    resolved = self._resolve_append_metadata(
                        client=client,
                        artifact=artifact,
                        document=document,
                        append_result=append_result,
                    )
                    delivery_id = self.storage.upsert_delivery(
                        run_id=run_record.run_id,
                        target=DeliveryTarget.DOCS.value,
                        status="completed",
                        external_id=_build_external_id(document.document_id, resolved.heading_id),
                        external_link=resolved.deep_link,
                        payload_hash=artifact.docs_payload_hash,
                    )
                    result = DocsPublishResult(
                        run_id=run_record.run_id,
                        product_slug=product.slug,
                        iso_week=run_record.iso_week,
                        delivery_id=delivery_id,
                        document_id=document.document_id,
                        heading_id=resolved.heading_id,
                        deep_link=resolved.deep_link,
                        document_url=resolved.document_url,
                        payload_hash=artifact.docs_payload_hash,
                        artifact_path=artifact_path,
                        publish_action="appended",
                        published=True,
                        warning=resolved.warning,
                    )
                    record_publish_status(
                        target=DeliveryTarget.DOCS.value,
                        status="completed",
                        action=result.publish_action,
                    )
                    self.logger.info(
                        "docs_publish_completed",
                        run_id=run_record.run_id,
                        document_id=document.document_id,
                        heading_id=resolved.heading_id,
                        deep_link=resolved.deep_link,
                        warning=resolved.warning,
                    )
                    return result
                finally:
                    if owned_client:
                        client.close()
        except Exception:
            record_publish_status(
                target=DeliveryTarget.DOCS.value,
                status="failed",
                action="error",
            )
            raise

    def _load_render_artifact(self, run_record: StoredRunRecord) -> tuple[RenderArtifact, Path]:
        artifact_path_raw = run_record.metadata.get("render_artifact_path")
        if not isinstance(artifact_path_raw, str) or not artifact_path_raw.strip():
            raise FileNotFoundError(
                f"Run {run_record.run_id} does not have a render artifact path in metadata."
            )

        artifact_path = Path(artifact_path_raw)
        if not artifact_path.exists():
            raise FileNotFoundError(f"Render artifact not found: {artifact_path}")

        artifact = RenderArtifact.model_validate_json(artifact_path.read_text(encoding="utf-8"))
        if artifact.run_id != run_record.run_id:
            raise ValueError(
                f"Render artifact run id {artifact.run_id} did not match {run_record.run_id}."
            )
        return artifact, artifact_path

    def _resolve_preferred_document_id(
        self,
        run_record: StoredRunRecord,
        product: ProductConfig,
    ) -> str | None:
        metadata_doc_id = run_record.metadata.get("gdoc_id")
        if (
            isinstance(metadata_doc_id, str)
            and metadata_doc_id.strip()
            and not _is_placeholder_id(metadata_doc_id)
        ):
            return metadata_doc_id.strip()

        stored_doc_id = self.storage.get_product_google_doc_id(product.slug)
        if stored_doc_id:
            return stored_doc_id

        if (
            product.google_doc_id is not None
            and product.google_doc_id.strip()
            and not _is_placeholder_id(product.google_doc_id)
        ):
            return product.google_doc_id.strip()
        return None

    def _persist_product_doc_id(self, product_slug: str, document_id: str) -> None:
        stored_doc_id = self.storage.get_product_google_doc_id(product_slug)
        if stored_doc_id == document_id:
            return
        self.storage.update_product_google_doc_id(product_slug, document_id)

    def _resolve_append_metadata(
        self,
        *,
        client: DocsPublisherClient,
        artifact: RenderArtifact,
        document: DocsDocument,
        append_result: DocsAppendResult,
    ) -> _ResolvedAppendMetadata:
        document_url = (
            append_result.document_url
            or document.document_url
            or _build_document_url(document.document_id)
        )
        deep_link = append_result.deep_link or document_url
        heading_id = append_result.heading_id
        warning: str | None = None

        if heading_id is None or append_result.deep_link is None:
            refreshed = client.get_document(document.document_id)
            matched_section = refreshed.find_section(
                machine_key_line=artifact.machine_key_line,
                section_heading=artifact.section_heading,
            )
            if matched_section is not None:
                heading_id = heading_id or matched_section.heading_id
                deep_link = matched_section.deep_link or deep_link
                document_url = refreshed.document_url or document_url

        if heading_id is None:
            warning = "Docs publish succeeded but heading ID was unavailable; using document URL."
            deep_link = deep_link or document_url

        return _ResolvedAppendMetadata(
            heading_id=heading_id,
            deep_link=deep_link,
            document_url=document_url,
            warning=warning,
        )


class _ResolvedAppendMetadata:
    def __init__(
        self,
        *,
        heading_id: str | None,
        deep_link: str,
        document_url: str,
        warning: str | None,
    ) -> None:
        self.heading_id = heading_id
        self.deep_link = deep_link
        self.document_url = document_url
        self.warning = warning


def _build_external_id(document_id: str, heading_id: str | None) -> str:
    if heading_id:
        return f"{document_id}#{heading_id}"
    return document_id


def _build_document_url(document_id: str) -> str:
    return f"https://docs.google.com/document/d/{document_id}/edit"


def _is_placeholder_id(value: str) -> bool:
    return value.strip().lower().startswith("replace-with-")


def _string_or_none(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None
