from __future__ import annotations

import re
from pathlib import Path
from typing import Literal, Protocol

from agent.config import Settings
from agent.logging import get_logger
from agent.mcp_client.gmail_client import GmailDraftResult, GmailMcpClient, GmailSendResult
from agent.publish.docs_pipeline import (
    DocsPublisherClient,
    hydrate_docs_publish_result,
    run_docs_publish_for_run,
)
from agent.publish.models import DocsPublishResult, GmailPublishResult
from agent.pulse_types import DeliveryTarget, ProductConfig, StoredRunRecord
from agent.rendering.models import DOC_SECTION_URL_PLACEHOLDER, RenderArtifact
from agent.storage import Storage
from agent.telemetry import record_publish_status, start_span

EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


class GmailPublisherClient(Protocol):
    def create_draft(
        self,
        *,
        to: list[str],
        subject: str,
        plain_text_body: str,
        html_body: str,
        idempotency_key: str,
    ) -> GmailDraftResult: ...

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
    ) -> GmailDraftResult: ...

    def send_draft(self, *, draft_id: str) -> GmailSendResult: ...

    def close(self) -> None: ...


def build_gmail_client(settings: Settings) -> GmailMcpClient:
    return GmailMcpClient.from_settings(settings)


def run_gmail_publish_for_run(
    *,
    settings: Settings,
    storage: Storage,
    run_record: StoredRunRecord,
    product: ProductConfig,
    docs_result: DocsPublishResult | None = None,
    docs_client: DocsPublisherClient | None = None,
    gmail_client: GmailPublisherClient | None = None,
) -> GmailPublishResult:
    service = GmailPublishService(settings=settings, storage=storage)
    return service.run(
        run_record=run_record,
        product=product,
        docs_result=docs_result,
        docs_client=docs_client,
        gmail_client=gmail_client,
    )


class GmailPublishService:
    def __init__(self, *, settings: Settings, storage: Storage) -> None:
        self.settings = settings
        self.storage = storage
        self.logger = get_logger("pulse.publish.gmail")

    def run(
        self,
        *,
        run_record: StoredRunRecord,
        product: ProductConfig,
        docs_result: DocsPublishResult | None = None,
        docs_client: DocsPublisherClient | None = None,
        gmail_client: GmailPublisherClient | None = None,
    ) -> GmailPublishResult:
        try:
            with start_span(
                "publish.gmail",
                {
                    "product_slug": product.slug,
                    "iso_week": run_record.iso_week,
                },
            ):
                artifact, artifact_path = self._load_render_artifact(run_record)
                resolved_docs_result = docs_result or hydrate_docs_publish_result(
                    storage=self.storage,
                    run_record=run_record,
                )
                if resolved_docs_result is None:
                    resolved_docs_result = run_docs_publish_for_run(
                        settings=self.settings,
                        storage=self.storage,
                        run_record=run_record,
                        product=product,
                        docs_client=docs_client,
                    )

                recipients = _validate_recipients(product.stakeholder_emails)
                if not recipients:
                    raise ValueError(
                        f"Product {product.slug} has no valid stakeholder emails configured."
                    )

                plain_text_body = artifact.email_teaser.plain_text_template.replace(
                    DOC_SECTION_URL_PLACEHOLDER,
                    resolved_docs_result.deep_link,
                )
                html_body = artifact.email_teaser.html_template.replace(
                    DOC_SECTION_URL_PLACEHOLDER,
                    resolved_docs_result.deep_link,
                )
                subject = artifact.email_teaser.subject
                idempotency_key = f"{artifact.anchor_key}:gmail"
                publish_mode: Literal["draft", "send"] = (
                    "send" if self.settings.confirm_send else "draft"
                )

                existing_state = self._load_existing_state(run_record)
                if existing_state.message_id:
                    warning: str | None = None
                    if (
                        existing_state.payload_hash
                        and existing_state.payload_hash != artifact.email_payload_hash
                    ):
                        warning = (
                            "Email was already sent for this run; not sending a second "
                            "stakeholder email even though the teaser payload has changed."
                        )
                    delivery_id = self.storage.upsert_delivery(
                        run_id=run_record.run_id,
                        target=DeliveryTarget.GMAIL.value,
                        status="sent",
                        external_id=existing_state.message_id,
                        external_link=existing_state.thread_link,
                        payload_hash=existing_state.payload_hash or artifact.email_payload_hash,
                    )
                    result = GmailPublishResult(
                        run_id=run_record.run_id,
                        product_slug=product.slug,
                        iso_week=run_record.iso_week,
                        delivery_id=delivery_id,
                        docs_deep_link=resolved_docs_result.deep_link,
                        draft_id=existing_state.draft_id,
                        message_id=existing_state.message_id,
                        thread_id=existing_state.thread_id,
                        thread_link=existing_state.thread_link,
                        payload_hash=existing_state.payload_hash or artifact.email_payload_hash,
                        artifact_path=artifact_path,
                        publish_mode=publish_mode,
                        publish_action="already_sent",
                        drafted=bool(existing_state.draft_id),
                        sent=True,
                        warning=warning,
                    )
                    record_publish_status(
                        target=DeliveryTarget.GMAIL.value,
                        status="completed",
                        action=result.publish_action,
                        mode=result.publish_mode,
                    )
                    self.logger.info(
                        "gmail_publish_reused_sent_message",
                        run_id=run_record.run_id,
                        message_id=result.message_id,
                        thread_id=result.thread_id,
                        warning=result.warning,
                    )
                    return result

                owned_client = gmail_client is None
                client = gmail_client or build_gmail_client(self.settings)
                try:
                    if publish_mode == "draft":
                        result = self._run_draft_mode(
                            client=client,
                            run_record=run_record,
                            product=product,
                            docs_result=resolved_docs_result,
                            artifact=artifact,
                            artifact_path=artifact_path,
                            recipients=recipients,
                            subject=subject,
                            plain_text_body=plain_text_body,
                            html_body=html_body,
                            idempotency_key=idempotency_key,
                            existing_state=existing_state,
                        )
                    else:
                        result = self._run_send_mode(
                            client=client,
                            run_record=run_record,
                            product=product,
                            docs_result=resolved_docs_result,
                            artifact=artifact,
                            artifact_path=artifact_path,
                            recipients=recipients,
                            subject=subject,
                            plain_text_body=plain_text_body,
                            html_body=html_body,
                            idempotency_key=idempotency_key,
                            existing_state=existing_state,
                        )
                    record_publish_status(
                        target=DeliveryTarget.GMAIL.value,
                        status="completed",
                        action=result.publish_action,
                        mode=result.publish_mode,
                    )
                    return result
                finally:
                    if owned_client:
                        client.close()
        except Exception:
            record_publish_status(
                target=DeliveryTarget.GMAIL.value,
                status="failed",
                action="error",
                mode="send" if self.settings.confirm_send else "draft",
            )
            raise

    def _run_draft_mode(
        self,
        *,
        client: GmailPublisherClient,
        run_record: StoredRunRecord,
        product: ProductConfig,
        docs_result: DocsPublishResult,
        artifact: RenderArtifact,
        artifact_path: Path,
        recipients: list[str],
        subject: str,
        plain_text_body: str,
        html_body: str,
        idempotency_key: str,
        existing_state: _ExistingGmailState,
    ) -> GmailPublishResult:
        if existing_state.draft_id and existing_state.payload_hash == artifact.email_payload_hash:
            delivery_id = self.storage.upsert_delivery(
                run_id=run_record.run_id,
                target=DeliveryTarget.GMAIL.value,
                status="drafted",
                external_id=existing_state.draft_id,
                external_link=existing_state.thread_link,
                payload_hash=artifact.email_payload_hash,
            )
            result = GmailPublishResult(
                run_id=run_record.run_id,
                product_slug=product.slug,
                iso_week=run_record.iso_week,
                delivery_id=delivery_id,
                docs_deep_link=docs_result.deep_link,
                draft_id=existing_state.draft_id,
                message_id=None,
                thread_id=existing_state.thread_id,
                thread_link=existing_state.thread_link,
                payload_hash=artifact.email_payload_hash,
                artifact_path=artifact_path,
                publish_mode="draft",
                publish_action="draft_reused",
                drafted=True,
                sent=False,
                warning=None,
            )
            self.logger.info(
                "gmail_publish_reused_draft",
                run_id=run_record.run_id,
                draft_id=result.draft_id,
                thread_id=result.thread_id,
            )
            return result

        publish_action: Literal["draft_created", "draft_updated"]
        if existing_state.draft_id:
            draft_result = client.update_draft(
                draft_id=existing_state.draft_id,
                to=recipients,
                subject=subject,
                plain_text_body=plain_text_body,
                html_body=html_body,
                idempotency_key=idempotency_key,
                thread_id=existing_state.thread_id,
            )
            publish_action = "draft_updated"
        else:
            draft_result = client.create_draft(
                to=recipients,
                subject=subject,
                plain_text_body=plain_text_body,
                html_body=html_body,
                idempotency_key=idempotency_key,
            )
            publish_action = "draft_created"

        thread_id = draft_result.thread_id or existing_state.thread_id
        thread_link = (
            draft_result.thread_link
            or _build_thread_link(thread_id)
            or draft_result.draft_link
            or existing_state.thread_link
        )
        delivery_id = self.storage.upsert_delivery(
            run_id=run_record.run_id,
            target=DeliveryTarget.GMAIL.value,
            status="drafted",
            external_id=draft_result.draft_id,
            external_link=thread_link,
            payload_hash=artifact.email_payload_hash,
        )
        result = GmailPublishResult(
            run_id=run_record.run_id,
            product_slug=product.slug,
            iso_week=run_record.iso_week,
            delivery_id=delivery_id,
            docs_deep_link=docs_result.deep_link,
            draft_id=draft_result.draft_id,
            message_id=None,
            thread_id=thread_id,
            thread_link=thread_link,
            payload_hash=artifact.email_payload_hash,
            artifact_path=artifact_path,
            publish_mode="draft",
            publish_action=publish_action,
            drafted=True,
            sent=False,
            warning=None if thread_link else "Draft created without a reusable Gmail thread link.",
        )
        self.logger.info(
            "gmail_publish_drafted",
            run_id=run_record.run_id,
            draft_id=result.draft_id,
            thread_id=result.thread_id,
            action=publish_action,
        )
        return result

    def _run_send_mode(
        self,
        *,
        client: GmailPublisherClient,
        run_record: StoredRunRecord,
        product: ProductConfig,
        docs_result: DocsPublishResult,
        artifact: RenderArtifact,
        artifact_path: Path,
        recipients: list[str],
        subject: str,
        plain_text_body: str,
        html_body: str,
        idempotency_key: str,
        existing_state: _ExistingGmailState,
    ) -> GmailPublishResult:
        prepared_draft_id: str
        prepared_thread_id = existing_state.thread_id
        prepared_thread_link = existing_state.thread_link
        prepared_draft_link: str | None = None

        if existing_state.draft_id and existing_state.payload_hash == artifact.email_payload_hash:
            prepared_draft_id = existing_state.draft_id
        elif existing_state.draft_id:
            draft_result = client.update_draft(
                draft_id=existing_state.draft_id,
                to=recipients,
                subject=subject,
                plain_text_body=plain_text_body,
                html_body=html_body,
                idempotency_key=idempotency_key,
                thread_id=existing_state.thread_id,
            )
            prepared_draft_id = draft_result.draft_id
            prepared_thread_id = draft_result.thread_id or prepared_thread_id
            prepared_thread_link = (
                draft_result.thread_link
                or _build_thread_link(draft_result.thread_id)
                or prepared_thread_link
            )
            prepared_draft_link = draft_result.draft_link
        else:
            draft_result = client.create_draft(
                to=recipients,
                subject=subject,
                plain_text_body=plain_text_body,
                html_body=html_body,
                idempotency_key=idempotency_key,
            )
            prepared_draft_id = draft_result.draft_id
            prepared_thread_id = draft_result.thread_id or prepared_thread_id
            prepared_thread_link = (
                draft_result.thread_link
                or _build_thread_link(draft_result.thread_id)
                or prepared_thread_link
            )
            prepared_draft_link = draft_result.draft_link

        send_result = client.send_draft(draft_id=prepared_draft_id)
        thread_id = send_result.thread_id or prepared_thread_id
        thread_link = (
            send_result.thread_link
            or _build_thread_link(thread_id)
            or prepared_thread_link
            or prepared_draft_link
        )
        delivery_id = self.storage.upsert_delivery(
            run_id=run_record.run_id,
            target=DeliveryTarget.GMAIL.value,
            status="sent",
            external_id=send_result.message_id,
            external_link=thread_link,
            payload_hash=artifact.email_payload_hash,
        )
        warning = None if thread_link else "Email sent without a Gmail thread link."
        result = GmailPublishResult(
            run_id=run_record.run_id,
            product_slug=product.slug,
            iso_week=run_record.iso_week,
            delivery_id=delivery_id,
            docs_deep_link=docs_result.deep_link,
            draft_id=send_result.draft_id or prepared_draft_id,
            message_id=send_result.message_id,
            thread_id=thread_id,
            thread_link=thread_link,
            payload_hash=artifact.email_payload_hash,
            artifact_path=artifact_path,
            publish_mode="send",
            publish_action="sent",
            drafted=bool(send_result.draft_id or prepared_draft_id),
            sent=True,
            warning=warning,
        )
        self.logger.info(
            "gmail_publish_sent",
            run_id=run_record.run_id,
            message_id=result.message_id,
            thread_id=result.thread_id,
            draft_id=result.draft_id,
            warning=result.warning,
        )
        return result

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

    def _load_existing_state(self, run_record: StoredRunRecord) -> _ExistingGmailState:
        existing_delivery = self.storage.get_delivery(run_record.run_id, DeliveryTarget.GMAIL.value)
        metadata = run_record.metadata

        draft_id = _string_or_none(metadata.get("gmail_draft_id"))
        if draft_id is None and existing_delivery and existing_delivery.status == "drafted":
            draft_id = existing_delivery.external_id

        message_id = _string_or_none(metadata.get("gmail_message_id"))
        if message_id is None and existing_delivery and existing_delivery.status == "sent":
            message_id = existing_delivery.external_id

        thread_id = _string_or_none(metadata.get("gmail_thread_id"))
        thread_link = _string_or_none(metadata.get("gmail_thread_link"))
        if thread_link is None and existing_delivery:
            thread_link = existing_delivery.external_link
        if thread_link is None and thread_id is not None:
            thread_link = _build_thread_link(thread_id)

        payload_hash = _string_or_none(metadata.get("gmail_payload_hash"))
        if payload_hash is None and existing_delivery:
            payload_hash = existing_delivery.payload_hash

        return _ExistingGmailState(
            draft_id=draft_id,
            message_id=message_id,
            thread_id=thread_id,
            thread_link=thread_link,
            payload_hash=payload_hash,
        )


class _ExistingGmailState:
    def __init__(
        self,
        *,
        draft_id: str | None,
        message_id: str | None,
        thread_id: str | None,
        thread_link: str | None,
        payload_hash: str | None,
    ) -> None:
        self.draft_id = draft_id
        self.message_id = message_id
        self.thread_id = thread_id
        self.thread_link = thread_link
        self.payload_hash = payload_hash


def _validate_recipients(emails: list[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for email in emails:
        candidate = email.strip()
        if not candidate:
            continue
        lowered = candidate.casefold()
        if lowered in seen:
            continue
        if not EMAIL_PATTERN.match(candidate):
            raise ValueError(f"Invalid stakeholder email configured: {candidate}")
        normalized.append(candidate)
        seen.add(lowered)
    return normalized


def _build_thread_link(thread_id: str | None) -> str | None:
    if thread_id is None or not thread_id.strip():
        return None
    return f"https://mail.google.com/mail/u/0/#inbox/{thread_id}"


def _string_or_none(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None
