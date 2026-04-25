from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from agent.config import Settings

from .session import McpSession, McpToolCallResult, StdioJsonRpcTransport, parse_command_args


@dataclass(slots=True)
class GmailToolNames:
    create_draft: str
    update_draft: str
    send_draft: str


@dataclass(slots=True)
class GmailDraftResult:
    draft_id: str
    message_id: str | None = None
    thread_id: str | None = None
    draft_link: str | None = None
    thread_link: str | None = None


@dataclass(slots=True)
class GmailSendResult:
    message_id: str
    draft_id: str | None = None
    thread_id: str | None = None
    thread_link: str | None = None


class GmailMcpClient:
    def __init__(
        self,
        *,
        session: McpSession,
        tool_names: GmailToolNames,
    ) -> None:
        self.session = session
        self.tool_names = tool_names
        self._started = False

    @classmethod
    def from_settings(cls, settings: Settings) -> GmailMcpClient:
        if settings.gmail_mcp_command is None or not settings.gmail_mcp_command.strip():
            raise ValueError(
                "Gmail MCP is not configured. Set PULSE_GMAIL_MCP_COMMAND before publishing."
            )

        transport = StdioJsonRpcTransport(
            command=settings.gmail_mcp_command,
            args=parse_command_args(settings.gmail_mcp_args),
            cwd=settings.gmail_mcp_cwd,
            timeout_seconds=settings.gmail_mcp_timeout_seconds,
            message_mode=settings.gmail_mcp_message_mode,
        )
        session = McpSession(
            transport=transport,
            protocol_version=settings.mcp_protocol_version,
            client_name="weekly-product-review-pulse",
            client_version="0.1.0",
        )
        return cls(
            session=session,
            tool_names=GmailToolNames(
                create_draft=settings.gmail_mcp_tool_create_draft,
                update_draft=settings.gmail_mcp_tool_update_draft,
                send_draft=settings.gmail_mcp_tool_send_draft,
            ),
        )

    def __enter__(self) -> GmailMcpClient:
        self.start()
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def start(self) -> None:
        if self._started:
            return
        self.session.start(
            required_tools=[
                self.tool_names.create_draft,
                self.tool_names.update_draft,
                self.tool_names.send_draft,
            ]
        )
        self._started = True

    def close(self) -> None:
        self.session.close()
        self._started = False

    def create_draft(
        self,
        *,
        to: list[str],
        subject: str,
        plain_text_body: str,
        html_body: str,
        idempotency_key: str,
    ) -> GmailDraftResult:
        self.start()
        result = self.session.call_tool(
            self.tool_names.create_draft,
            {
                "to": to,
                "subject": subject,
                "body": plain_text_body,
            },
        )
        payload = _extract_payload(result)
        return _parse_draft_result(payload)

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
        self.start()
        arguments: dict[str, object] = {
            "draftId": draft_id,
            "to": to,
            "subject": subject,
            "body": plain_text_body,
        }
        result = self.session.call_tool(
            self.tool_names.update_draft,
            arguments,
        )
        payload = _extract_payload(result)
        return _parse_draft_result(payload, fallback_draft_id=draft_id)

    def send_draft(self, *, draft_id: str) -> GmailSendResult:
        self.start()
        result = self.session.call_tool(
            self.tool_names.send_draft,
            {"draftId": draft_id},
        )
        payload = _extract_payload(result)
        return _parse_send_result(payload, fallback_draft_id=draft_id)


def _extract_payload(result: McpToolCallResult) -> dict[str, Any]:
    if result.structured_content is not None:
        return result.structured_content

    for item in result.content:
        if not isinstance(item, dict):
            continue
        json_payload = item.get("json")
        if item.get("type") == "json" and isinstance(json_payload, dict):
            return json_payload
        text = item.get("text")
        if not isinstance(text, str):
            continue
        stripped = text.strip()
        if not stripped:
            continue
        if stripped.startswith("{"):
            parsed = json.loads(stripped)
            if isinstance(parsed, dict):
                return parsed

    raise ValueError("Gmail MCP tool returned no structured payload.")


def _parse_draft_result(
    payload: dict[str, Any],
    *,
    fallback_draft_id: str | None = None,
) -> GmailDraftResult:
    draft_id = _first_string(payload, "draft_id", "draftId", "id") or fallback_draft_id
    if draft_id is None:
        raise ValueError("Gmail MCP payload did not include a draft ID.")

    return GmailDraftResult(
        draft_id=draft_id,
        message_id=_first_string(payload, "message_id", "messageId"),
        thread_id=_first_string(payload, "thread_id", "threadId"),
        draft_link=_first_string(payload, "draft_link", "draftLink", "link", "url"),
        thread_link=_first_string(payload, "thread_link", "threadLink"),
    )


def _parse_send_result(
    payload: dict[str, Any],
    *,
    fallback_draft_id: str | None = None,
) -> GmailSendResult:
    message_id = _first_string(payload, "message_id", "messageId", "id")
    if message_id is None:
        raise ValueError("Gmail send result did not include a message ID.")

    return GmailSendResult(
        message_id=message_id,
        draft_id=_first_string(payload, "draft_id", "draftId") or fallback_draft_id,
        thread_id=_first_string(payload, "thread_id", "threadId"),
        thread_link=_first_string(payload, "thread_link", "threadLink", "link", "url"),
    )


def _first_string(payload: dict[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None
