from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from agent.config import Settings
from agent.rendering.models import DocsRequestTree

from .session import McpSession, McpToolCallResult, StdioJsonRpcTransport, parse_command_args


@dataclass(slots=True)
class DocsToolNames:
    get_document: str
    create_document: str
    append_section: str


@dataclass(slots=True)
class DocsSection:
    heading: str | None = None
    heading_id: str | None = None
    deep_link: str | None = None
    text_content: str = ""
    machine_key_line: str | None = None


@dataclass(slots=True)
class DocsDocument:
    document_id: str
    title: str | None = None
    document_url: str | None = None
    text_content: str = ""
    sections: list[DocsSection] = field(default_factory=list)

    def find_section(
        self,
        *,
        machine_key_line: str,
        section_heading: str,
    ) -> DocsSection | None:
        for section in self.sections:
            if section.machine_key_line == machine_key_line:
                return section
            if machine_key_line and machine_key_line in section.text_content:
                return section

        if machine_key_line in self.text_content:
            for section in self.sections:
                if section.heading == section_heading:
                    return section
            return DocsSection(
                heading=section_heading,
                deep_link=self.document_url,
                text_content=machine_key_line,
                machine_key_line=machine_key_line,
            )
        return None


@dataclass(slots=True)
class DocsAppendResult:
    document_id: str
    heading_id: str | None = None
    deep_link: str | None = None
    document_url: str | None = None


class DocsMcpClient:
    def __init__(
        self,
        *,
        session: McpSession,
        tool_names: DocsToolNames,
    ) -> None:
        self.session = session
        self.tool_names = tool_names
        self._started = False

    @classmethod
    def from_settings(cls, settings: Settings) -> DocsMcpClient:
        if settings.docs_mcp_command is None or not settings.docs_mcp_command.strip():
            raise ValueError(
                "Docs MCP is not configured. Set PULSE_DOCS_MCP_COMMAND before publishing."
            )

        transport = StdioJsonRpcTransport(
            command=settings.docs_mcp_command,
            args=parse_command_args(settings.docs_mcp_args),
            cwd=settings.docs_mcp_cwd,
            timeout_seconds=settings.docs_mcp_timeout_seconds,
            message_mode=settings.docs_mcp_message_mode,
        )
        session = McpSession(
            transport=transport,
            protocol_version=settings.mcp_protocol_version,
            client_name="weekly-product-review-pulse",
            client_version="0.1.0",
        )
        return cls(
            session=session,
            tool_names=DocsToolNames(
                get_document=settings.docs_mcp_tool_get_document,
                create_document=settings.docs_mcp_tool_create_document,
                append_section=settings.docs_mcp_tool_append_section,
            ),
        )

    def __enter__(self) -> DocsMcpClient:
        self.start()
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def start(self) -> None:
        if self._started:
            return
        self.session.start(
            required_tools=[
                self.tool_names.get_document,
                self.tool_names.create_document,
                self.tool_names.append_section,
            ]
        )
        self._started = True

    def close(self) -> None:
        self.session.close()
        self._started = False

    def ensure_document(
        self,
        *,
        preferred_document_id: str | None,
        title: str,
    ) -> DocsDocument:
        self.start()
        if preferred_document_id is not None and not _is_placeholder_id(preferred_document_id):
            return self.get_document(preferred_document_id)
        return self.create_document(title)

    def get_document(self, document_id: str) -> DocsDocument:
        self.start()
        result = self.session.call_tool(
            self.tool_names.get_document,
            {
                "documentId": document_id,
                "format": "text",
            },
        )
        try:
            payload = _extract_payload(result)
        except ValueError:
            return DocsDocument(
                document_id=document_id,
                document_url=_build_document_url(document_id),
                text_content=_normalize_read_document_text(_extract_text_content_from_result(result)),
            )

        document = _parse_document(payload, fallback_document_id=document_id)
        if not document.document_url:
            document.document_url = _build_document_url(document_id)
        return document

    def create_document(self, title: str) -> DocsDocument:
        self.start()
        result = self.session.call_tool(
            self.tool_names.create_document,
            {"title": title},
        )
        payload = _extract_payload(result)
        document = _parse_document(payload, fallback_document_id=None)
        if not document.title:
            document.title = title
        if not document.document_url:
            document.document_url = _build_document_url(document.document_id)
        return document

    def append_section(
        self,
        *,
        document_id: str,
        request_tree: DocsRequestTree,
    ) -> DocsAppendResult:
        self.start()
        result = self.session.call_tool(
            self.tool_names.append_section,
            {
                "documentId": document_id,
                "markdown": request_tree.markdown,
            },
        )
        try:
            payload = _extract_payload(result)
        except ValueError:
            return DocsAppendResult(
                document_id=document_id,
                document_url=_build_document_url(document_id),
            )

        append_result = _parse_append_result(payload, fallback_document_id=document_id)
        if not append_result.document_url:
            append_result.document_url = _build_document_url(document_id)
        return append_result


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

    raise ValueError("Docs MCP tool returned no structured payload.")


def _extract_text_content_from_result(result: McpToolCallResult) -> str:
    lines: list[str] = []
    for item in result.content:
        if not isinstance(item, dict):
            continue
        text = item.get("text")
        if isinstance(text, str) and text.strip():
            lines.append(text)
    return "\n".join(lines)


def _parse_document(
    payload: dict[str, Any],
    *,
    fallback_document_id: str | None,
) -> DocsDocument:
    document_id = _first_string(
        payload,
        "document_id",
        "documentId",
        "doc_id",
        "docId",
        "id",
    )
    if document_id is None:
        if fallback_document_id is None:
            raise ValueError("Docs MCP payload did not include a document ID.")
        document_id = fallback_document_id

    sections = _parse_sections(payload.get("sections")) + _parse_sections(payload.get("headings"))
    return DocsDocument(
        document_id=document_id,
        title=_first_string(payload, "title", "name"),
        document_url=_first_string(payload, "document_url", "documentUrl", "url", "link"),
        text_content=_extract_text_content(payload),
        sections=sections,
    )


def _parse_append_result(
    payload: dict[str, Any],
    *,
    fallback_document_id: str,
) -> DocsAppendResult:
    document_id = _first_string(
        payload,
        "document_id",
        "documentId",
        "doc_id",
        "docId",
        "id",
    ) or fallback_document_id
    return DocsAppendResult(
        document_id=document_id,
        heading_id=_first_string(
            payload,
            "heading_id",
            "headingId",
            "anchor_id",
            "anchorId",
            "section_id",
            "sectionId",
        ),
        deep_link=_first_string(payload, "deep_link", "deepLink", "heading_link", "headingLink"),
        document_url=_first_string(payload, "document_url", "documentUrl", "url", "link"),
    )


def _parse_sections(raw_sections: object) -> list[DocsSection]:
    if not isinstance(raw_sections, list):
        return []

    sections: list[DocsSection] = []
    for item in raw_sections:
        if not isinstance(item, dict):
            continue
        text_content = _extract_text_content(item)
        machine_key_line = _extract_machine_key_line(text_content) or _first_string(
            item,
            "machine_key_line",
            "machineKeyLine",
        )
        sections.append(
            DocsSection(
                heading=_first_string(item, "heading", "title", "name"),
                heading_id=_first_string(
                    item,
                    "heading_id",
                    "headingId",
                    "anchor_id",
                    "anchorId",
                    "section_id",
                    "sectionId",
                    "id",
                ),
                deep_link=_first_string(
                    item,
                    "deep_link",
                    "deepLink",
                    "url",
                    "link",
                ),
                text_content=text_content,
                machine_key_line=machine_key_line,
            )
        )
    return sections


def _extract_text_content(payload: object) -> str:
    if isinstance(payload, str):
        return payload
    if isinstance(payload, list):
        return "\n".join(part for part in (_extract_text_content(item) for item in payload) if part)
    if not isinstance(payload, dict):
        return ""

    for key in ("markdown", "text", "body", "document_text", "plain_text", "content"):
        value = payload.get(key)
        text = _extract_text_content(value)
        if text:
            return text

    if "items" in payload and isinstance(payload["items"], list):
        return "\n".join(
            part for part in (_extract_text_content(item) for item in payload["items"]) if part
        )
    return ""


def _normalize_read_document_text(raw_text: str) -> str:
    stripped = raw_text.strip()
    if not stripped or stripped == "Document found, but appears empty.":
        return ""

    delimiter = "\n---\n"
    if delimiter in raw_text:
        return raw_text.split(delimiter, maxsplit=1)[1]
    return stripped


def _extract_machine_key_line(text_content: str) -> str | None:
    for line in text_content.splitlines():
        stripped = line.strip()
        if stripped.startswith("Run key: "):
            return stripped
    return None


def _first_string(payload: dict[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _is_placeholder_id(value: str) -> bool:
    return value.strip().lower().startswith("replace-with-")


def _build_document_url(document_id: str) -> str:
    return f"https://docs.google.com/document/d/{document_id}/edit"
