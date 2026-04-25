"""MCP client helpers used by downstream publish phases."""

from .docs_client import DocsAppendResult, DocsDocument, DocsMcpClient, DocsSection
from .gmail_client import GmailDraftResult, GmailMcpClient, GmailSendResult, GmailToolNames
from .session import (
    McpSession,
    McpSessionError,
    McpToolCallError,
    McpToolCallResult,
    StdioJsonRpcTransport,
    parse_command_args,
)

__all__ = [
    "DocsAppendResult",
    "DocsDocument",
    "DocsMcpClient",
    "DocsSection",
    "GmailDraftResult",
    "GmailMcpClient",
    "GmailSendResult",
    "GmailToolNames",
    "McpSession",
    "McpSessionError",
    "McpToolCallError",
    "McpToolCallResult",
    "StdioJsonRpcTransport",
    "parse_command_args",
]
