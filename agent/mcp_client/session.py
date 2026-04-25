from __future__ import annotations

import json
import queue
import shlex
import subprocess
import threading
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path
from time import perf_counter
from typing import Any, Protocol

from agent.telemetry import record_mcp_call_latency_ms, start_span

_STREAM_CLOSED = object()


class McpSessionError(RuntimeError):
    """Raised when the MCP transport or protocol handshake fails."""


class McpToolCallError(McpSessionError):
    """Raised when an MCP tool call returns an error result."""

    def __init__(self, tool_name: str, message: str) -> None:
        super().__init__(f"MCP tool `{tool_name}` failed: {message}")
        self.tool_name = tool_name
        self.message = message


class JsonRpcTransport(Protocol):
    def start(self) -> None: ...

    def request(self, method: str, params: dict[str, Any] | None = None) -> Any: ...

    def notify(self, method: str, params: dict[str, Any] | None = None) -> None: ...

    def close(self) -> None: ...


@dataclass(slots=True)
class McpToolDefinition:
    name: str
    description: str | None = None


@dataclass(slots=True)
class McpToolCallResult:
    raw_result: dict[str, Any]
    content: list[dict[str, Any]] = field(default_factory=list)
    structured_content: dict[str, Any] | None = None
    is_error: bool = False


class StdioJsonRpcTransport:
    def __init__(
        self,
        *,
        command: str,
        args: Sequence[str] = (),
        cwd: Path | None = None,
        timeout_seconds: float = 20.0,
        message_mode: str = "content-length",
    ) -> None:
        self.command = command
        self.args = list(args)
        self.cwd = cwd
        self.timeout_seconds = timeout_seconds
        self.message_mode = message_mode
        self._process: subprocess.Popen[bytes] | None = None
        self._reader_thread: threading.Thread | None = None
        self._stderr_thread: threading.Thread | None = None
        self._message_queue: queue.Queue[dict[str, Any] | object] = queue.Queue()
        self._pending_responses: dict[object, dict[str, Any]] = {}
        self._stderr_lines: list[str] = []
        self._next_request_id = 1
        self._write_lock = threading.Lock()

    def start(self) -> None:
        if self._process is not None:
            return

        self._message_queue = queue.Queue()
        self._pending_responses = {}
        self._stderr_lines = []

        process = subprocess.Popen(
            [self.command, *self.args],
            cwd=self.cwd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=0,
        )
        self._process = process
        assert process.stdout is not None
        assert process.stderr is not None
        self._reader_thread = threading.Thread(
            target=self._reader_loop,
            args=(process.stdout,),
            daemon=True,
        )
        self._stderr_thread = threading.Thread(
            target=self._stderr_loop,
            args=(process.stderr,),
            daemon=True,
        )
        self._reader_thread.start()
        self._stderr_thread.start()

    def request(self, method: str, params: dict[str, Any] | None = None) -> Any:
        request_params = params or {}
        tool_name = _tool_name_for_request(method, request_params)
        start = perf_counter()
        with start_span(
            "mcp.request",
            {
                "mcp_method": method,
                "tool_name": tool_name or "",
            },
        ):
            try:
                self.start()
                request_id = self._next_request_id
                self._next_request_id += 1
                self._send_message(
                    {
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "method": method,
                        "params": request_params,
                    }
                )

                if request_id in self._pending_responses:
                    result = self._resolve_response(method, self._pending_responses.pop(request_id))
                    record_mcp_call_latency_ms(
                        duration_ms=(perf_counter() - start) * 1000,
                        method=method,
                        status="ok",
                        tool_name=tool_name,
                    )
                    return result

                while True:
                    try:
                        incoming = self._message_queue.get(timeout=self.timeout_seconds)
                    except queue.Empty as exc:
                        raise TimeoutError(
                            f"Timed out waiting for MCP response to `{method}` after "
                            f"{self.timeout_seconds:.1f}s."
                        ) from exc

                    if incoming is _STREAM_CLOSED:
                        raise McpSessionError(self._build_process_exit_message())

                    if not isinstance(incoming, dict):
                        continue

                    incoming_id = incoming.get("id")
                    if "method" in incoming and incoming_id is not None:
                        self._send_message(
                            {
                                "jsonrpc": "2.0",
                                "id": incoming_id,
                                "error": {
                                    "code": -32601,
                                    "message": "Client does not support server-initiated requests.",
                                },
                            }
                        )
                        continue
                    if incoming_id == request_id:
                        result = self._resolve_response(method, incoming)
                        record_mcp_call_latency_ms(
                            duration_ms=(perf_counter() - start) * 1000,
                            method=method,
                            status="ok",
                            tool_name=tool_name,
                        )
                        return result

                    if incoming_id is not None:
                        self._pending_responses[incoming_id] = incoming
                        continue
            except Exception:
                record_mcp_call_latency_ms(
                    duration_ms=(perf_counter() - start) * 1000,
                    method=method,
                    status="error",
                    tool_name=tool_name,
                )
                raise

    def notify(self, method: str, params: dict[str, Any] | None = None) -> None:
        notify_params = params or {}
        tool_name = _tool_name_for_request(method, notify_params)
        start = perf_counter()
        with start_span(
            "mcp.notify",
            {
                "mcp_method": method,
                "tool_name": tool_name or "",
            },
        ):
            try:
                self.start()
                self._send_message(
                    {
                        "jsonrpc": "2.0",
                        "method": method,
                        "params": notify_params,
                    }
                )
                record_mcp_call_latency_ms(
                    duration_ms=(perf_counter() - start) * 1000,
                    method=method,
                    status="ok",
                    tool_name=tool_name,
                )
            except Exception:
                record_mcp_call_latency_ms(
                    duration_ms=(perf_counter() - start) * 1000,
                    method=method,
                    status="error",
                    tool_name=tool_name,
                )
                raise

    def close(self) -> None:
        if self._process is None:
            return

        with self._write_lock:
            process = self._process
            reader_thread = self._reader_thread
            stderr_thread = self._stderr_thread
            if process is None:
                return
            self._process = None
            self._reader_thread = None
            self._stderr_thread = None

            stdin = process.stdin
            if stdin is not None and not stdin.closed:
                stdin.close()

        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=2.0)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=2.0)

        if reader_thread is not None and reader_thread.is_alive():
            reader_thread.join(timeout=2.0)
        if stderr_thread is not None and stderr_thread.is_alive():
            stderr_thread.join(timeout=2.0)

    def _send_message(self, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        with self._write_lock:
            process = self._process
            if process is None or process.stdin is None:
                raise McpSessionError("MCP process is not running.")
            if self.message_mode == "jsonl":
                process.stdin.write(body + b"\n")
            else:
                header = f"Content-Length: {len(body)}\r\n\r\n".encode("ascii")
                process.stdin.write(header)
                process.stdin.write(body)
            process.stdin.flush()

    def _reader_loop(self, stream: Any) -> None:
        while True:
            if self.message_mode == "jsonl":
                message = self._read_jsonl_message(stream)
            else:
                message = self._read_message(stream)
            if message is None:
                self._message_queue.put(_STREAM_CLOSED)
                return
            self._message_queue.put(message)

    def _stderr_loop(self, stream: Any) -> None:
        while True:
            line = stream.readline()
            if not line:
                return
            decoded = line.decode("utf-8", errors="replace").rstrip()
            if decoded:
                self._stderr_lines.append(decoded)
                if len(self._stderr_lines) > 20:
                    self._stderr_lines = self._stderr_lines[-20:]

    def _resolve_response(self, method: str, response: dict[str, Any]) -> Any:
        if "error" in response:
            error = response["error"]
            if isinstance(error, dict):
                message = error.get("message") or json.dumps(error, sort_keys=True)
            else:
                message = str(error)
            raise McpSessionError(f"MCP request `{method}` failed: {message}")
        return response.get("result")

    def _build_process_exit_message(self) -> str:
        if self._process is None:
            return "MCP process exited unexpectedly."

        message = "MCP process exited unexpectedly."
        returncode = self._process.poll()
        if returncode is not None:
            message = f"MCP process exited with return code {returncode}."
        if self._stderr_lines:
            message = f"{message} stderr: {' | '.join(self._stderr_lines[-5:])}"
        return message

    @staticmethod
    def _read_message(stream: Any) -> dict[str, Any] | None:
        headers: dict[str, str] = {}
        while True:
            raw_line = stream.readline()
            if raw_line == b"":
                return None
            if raw_line in {b"\r\n", b"\n"}:
                break
            line = raw_line.decode("ascii", errors="replace").strip()
            if not line:
                break
            key, separator, value = line.partition(":")
            if separator != ":":
                raise McpSessionError(f"Malformed MCP header line: {line!r}")
            headers[key.strip().lower()] = value.strip()

        content_length_raw = headers.get("content-length")
        if content_length_raw is None:
            raise McpSessionError("Missing Content-Length header from MCP server.")

        content_length = int(content_length_raw)
        payload = stream.read(content_length)
        if len(payload) != content_length:
            return None
        parsed = json.loads(payload.decode("utf-8"))
        if not isinstance(parsed, dict):
            raise McpSessionError("MCP server returned a non-object JSON-RPC message.")
        return parsed

    @staticmethod
    def _read_jsonl_message(stream: Any) -> dict[str, Any] | None:
        while True:
            raw_line = stream.readline()
            if raw_line == b"":
                return None
            stripped = raw_line.strip()
            if not stripped:
                continue
            parsed = json.loads(stripped.decode("utf-8"))
            if not isinstance(parsed, dict):
                raise McpSessionError("MCP server returned a non-object JSON-RPC message.")
            return parsed


class McpSession:
    def __init__(
        self,
        *,
        transport: JsonRpcTransport,
        protocol_version: str,
        client_name: str,
        client_version: str,
    ) -> None:
        self.transport = transport
        self.protocol_version = protocol_version
        self.client_name = client_name
        self.client_version = client_version
        self._started = False

    def start(self, *, required_tools: Sequence[str] = ()) -> None:
        if self._started:
            if required_tools:
                self.ensure_tools(required_tools)
            return

        self.transport.start()
        self.transport.request(
            "initialize",
            {
                "protocolVersion": self.protocol_version,
                "capabilities": {},
                "clientInfo": {
                    "name": self.client_name,
                    "version": self.client_version,
                },
            },
        )
        self.transport.notify("notifications/initialized", {})
        self._started = True
        if required_tools:
            self.ensure_tools(required_tools)

    def close(self) -> None:
        self.transport.close()
        self._started = False

    def list_tools(self) -> list[McpToolDefinition]:
        self.start()
        result = self.transport.request("tools/list", {})
        if not isinstance(result, dict):
            raise McpSessionError("Unexpected MCP tools/list result shape.")

        tools = result.get("tools")
        if not isinstance(tools, list):
            raise McpSessionError("Missing `tools` in MCP tools/list result.")

        definitions: list[McpToolDefinition] = []
        for item in tools:
            if not isinstance(item, dict):
                continue
            name = item.get("name")
            if not isinstance(name, str):
                continue
            description = item.get("description")
            definitions.append(
                McpToolDefinition(
                    name=name,
                    description=description if isinstance(description, str) else None,
                )
            )
        return definitions

    def ensure_tools(self, required_tools: Sequence[str]) -> None:
        available = {tool.name for tool in self.list_tools()}
        missing = sorted(set(required_tools) - available)
        if missing:
            raise McpSessionError(
                "MCP server is missing required tools: " + ", ".join(missing)
            )

    def call_tool(self, name: str, arguments: dict[str, Any] | None = None) -> McpToolCallResult:
        self.start()
        result = self.transport.request(
            "tools/call",
            {
                "name": name,
                "arguments": arguments or {},
            },
        )
        if not isinstance(result, dict):
            raise McpSessionError(f"Unexpected result shape from tool `{name}`.")

        structured_content = result.get("structuredContent")
        content = result.get("content")
        tool_result = McpToolCallResult(
            raw_result=result,
            content=content if isinstance(content, list) else [],
            structured_content=structured_content
            if isinstance(structured_content, dict)
            else None,
            is_error=bool(result.get("isError")),
        )
        if tool_result.is_error:
            message = _extract_tool_error_message(result)
            raise McpToolCallError(name, message)
        return tool_result


def parse_command_args(raw_args: str) -> list[str]:
    stripped = raw_args.strip()
    if not stripped:
        return []
    if stripped.startswith("["):
        parsed = json.loads(stripped)
        if not isinstance(parsed, list) or not all(isinstance(item, str) for item in parsed):
            raise ValueError("docs_mcp_args JSON must be a list of strings.")
        return list(parsed)
    return shlex.split(stripped, posix=False)


def _extract_tool_error_message(result: dict[str, Any]) -> str:
    content = result.get("content")
    if isinstance(content, list):
        for item in content:
            if not isinstance(item, dict):
                continue
            text = item.get("text")
            if isinstance(text, str) and text.strip():
                return text.strip()
    structured = result.get("structuredContent")
    if isinstance(structured, dict):
        message = structured.get("message")
        if isinstance(message, str) and message.strip():
            return message.strip()
    return json.dumps(result, sort_keys=True)


def _tool_name_for_request(method: str, params: dict[str, Any]) -> str | None:
    if method != "tools/call":
        return None
    name = params.get("name")
    if isinstance(name, str) and name.strip():
        return name.strip()
    return None
