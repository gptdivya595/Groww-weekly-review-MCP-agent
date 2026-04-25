from __future__ import annotations

import sys
import threading
import time

from agent.mcp_client.session import _STREAM_CLOSED, StdioJsonRpcTransport


def test_close_does_not_raise_thread_exceptions(monkeypatch) -> None:
    thread_errors: list[type[BaseException]] = []

    def _capture_thread_error(args: threading.ExceptHookArgs) -> None:
        thread_errors.append(args.exc_type)

    monkeypatch.setattr(threading, "excepthook", _capture_thread_error)

    script = (
        "import sys, time; "
        "sys.stderr.write('boot\\n'); "
        "sys.stderr.flush(); "
        "time.sleep(1.0)"
    )
    transport = StdioJsonRpcTransport(
        command=sys.executable,
        args=["-c", script],
        timeout_seconds=1.0,
    )

    transport.start()
    deadline = time.time() + 2.0
    while not transport._stderr_lines and time.time() < deadline:
        time.sleep(0.01)

    transport.close()
    time.sleep(0.1)

    assert transport._stderr_lines == ["boot"]
    assert thread_errors == []


def test_start_resets_transient_transport_state() -> None:
    transport = StdioJsonRpcTransport(
        command=sys.executable,
        args=["-c", "import time; time.sleep(0.2)"],
        timeout_seconds=1.0,
    )
    transport._message_queue.put(_STREAM_CLOSED)
    transport._pending_responses[7] = {"id": 7, "result": {"ok": True}}
    transport._stderr_lines = ["stale"]

    transport.start()
    try:
        assert transport._message_queue.empty()
        assert transport._pending_responses == {}
        assert transport._stderr_lines == []
    finally:
        transport.close()


def test_request_supports_jsonl_framing() -> None:
    script = """
import json, sys
line = sys.stdin.readline()
payload = json.loads(line)
response = {
    'jsonrpc': '2.0',
    'id': payload['id'],
    'result': {
        'protocolVersion': payload['params']['protocolVersion'],
        'capabilities': {'tools': {}},
        'serverInfo': {'name': 'jsonl-server', 'version': '1.0.0'},
    },
}
sys.stdout.write(json.dumps(response) + '\\n')
sys.stdout.flush()
"""
    transport = StdioJsonRpcTransport(
        command=sys.executable,
        args=["-c", script],
        timeout_seconds=2.0,
        message_mode="jsonl",
    )

    result = transport.request(
        "initialize",
        {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "test-client", "version": "0.1.0"},
        },
    )

    assert result == {
        "protocolVersion": "2024-11-05",
        "capabilities": {"tools": {}},
        "serverInfo": {"name": "jsonl-server", "version": "1.0.0"},
    }
    transport.close()
