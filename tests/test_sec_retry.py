"""Tests for the SEC session helper and retry configuration."""

from __future__ import annotations

import threading
from collections import deque
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Deque, Iterable, Tuple

import pytest

from cik_cusip_mapping import sec


class _SequentialHandler(BaseHTTPRequestHandler):
    """HTTP handler that serves a predefined sequence of responses."""

    responses: Deque[Tuple[int, dict[str, str], bytes]] = deque()
    lock = threading.Lock()
    request_count = 0

    def do_GET(self) -> None:  # pragma: no cover - exercised indirectly
        """Serve the next response in the queue and record call counts."""

        with self.lock:
            if not self.responses:
                status, headers, body = 500, {}, b""
            else:
                status, headers, body = self.responses.popleft()
            type(self).request_count += 1
        self.send_response(status)
        for key, value in headers.items():
            self.send_header(key, value)
        self.end_headers()
        if body:
            self.wfile.write(body)

    def log_message(self, *_args, **_kwargs):  # pragma: no cover - silence server logs
        """Suppress default logging for cleaner test output."""

        return None


@contextmanager
def _serve_responses(responses: Iterable[Tuple[int, dict[str, str], bytes]]):
    """Spin up a local HTTP server that serves the provided responses."""

    _SequentialHandler.responses = deque(responses)
    _SequentialHandler.request_count = 0
    server = HTTPServer(("127.0.0.1", 0), _SequentialHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        host, port = server.server_address
        yield f"http://{host}:{port}", _SequentialHandler
    finally:
        server.shutdown()
        thread.join()


@pytest.mark.parametrize("status_code", [429, 500])
def test_create_session_retries_on_error(status_code):
    """create_session should retry requests on retryable status codes."""

    responses = [
        (status_code, {"Content-Length": "0"}, b""),
        (200, {"Content-Type": "text/plain"}, b"ok"),
    ]
    with _serve_responses(responses) as (url, handler):
        session = sec.create_session(backoff_factor=0)
        try:
            response = session.get(url, timeout=5)
        finally:
            session.close()
    assert response.status_code == 200
    assert handler.request_count == 2
