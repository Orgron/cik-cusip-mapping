"""Tests ensuring streaming session lifecycle is handled correctly."""

from types import SimpleNamespace

from cik_cusip_mapping import streaming


def test_stream_filings_closes_internal_session(monkeypatch):
    """stream_filings should close any session it creates internally."""

    created_sessions = []

    class DummySession:
        """Minimal Session implementation that records close calls."""

        def __init__(self):
            self.closed = False
            created_sessions.append(self)

        def close(self):
            """Mark the session as closed for later assertions."""

            self.closed = True

    monkeypatch.setattr(streaming.requests, "Session", DummySession)
    monkeypatch.setattr(
        streaming, "RateLimiter", lambda *_args, **_kwargs: SimpleNamespace(wait=lambda: None)
    )
    monkeypatch.setattr(streaming, "build_request_headers", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(streaming, "_iter_index_rows", lambda *_args, **_kwargs: [])

    list(
        streaming.stream_filings(
            "13D",
            1.0,
            name=None,
            email=None,
            index_path="ignored.csv",
            show_progress=False,
        )
    )

    assert len(created_sessions) == 1
    assert created_sessions[0].closed is True
