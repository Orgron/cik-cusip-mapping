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

    monkeypatch.setattr(streaming, "create_session", lambda: DummySession())
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


def test_stream_filings_uses_total_hint(monkeypatch):
    """stream_filings should forward total hints to the progress factory."""

    rows = [
        {
            "cik": "1",
            "comnam": "Example",
            "form": "13D",
            "date": "2020-01-01",
            "url": "edgar/data/1/0001-0000000000.txt",
        }
    ]
    monkeypatch.setattr(streaming, "_iter_index_rows", lambda *_args, **_kwargs: iter(rows))
    monkeypatch.setattr(
        streaming,
        "RateLimiter",
        lambda *_args, **_kwargs: SimpleNamespace(wait=lambda: None),
    )
    monkeypatch.setattr(streaming, "build_request_headers", lambda *_args, **_kwargs: {})

    class DummyResponse:
        text = "content"

        def raise_for_status(self) -> None:  # pragma: no cover - defensive
            return None

    class DummySession:
        def __init__(self) -> None:
            self.closed = False

        def get(self, *_args, **_kwargs) -> DummyResponse:
            return DummyResponse()

        def close(self) -> None:
            self.closed = True

    dummy_session = DummySession()
    progress_calls: list[dict[str, object]] = []

    def fake_resolve(use_notebook):
        def factory(**kwargs):
            progress_calls.append(kwargs)
            return SimpleNamespace(update=lambda *_args, **_kwargs: None, close=lambda: None)

        return factory

    monkeypatch.setattr(streaming, "resolve_tqdm", fake_resolve)

    list(
        streaming.stream_filings(
            "13D",
            1.0,
            name=None,
            email=None,
            index_path="ignored.csv",
            session=dummy_session,
            total_hint=5,
        )
    )

    assert progress_calls
    assert progress_calls[0]["total"] == 5
