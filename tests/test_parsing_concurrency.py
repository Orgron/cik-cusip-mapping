import threading
from types import SimpleNamespace

from cik_cusip_mapping import parsing


def test_parse_filings_concurrently_uses_worker_thread(monkeypatch):
    seen_threads: list[int] = []
    main_thread = threading.get_ident()

    def fake_parse_text(text: str, *, debug: bool = False):
        seen_threads.append(threading.get_ident())
        return "0000123456", "123456789"

    monkeypatch.setattr(parsing, "parse_text", fake_parse_text)

    filings = [SimpleNamespace(identifier=str(idx), content="") for idx in range(3)]

    results = list(
        parsing.parse_filings_concurrently(
            filings,
            max_queue=2,
            workers=2,
        )
    )

    assert len(results) == len(filings)
    assert any(thread_id != main_thread for thread_id in seen_threads)
