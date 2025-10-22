"""Tests covering progress bar integration for indexing helpers."""

from types import SimpleNamespace

from cik_cusip_mapping import indexing


def test_download_master_index_uses_progress_factory(monkeypatch, tmp_path):
    """download_master_index should honour progress configuration flags."""

    quarters = [(2020, 1), (2020, 2), (2020, 3)]
    monkeypatch.setattr(indexing, "_iter_quarters", lambda *args, **kwargs: quarters)
    monkeypatch.setattr(
        indexing,
        "RateLimiter",
        lambda *_args, **_kwargs: SimpleNamespace(wait=lambda: None),
    )
    monkeypatch.setattr(indexing, "build_request_headers", lambda *_args, **_kwargs: {})

    class DummyResponse:
        content = b"data"

        def raise_for_status(self) -> None:  # pragma: no cover - defensive
            return None

    class DummySession:
        def __init__(self) -> None:
            self.closed = False

        def get(self, *_args, **_kwargs):
            return DummyResponse()

        def close(self) -> None:
            self.closed = True

    dummy_session = DummySession()
    monkeypatch.setattr(indexing, "create_session", lambda: dummy_session)

    resolve_calls: list[bool | None] = []
    progress_kwargs: list[dict[str, object]] = []
    updates: list[int] = []
    closed: list[bool] = []

    def fake_resolve(use_notebook):
        resolve_calls.append(use_notebook)

        class DummyProgress:
            def update(self, amount: int) -> None:
                updates.append(amount)

            def close(self) -> None:
                closed.append(True)

        def factory(**kwargs):
            progress_kwargs.append(kwargs)
            return DummyProgress()

        return factory

    monkeypatch.setattr(indexing, "resolve_tqdm", fake_resolve)

    output_path = tmp_path / "master.idx"
    indexing.download_master_index(
        1.0,
        name=None,
        email=None,
        start_year=2020,
        end_year=2020,
        output_path=output_path,
        show_progress=True,
        use_notebook=True,
    )

    assert resolve_calls == [True]
    assert progress_kwargs
    assert progress_kwargs[0]["total"] == len(quarters)
    assert sum(updates) == len(quarters)
    assert closed == [True]


def test_write_full_index_uses_progress_factory(monkeypatch, tmp_path):
    """write_full_index should use the shared tqdm resolver."""

    master_path = tmp_path / "master.idx"
    master_path.write_text(
        "header\n"
        "1|Example|13D|2020-01-01|edgar/data/1/0001.txt\n"
        "skip|line|without|txt\n"
        "2|Example|13G|2020-02-01|edgar/data/2/0002.txt\n"
    )

    resolve_calls: list[bool | None] = []
    progress_kwargs: list[dict[str, object]] = []
    updates: list[int] = []
    closed: list[bool] = []

    def fake_resolve(use_notebook):
        resolve_calls.append(use_notebook)

        class DummyProgress:
            def update(self, amount: int) -> None:
                updates.append(amount)

            def close(self) -> None:
                closed.append(True)

        def factory(**kwargs):
            progress_kwargs.append(kwargs)
            return DummyProgress()

        return factory

    monkeypatch.setattr(indexing, "resolve_tqdm", fake_resolve)

    output_path = tmp_path / "full_index.csv"
    indexing.write_full_index(
        master_path=master_path,
        output_path=output_path,
        show_progress=True,
        use_notebook=False,
    )

    assert resolve_calls == [False]
    assert progress_kwargs
    assert progress_kwargs[0]["total"] == 2
    assert sum(updates) == 2
    assert closed == [True]
