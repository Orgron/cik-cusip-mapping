"""Integration tests that exercise the end-to-end pipeline helper."""

from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from cik_cusip_mapping import pipeline


def test_skip_index_requires_existing_full_index(tmp_path):
    """Skipping the index step should fail without an existing full index."""

    with pytest.raises(FileNotFoundError):
        pipeline.run_pipeline(output_root=tmp_path, skip_index=True)


def test_skip_parse_requires_existing_csv(tmp_path):
    """Skipping parsing requires CSV outputs to already be present."""

    index_path = tmp_path / "full_index.csv"
    index_path.write_text("cik,comnam,form,date,url\n")

    with pytest.raises(FileNotFoundError):
        pipeline.run_pipeline(
            output_root=tmp_path,
            skip_index=True,
            skip_parse=True,
            skip_download=True,
        )


def test_pipeline_invokes_all_steps(monkeypatch, tmp_path):
    """The pipeline should invoke each stage in order with expected inputs."""

    calls = []

    class DummySession:
        def __init__(self) -> None:
            self.closed = False

        def close(self) -> None:
            self.closed = True

    dummy_session = DummySession()
    monkeypatch.setattr(pipeline, "create_session", lambda: dummy_session)

    def fake_download(
        rps,
        name,
        email,
        *,
        output_path,
        session,
        show_progress=True,
        use_notebook=None,
    ):
        """Record calls to the download helper and create a stub index."""

        calls.append(("download_index", rps, name, email, output_path, session))
        assert session is dummy_session
        output_path.write_text("master")

    def fake_write(
        master_path,
        *,
        output_path,
        show_progress=True,
        use_notebook=None,
    ):
        """Record writes to the full index and produce a minimal CSV."""

        calls.append(("write_index", master_path, output_path))
        output_path.write_text("cik,comnam,form,date,url\n")

    def fake_stream_filings(
        form,
        rps,
        name,
        email,
        *,
        index_path,
        session,
        show_progress,
        progress_desc=None,
        total_hint=None,
        use_notebook=None,
    ):
        """Track streaming invocations and yield a dummy filing."""

        calls.append(
            (
                "stream_filings",
                form,
                rps,
                name,
                email,
                index_path,
                session,
                show_progress,
                progress_desc,
                total_hint,
                use_notebook,
            )
        )
        assert session is dummy_session
        yield SimpleNamespace(identifier="id", content="")

    def fake_stream_events_to_csv(
        filings,
        events_path,
        *,
        debug=False,
        concurrent=True,
        max_queue=32,
        workers=2,
        show_progress=True,
        total_hint=None,
        use_notebook=None,
    ):
        """Capture streamed filings and emit placeholder event outputs."""

        rows = list(filings)
        calls.append(
            (
                "stream_events_to_csv",
                [row.identifier for row in rows],
                Path(events_path),
                debug,
                concurrent,
                max_queue,
                workers,
                show_progress,
                total_hint,
                use_notebook,
            )
        )
        Path(events_path).write_text(
            "cik,form,filing_date,accession_number,company_name,cusip9,cusip8,cusip6,parse_method\n"
            "1,13G,2020-01-01,0001,Example Corp,123456789,12345678,123456,window\n"
        )
        return len(rows)

    def fake_postprocess(events_paths, *, output=None):
        """Record post-processing inputs and write a sample mapping file."""

        calls.append(
            (
                "postprocess",
                [Path(p) for p in events_paths],
                Path(output) if output else None,
            )
        )
        if output:
            Path(output).write_text("cik,cusip6,cusip8\n1,123456,12345678\n")
        columns = ["cik", "cusip6", "cusip8"]
        rows = [{"cik": 1, "cusip6": "123456", "cusip8": "12345678"}]
        return pd.DataFrame(rows, columns=columns)

    def fake_build_dynamics(events_paths, *, output=None):
        """Record dynamics aggregation inputs and emit a sample output."""

        calls.append(("build_dynamics", [Path(p) for p in events_paths], Path(output)))
        if output:
            Path(output).write_text(
                "cik,cusip6,cusip8,first_seen,last_seen,filings_count,forms,months_active,most_recent_accession,most_recent_form,most_recent_filing_date\n"
            )
        return pd.DataFrame(
            [{"cik": 1, "cusip8": "12345678"}], columns=["cik", "cusip8"]
        )

    monkeypatch.setattr(pipeline.indexing, "download_master_index", fake_download)
    monkeypatch.setattr(pipeline.indexing, "write_full_index", fake_write)
    monkeypatch.setattr(pipeline.streaming, "stream_filings", fake_stream_filings)
    monkeypatch.setattr(
        pipeline.parsing, "stream_events_to_csv", fake_stream_events_to_csv
    )
    monkeypatch.setattr(
        pipeline.postprocessing,
        "postprocess_mapping_from_events",
        fake_postprocess,
    )
    monkeypatch.setattr(
        pipeline.postprocessing, "build_cusip_dynamics", fake_build_dynamics
    )

    output_file = tmp_path / "final.csv"
    mapping, dynamics, events_counts = pipeline.run_pipeline(
        forms=["13G"],
        output_root=tmp_path,
        output_file=output_file,
        write_final_mapping=True,
        sec_name="Jane Doe",
        sec_email="jane@example.com",
    )

    assert calls[0][0] == "download_index"
    assert calls[1][0] == "write_index"
    assert calls[2][0] == "stream_filings"
    assert calls[3][0] == "stream_events_to_csv"
    assert calls[3][7] is True
    assert calls[2][7] is False
    assert calls[2][-2] == 0
    assert calls[2][-1] is None
    assert calls[3][-2] == 0
    assert calls[3][-1] is None
    assert calls[4][0] == "postprocess"
    assert calls[5][0] == "build_dynamics"
    assert output_file.exists()
    assert list(mapping.columns) == ["cik", "cusip6", "cusip8"]
    assert dynamics is not None
    assert events_counts["13G"] == 1
    assert dummy_session.closed is True


def test_pipeline_passes_request_metadata(monkeypatch, tmp_path):
    """The pipeline should forward SEC contact metadata to streaming."""

    calls = []

    class DummySession:
        def __init__(self) -> None:
            self.closed = False

        def close(self) -> None:
            self.closed = True

    dummy_session = DummySession()
    monkeypatch.setattr(pipeline, "create_session", lambda: dummy_session)

    def fake_download(
        rps,
        name,
        email,
        *,
        output_path,
        session,
        show_progress=True,
        use_notebook=None,
    ):
        """Write a stub master index file."""

        assert session is dummy_session
        output_path.write_text("master")

    def fake_write(
        master_path,
        *,
        output_path,
        show_progress=True,
        use_notebook=None,
    ):
        """Write a stub full index file."""

        output_path.write_text("cik,comnam,form,date,url\n")

    def fake_stream_filings(
        form,
        rps,
        name,
        email,
        *,
        index_path,
        session,
        show_progress,
        progress_desc=None,
        total_hint=None,
        use_notebook=None,
    ):
        """Capture request metadata passed to the streaming helper."""

        calls.append(
            (
                form,
                rps,
                name,
                email,
                index_path,
                session,
                show_progress,
                progress_desc,
                total_hint,
                use_notebook,
            )
        )
        assert session is dummy_session
        yield SimpleNamespace(identifier="id", content="")

    monkeypatch.setattr(pipeline.indexing, "download_master_index", fake_download)
    monkeypatch.setattr(pipeline.indexing, "write_full_index", fake_write)
    monkeypatch.setattr(pipeline.streaming, "stream_filings", fake_stream_filings)

    def consume_events(
        filings,
        events_path,
        *,
        show_progress=True,
        total_hint=None,
        use_notebook=None,
        **kwargs,
    ):
        """Consume filings generator and create placeholder event outputs."""

        for _ in filings:
            pass
        Path(events_path).write_text(
            "cik,form,filing_date,accession_number,company_name,cusip9,cusip8,cusip6,parse_method\n"
        )
        return 1

    monkeypatch.setattr(pipeline.parsing, "stream_events_to_csv", consume_events)
    monkeypatch.setattr(
        pipeline.postprocessing,
        "postprocess_mapping_from_events",
        lambda events_paths, **kwargs: pd.DataFrame(
            columns=["cik", "cusip6", "cusip8"]
        ),
    )
    monkeypatch.setattr(
        pipeline.postprocessing,
        "build_cusip_dynamics",
        lambda events_paths, **kwargs: pd.DataFrame(columns=["cik"]),
    )

    pipeline.run_pipeline(
        output_root=tmp_path,
        sec_name="Jane Doe",
        sec_email="jane@example.com",
    )

    assert calls[0][:4] == ("13D", 10.0, "Jane Doe", "jane@example.com")
    assert calls[0][5] is dummy_session
    assert calls[0][6] is False


def test_pipeline_uses_environment_metadata(monkeypatch, tmp_path):
    """The pipeline should fall back to SEC metadata provided via environment variables."""

    calls = []

    class DummySession:
        def __init__(self) -> None:
            self.closed = False

        def close(self) -> None:
            self.closed = True

    dummy_session = DummySession()
    monkeypatch.setattr(pipeline, "create_session", lambda: dummy_session)

    monkeypatch.setenv("SEC_NAME", "Env Contact")
    monkeypatch.setenv("SEC_EMAIL", "env@example.com")

    def fake_download(
        rps,
        name,
        email,
        *,
        output_path,
        session,
        show_progress=True,
        use_notebook=None,
    ):
        calls.append(("download", name, email))
        assert session is dummy_session
        output_path.write_text("master")

    def fake_write(
        master_path,
        *,
        output_path,
        show_progress=True,
        use_notebook=None,
    ):
        output_path.write_text("cik,comnam,form,date,url\n")

    def fake_stream_filings(
        form,
        rps,
        name,
        email,
        *,
        index_path,
        session,
        show_progress,
        progress_desc=None,
        total_hint=None,
        use_notebook=None,
    ):
        calls.append(("stream", name, email))
        assert session is dummy_session
        yield SimpleNamespace(identifier="id", content="")

    def consume_events(
        filings,
        events_path,
        *,
        show_progress=True,
        total_hint=None,
        use_notebook=None,
        **kwargs,
    ):
        for _ in filings:
            pass
        Path(events_path).write_text(
            "cik,form,filing_date,accession_number,company_name,cusip9,cusip8,cusip6,parse_method\n"
        )
        return 1

    monkeypatch.setattr(pipeline.indexing, "download_master_index", fake_download)
    monkeypatch.setattr(pipeline.indexing, "write_full_index", fake_write)
    monkeypatch.setattr(pipeline.streaming, "stream_filings", fake_stream_filings)
    monkeypatch.setattr(pipeline.parsing, "stream_events_to_csv", consume_events)
    monkeypatch.setattr(
        pipeline.postprocessing,
        "postprocess_mapping_from_events",
        lambda events_paths, **kwargs: pd.DataFrame(columns=["cik", "cusip6", "cusip8"]),
    )
    monkeypatch.setattr(
        pipeline.postprocessing,
        "build_cusip_dynamics",
        lambda events_paths, **kwargs: pd.DataFrame(columns=["cik"]),
    )

    pipeline.run_pipeline(output_root=tmp_path)

    assert ("download", "Env Contact", "env@example.com") in calls
    assert ("stream", "Env Contact", "env@example.com") in calls
