import sys
from pathlib import Path
from types import SimpleNamespace
import types

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:  # pragma: no cover - exercised when requests is unavailable
    import requests  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - fallback for test environment
    class _DummySession:
        def get(self, *args, **kwargs):  # noqa: D401
            """Placeholder method to satisfy dl.stream_filings in tests."""

            raise RuntimeError("requests session stub invoked; tests should monkeypatch dl.stream_filings")

    requests = types.SimpleNamespace(Session=_DummySession)
    sys.modules["requests"] = requests

import run_pipeline


@pytest.fixture(autouse=True)
def clean_generated_mapping():
    mapping_path = Path("cik-cusip-maps.csv")
    index_path = Path("full_index.csv")
    if mapping_path.exists():
        mapping_path.unlink()
    if index_path.exists():
        index_path.unlink()
    yield
    if mapping_path.exists():
        mapping_path.unlink()
    if index_path.exists():
        index_path.unlink()


def _touch_full_index():
    Path("full_index.csv").write_text("cik,comnam,form,date,url\n")


def test_skip_index_requires_existing_full_index(tmp_path):
    with pytest.raises(FileNotFoundError):
        run_pipeline.main(["--skip-index"])


def test_skip_parse_requires_existing_csv(monkeypatch, tmp_path):
    _touch_full_index()
    monkeypatch.setattr(run_pipeline, "_download_master_index", lambda *args, **kwargs: None)
    with pytest.raises(FileNotFoundError):
        run_pipeline.main(["--skip-parse", "--skip-index", "--output-root", str(tmp_path)])


def test_pipeline_invokes_all_steps(monkeypatch, tmp_path):
    calls = []

    def fake_download(rps, name, email):
        calls.append(("download_index", rps, name, email))
        _touch_full_index()

    def fake_stream_filings(form, rps, name, email):
        calls.append(("stream_filings", form, rps, name, email))
        return iter([SimpleNamespace(identifier="id", content="")])

    def fake_stream_to_csv(filings, csv_path, *, debug=False):
        rows = list(filings)
        calls.append(("stream_to_csv", [row.identifier for row in rows], csv_path, debug))
        Path(csv_path).write_text("id,cik,cusip\n")

    def fake_post_proc(command, check):
        calls.append(("post_proc", command, check))
        Path("cik-cusip-maps.csv").write_text("CIK,CUSIP\n")

    monkeypatch.setattr(run_pipeline, "_download_master_index", fake_download)
    monkeypatch.setattr(run_pipeline.dl, "stream_filings", fake_stream_filings)
    monkeypatch.setattr(run_pipeline.parse_cusip, "stream_to_csv", fake_stream_to_csv)
    monkeypatch.setattr(run_pipeline.subprocess, "run", fake_post_proc)

    output_file = tmp_path / "final.csv"
    run_pipeline.main(
        [
            "--forms",
            "13G",
            "--output-root",
            str(tmp_path),
            "--output-file",
            str(output_file),
        ]
    )

    assert calls[0] == ("download_index", 10.0, None, None)
    assert calls[1][:2] == ("stream_filings", "13G")
    assert calls[2][0] == "stream_to_csv"
    assert calls[3][0] == "post_proc"
    assert output_file.exists()
    assert not Path("cik-cusip-maps.csv").exists()


def test_pipeline_passes_identifiers(monkeypatch, tmp_path):
    calls = []

    def fake_download(*args, **kwargs):
        _touch_full_index()

    def fake_stream_filings(form, rps, name, email):
        calls.append((form, rps, name, email))
        return iter([SimpleNamespace(identifier="id", content="")])

    monkeypatch.setattr(run_pipeline, "_download_master_index", fake_download)
    monkeypatch.setattr(run_pipeline.dl, "stream_filings", fake_stream_filings)
    monkeypatch.setattr(
        run_pipeline.parse_cusip,
        "stream_to_csv",
        lambda filings, csv_path, *, debug=False: Path(csv_path).write_text(""),
    )
    monkeypatch.setattr(
        run_pipeline.subprocess,
        "run",
        lambda command, check: Path("cik-cusip-maps.csv").write_text("CIK,CUSIP\n"),
    )

    run_pipeline.main(
        [
            "--sec-name",
            "Jane Doe",
            "--sec-email",
            "jane@example.com",
        ]
    )

    assert calls[0] == ("13D", 10.0, "Jane Doe", "jane@example.com")
