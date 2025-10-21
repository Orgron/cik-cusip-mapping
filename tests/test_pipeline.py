from pathlib import Path
from types import SimpleNamespace

import pytest

from cik_cusip_mapping import pipeline


class DummyFrame:
    def __init__(self, columns, rows):
        self.columns = columns
        self._rows = rows

    def __len__(self) -> int:  # pragma: no cover - trivial
        return len(self._rows)


def test_skip_index_requires_existing_full_index(tmp_path):
    with pytest.raises(FileNotFoundError):
        pipeline.run_pipeline(output_root=tmp_path, skip_index=True)


def test_skip_parse_requires_existing_csv(tmp_path):
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
    calls = []

    def fake_download(rps, name, email, *, output_path):
        calls.append(("download_index", rps, name, email, output_path))
        output_path.write_text("master")

    def fake_write(master_path, *, output_path):
        calls.append(("write_index", master_path, output_path))
        output_path.write_text("cik,comnam,form,date,url\n")

    def fake_stream_filings(form, rps, name, email, *, index_path):
        calls.append(("stream_filings", form, rps, name, email, index_path))
        yield SimpleNamespace(identifier="id", content="")

    def fake_stream_to_csv(
        filings,
        csv_path,
        *,
        debug=False,
        concurrent=True,
        max_queue=32,
        workers=2,
    ):
        rows = list(filings)
        calls.append(("stream_to_csv", [row.identifier for row in rows], Path(csv_path), debug, concurrent))
        Path(csv_path).write_text("id,cik,cusip\n")
        return len(rows)

    def fake_postprocess(csv_paths, *, output=None):
        calls.append(("postprocess", [Path(p) for p in csv_paths], Path(output) if output else None))
        if output:
            Path(output).write_text("cik,cusip6,cusip8\n1,123456,12345678\n")
        return DummyFrame(["cik", "cusip6", "cusip8"], rows=[{"cik": 1}])

    monkeypatch.setattr(pipeline.indexing, "download_master_index", fake_download)
    monkeypatch.setattr(pipeline.indexing, "write_full_index", fake_write)
    monkeypatch.setattr(pipeline.streaming, "stream_filings", fake_stream_filings)
    monkeypatch.setattr(pipeline.parsing, "stream_to_csv", fake_stream_to_csv)
    monkeypatch.setattr(pipeline.postprocessing, "postprocess_mappings", fake_postprocess)

    output_file = tmp_path / "final.csv"
    result = pipeline.run_pipeline(
        forms=["13G"],
        output_root=tmp_path,
        output_file=output_file,
        sec_name="Jane Doe",
        sec_email="jane@example.com",
    )

    assert calls[0][0] == "download_index"
    assert calls[1][0] == "write_index"
    assert calls[2][0] == "stream_filings"
    assert calls[3][0] == "stream_to_csv"
    assert calls[4][0] == "postprocess"
    assert output_file.exists()
    assert list(result.columns) == ["cik", "cusip6", "cusip8"]


def test_pipeline_passes_request_metadata(monkeypatch, tmp_path):
    calls = []

    def fake_download(rps, name, email, *, output_path):
        output_path.write_text("master")

    def fake_write(master_path, *, output_path):
        output_path.write_text("cik,comnam,form,date,url\n")

    def fake_stream_filings(form, rps, name, email, *, index_path):
        calls.append((form, rps, name, email, index_path))
        yield SimpleNamespace(identifier="id", content="")

    monkeypatch.setattr(pipeline.indexing, "download_master_index", fake_download)
    monkeypatch.setattr(pipeline.indexing, "write_full_index", fake_write)
    monkeypatch.setattr(pipeline.streaming, "stream_filings", fake_stream_filings)
    def consume_to_csv(filings, csv_path, **kwargs):
        for _ in filings:
            pass
        Path(csv_path).write_text("id,cik,cusip\n")
        return 1

    monkeypatch.setattr(pipeline.parsing, "stream_to_csv", consume_to_csv)
    monkeypatch.setattr(
        pipeline.postprocessing,
        "postprocess_mappings",
        lambda csv_paths, **kwargs: DummyFrame(["cik", "cusip6", "cusip8"], rows=[]),
    )

    pipeline.run_pipeline(
        output_root=tmp_path,
        sec_name="Jane Doe",
        sec_email="jane@example.com",
    )

    assert calls[0][:4] == ("13D", 10.0, "Jane Doe", "jane@example.com")
