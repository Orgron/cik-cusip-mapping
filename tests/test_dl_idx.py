import csv
from pathlib import Path

from cik_cusip_mapping import indexing


def test_write_full_index_generates_rows_without_blanks(monkeypatch, tmp_path):
    data_dir = Path(__file__).resolve().parent / "data"
    master_idx = data_dir / "master_sample.idx"
    contents = master_idx.read_text()

    monkeypatch.chdir(tmp_path)
    tmp_master = tmp_path / "master.idx"
    tmp_master.write_text(contents)

    indexing.write_full_index(master_path=tmp_master)

    assert not tmp_master.exists()

    full_index = tmp_path / "full_index.csv"
    assert full_index.exists()

    with full_index.open(newline="") as csvfile:
        rows = list(csv.reader(csvfile))

    expected_entries = sum(1 for line in contents.splitlines() if ".txt" in line)
    assert len(rows) == expected_entries + 1
    assert rows[0] == ["cik", "comnam", "form", "date", "url"]

    for row in rows[1:]:
        assert any(cell.strip() for cell in row), "Blank row detected in full_index.csv"
