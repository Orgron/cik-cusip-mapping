import csv
import sys
import types
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

if "requests" not in sys.modules:
    sys.modules["requests"] = types.ModuleType("requests")

import dl_idx


def test_write_full_index_generates_rows_without_blanks(monkeypatch, tmp_path):
    data_dir = Path(__file__).resolve().parent / "data"
    master_idx = data_dir / "master_sample.idx"
    contents = master_idx.read_text()

    monkeypatch.chdir(tmp_path)
    tmp_master = tmp_path / "master.idx"
    tmp_master.write_text(contents)

    dl_idx.write_full_index()

    full_index = tmp_path / "full_index.csv"
    assert full_index.exists()

    with full_index.open(newline="") as csvfile:
        rows = list(csv.reader(csvfile))

    expected_entries = sum(1 for line in contents.splitlines() if ".txt" in line)
    assert len(rows) == expected_entries + 1
    assert rows[0] == ["cik", "comnam", "form", "date", "url"]

    for row in rows[1:]:
        assert any(cell.strip() for cell in row), "Blank row detected in full_index.csv"
