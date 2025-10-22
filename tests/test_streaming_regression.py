"""Regression tests validating streaming output against legacy parsing."""

import csv
from types import SimpleNamespace

from cik_cusip_mapping import parsing


def test_streaming_matches_legacy(tmp_path):
    """stream_to_csv should match the output of the single-file parser."""

    text = (
        "SCHEDULE 13D\n"
        "SUBJECT COMPANY\n"
        "CENTRAL INDEX KEY:\t\t\t0000123456\n"
        "<DOCUMENT>\n"
        "CUSIP NO. 123456789\n"
    )

    legacy_dir = tmp_path / "legacy"
    legacy_dir.mkdir()
    legacy_path = legacy_dir / "sample.txt"
    legacy_path.write_text(text)

    legacy_result = parsing.parse_file(legacy_path)

    csv_path = tmp_path / "stream.csv"
    filing = SimpleNamespace(identifier="sample", content=text)
    parsing.stream_to_csv([filing], csv_path, show_progress=False)

    with csv_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        row = next(reader)

    assert row[1] == legacy_result.cik
    assert row[2] == legacy_result.cusip
