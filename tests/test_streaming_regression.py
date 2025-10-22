"""Regression tests validating streaming output against legacy parsing."""

import csv
from types import SimpleNamespace

from cik_cusip_mapping import parsing


def test_streaming_matches_legacy(tmp_path):
    """stream_events_to_csv should match the output of the single-file parser."""

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

    events_path = tmp_path / "stream_events.csv"
    filing = SimpleNamespace(identifier="sample", content=text)
    parsing.stream_events_to_csv([filing], events_path, show_progress=False)

    with events_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        row = next(reader)

    assert row["cik"] == legacy_result.cik
    assert row["cusip9"] == legacy_result.cusip
