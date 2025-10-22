"""Tests for event emission while streaming parsed filings."""

from __future__ import annotations

import csv
from types import SimpleNamespace

import pytest

from cik_cusip_mapping import parsing


CUSIP_CONTENT = """SUBJECT COMPANY\nCENTRAL INDEX KEY\t\t\t0000123456\n<DOCUMENT>\nCUSIP\n123456789\n"""


@pytest.mark.parametrize("concurrent", [False, True])
def test_stream_events_to_csv_writes_events(tmp_path, concurrent):
    """stream_events_to_csv should populate the event outputs."""

    filings = [
        SimpleNamespace(
            identifier="file1",
            content=CUSIP_CONTENT,
            form="13D",
            date="2020-01-01",
            accession_number="0001-0000000000",
            company_name="Example Corp",
        )
    ]

    events_path = tmp_path / "events.csv"

    parsing.stream_events_to_csv(
        filings,
        concurrent=concurrent,
        show_progress=False,
        events_csv_path=events_path,
    )

    with events_path.open(encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        event_rows = list(reader)

    assert len(event_rows) == 1
    row = event_rows[0]
    assert row["cik"] == "0000123456"
    assert row["form"] == "13D"
    assert row["filing_date"] == "2020-01-01"
    assert row["accession_number"] == "0001-0000000000"
    assert row["company_name"] == "Example Corp"
    assert row["cusip9"] == "123456789"
    assert row["cusip8"] == "12345678"
    assert row["cusip6"] == "123456"
    assert row["parse_method"] == "window"
