"""Tests for event emission while streaming parsed filings."""

from __future__ import annotations

import csv
from types import SimpleNamespace

import pytest

from cik_cusip_mapping import parsing


CUSIP_CONTENT = """SUBJECT COMPANY\nCENTRAL INDEX KEY\t\t\t0000123456\n<DOCUMENT>\nCUSIP\n123456789\n"""


@pytest.mark.parametrize("concurrent", [False, True])
def test_stream_to_csv_writes_events(tmp_path, concurrent):
    """stream_to_csv should populate both the mapping and event outputs."""

    filings = [
        SimpleNamespace(
            identifier="file1",
            content=CUSIP_CONTENT,
            form="13D",
            date="2020-01-01",
            accession_number="0001-0000000000",
            url="edgar/data/0001.txt",
            company_name="Example Corp",
        )
    ]

    csv_path = tmp_path / "output.csv"
    events_path = tmp_path / "events.csv"

    parsing.stream_to_csv(
        filings,
        csv_path,
        concurrent=concurrent,
        events_csv_path=events_path,
    )

    with csv_path.open() as handle:
        rows = list(csv.reader(handle))
    assert rows[0][0] == "file1"
    assert rows[0][2] == "123456789"

    with events_path.open() as handle:
        reader = csv.DictReader(handle)
        event_rows = list(reader)

    assert len(event_rows) == 1
    row = event_rows[0]
    assert row["identifier"] == "file1"
    assert row["cik"] == "0000123456"
    assert row["form"] == "13D"
    assert row["filing_date"] == "2020-01-01"
    assert row["accession_number"] == "0001-0000000000"
    assert row["company_name"] == "Example Corp"
    assert row["cusip9"] == "123456789"
    assert row["cusip8"] == "12345678"
    assert row["cusip6"] == "123456"
    assert row["parse_method"] == "window"
