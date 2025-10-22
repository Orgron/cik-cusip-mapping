"""Tests for the CUSIP dynamics aggregation helper."""

from __future__ import annotations

import csv

from cik_cusip_mapping import postprocessing


def test_build_cusip_dynamics(tmp_path):
    """build_cusip_dynamics should aggregate filings into summary metrics."""

    events_path = tmp_path / "13D_events.csv"
    with events_path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "identifier",
                "cik",
                "form",
                "filing_date",
                "accession_number",
                "url",
                "company_name",
                "cusip9",
                "cusip8",
                "cusip6",
                "parse_method",
            ]
        )
        writer.writerow(
            [
                "file1",
                "1000",
                "13D",
                "2020-01-15",
                "0001",
                "url1",
                "Example Corp",
                "123456789",
                "12345678",
                "123456",
                "window",
            ]
        )
        writer.writerow(
            [
                "file2",
                "1000",
                "13G",
                "2020-02-20",
                "0002",
                "url2",
                "Example Corp",
                "123456789",
                "12345678",
                "123456",
                "fallback",
            ]
        )

    output_path = tmp_path / "dynamics.csv"
    result = postprocessing.build_cusip_dynamics([events_path], output=output_path)

    assert len(result) == 1
    record = result.iloc[0]
    assert record.cik == 1000
    assert record.cusip8 == "12345678"
    assert record.first_seen == "2020-01-15"
    assert record.last_seen == "2020-02-20"
    assert record.filings_count == 2
    assert record.forms == "13D;13G"
    assert record.months_active == 2
    assert record.most_recent_accession == "0002"
    assert record.most_recent_form == "13G"
    assert record.most_recent_filing_date == "2020-02-20"
    assert output_path.exists()
