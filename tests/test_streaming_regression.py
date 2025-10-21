import csv
from types import SimpleNamespace

import parse_cusip


def test_streaming_matches_legacy(tmp_path):
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

    legacy_result = parse_cusip.parse_file(legacy_path)

    csv_path = tmp_path / "stream.csv"
    filing = SimpleNamespace(identifier="sample", content=text)
    parse_cusip.stream_to_csv([filing], csv_path)

    with csv_path.open(newline="") as handle:
        reader = csv.reader(handle)
        row = next(reader)

    assert row[1] == legacy_result.cik
    assert row[2] == legacy_result.cusip
