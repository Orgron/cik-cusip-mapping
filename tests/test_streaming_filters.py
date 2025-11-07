"""Tests covering the new filing filters for streaming and counting."""

from __future__ import annotations

import csv
from pathlib import Path

import pytest

from cik_cusip_mapping import pipeline, streaming


@pytest.fixture()
def sample_index(tmp_path: Path) -> Path:
    """Create a tiny EDGAR index CSV for filtering tests."""

    path = tmp_path / "index.csv"
    rows = [
        {"cik": "00001", "comnam": "Alpha", "form": "SC 13G", "date": "2023-12-31", "url": "a"},
        {"cik": "00002", "comnam": "Beta", "form": "SC 13G", "date": "2024-01-15", "url": "b"},
        {"cik": "00002", "comnam": "Beta", "form": "SC 13GA", "date": "2024-01-20", "url": "c"},
        {"cik": "00003", "comnam": "Gamma", "form": "SC 13GA", "date": "2024-01-25", "url": "d"},
        {"cik": "00004", "comnam": "Delta", "form": "SC 13DA", "date": "2024-02-10", "url": "e"},
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["cik", "comnam", "form", "date", "url"])
        writer.writeheader()
        writer.writerows(rows)
    return path


def test_iter_index_rows_respects_filters(sample_index: Path) -> None:
    """_iter_index_rows should apply date, CIK, and amendment filters."""

    rows = list(
        streaming._iter_index_rows(
            "13G",
            sample_index,
            start_date="2024-01-01",
            end_date="2024-01-31",
            cik_whitelist=[2],
            amended_only=True,
        )
    )
    assert [row["cik"] for row in rows] == ["00002"]


def test_count_index_rows_multi_respects_filters(sample_index: Path) -> None:
    """count_index_rows_multi should mirror streaming filters."""

    counts = pipeline.count_index_rows_multi(
        ["13G", "13D"],
        sample_index,
        start_date="2024-01-01",
        end_date="2024-01-31",
        cik_whitelist=["00002", "00004"],
        amended_only=True,
    )
    assert counts["13G"] == 1
    assert counts["13D"] == 0


def test_count_index_rows_rejects_invalid_range(sample_index: Path) -> None:
    """An inverted date range should raise an informative error."""

    with pytest.raises(ValueError):
        pipeline.count_index_rows(
            "13G",
            sample_index,
            start_date="2024-02-01",
            end_date="2024-01-01",
        )
