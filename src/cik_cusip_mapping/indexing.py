"""Utilities for downloading the SEC master index and building CSV snapshots."""

from __future__ import annotations

import datetime as _dt
import logging
import csv
from pathlib import Path
from typing import Iterable

import requests

from .sec import RateLimiter, build_request_headers, create_session
from .progress import resolve_tqdm

logger = logging.getLogger(__name__)


def _iter_quarters(
    start_year: int, end_year: int, end_quarter: int
) -> Iterable[tuple[int, int]]:
    """Yield ``(year, quarter)`` tuples across the requested range."""

    for year in range(start_year, end_year + 1):
        last_quarter = end_quarter if year == end_year else 4
        for quarter in range(1, last_quarter + 1):
            yield year, quarter


def download_master_index(
    requests_per_second: float,
    name: str | None,
    email: str | None,
    *,
    start_year: int = 1994,
    end_year: int | None = None,
    output_path: Path | str = Path("master.idx"),
    session: requests.Session | None = None,
    show_progress: bool = True,
    use_notebook: bool | None = None,
) -> Path:
    """Download the SEC master index covering the requested period."""

    headers = build_request_headers(name, email)
    limiter = RateLimiter(requests_per_second)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    today = _dt.date.today()
    end_year = end_year or today.year
    current_quarter = (today.month - 1) // 3 + 1
    end_quarter = current_quarter if end_year >= today.year else 4

    quarters = list(_iter_quarters(start_year, end_year, end_quarter))
    created_session = session is None
    http = session or create_session()

    progress = (
        resolve_tqdm(use_notebook)(
            total=len(quarters),
            desc="Downloading master index",
            unit="quarter",
            dynamic_ncols=True,
            mininterval=0.1,
            leave=False,
        )
        if show_progress
        else None
    )

    try:
        with output_path.open("wb") as handle:
            for year, quarter in quarters:
                logger.info("Downloading master index for %s Q%s", year, quarter)
                limiter.wait()
                response = http.get(
                    f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{quarter}/master.idx",
                    headers=headers,
                    timeout=60,
                )
                response.raise_for_status()
                handle.write(response.content)
                if progress is not None:
                    progress.update(1)
    finally:
        if progress is not None:
            progress.close()
        if created_session:
            http.close()

    return output_path


def write_full_index(
    master_path: Path | str = Path("master.idx"),
    *,
    output_path: Path | str = Path("full_index.csv"),
    show_progress: bool = True,
    use_notebook: bool | None = None,
) -> Path:
    """Convert the downloaded master index into a structured CSV file."""

    master_path = Path(master_path)
    output_path = Path(output_path)

    if not master_path.exists():
        raise FileNotFoundError(f"Master index not found: {master_path}")

    total_entries: int | None = None
    if show_progress:
        with master_path.open("r", encoding="latin1", errors="ignore") as count_handle:
            total_entries = sum(1 for line in count_handle if ".txt" in line)

    progress = (
        resolve_tqdm(use_notebook)(
            total=total_entries,
            desc="Building full index",
            unit="line",
            dynamic_ncols=True,
            mininterval=0.1,
            leave=True,
        )
        if show_progress
        else None
    )

    try:
        with output_path.open("w", newline="", encoding="utf-8", errors="ignore") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["cik", "comnam", "form", "date", "url"])
            with master_path.open("r", encoding="latin1", errors="ignore") as handle:
                for line in handle:
                    if ".txt" not in line:
                        continue
                    writer.writerow(line.strip().split("|"))
                    if progress is not None:
                        progress.update(1)
    finally:
        if progress is not None:
            progress.close()

    try:
        master_path.unlink()
        logger.info("Removed master index after building %s", output_path)
    except FileNotFoundError:
        logger.info("Master index already removed after building %s", output_path)

    return output_path
