#!/bin/python
import argparse
import csv
from typing import Iterable, Iterator, Optional, Tuple

import requests

from sec_utils import RateLimiter, build_request_headers

try:
    from tqdm import tqdm
except ImportError:  # pragma: no cover - fallback when tqdm isn't installed
    tqdm = None  # type: ignore


def _progress(
    iterable: Iterable[Tuple[int, int]],
    *,
    enabled: bool,
    total: int,
) -> Iterator[Tuple[int, int]]:
    """Return an iterator optionally wrapped in a tqdm progress bar."""

    if enabled and tqdm is not None:
        return iter(tqdm(iterable, total=total, desc="Master index", unit="file"))
    return iter(iterable)


def download_master_index(
    requests_per_second: float,
    name: Optional[str],
    email: Optional[str],
    *,
    show_progress: bool = True,
) -> None:
    headers = build_request_headers(name, email)
    limiter = RateLimiter(requests_per_second)

    with open("master.idx", "wb") as f:
        years = range(1994, 2023)
        quarters = range(1, 5)
        total_files = len(years) * len(quarters)
        iterator = ((year, quarter) for year in years for quarter in quarters)
        for year, quarter in _progress(iterator, enabled=show_progress, total=total_files):
            limiter.wait()
            response = requests.get(
                f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{quarter}/master.idx",
                headers=headers,
                timeout=60,
            )
            response.raise_for_status()
            f.write(response.content)


def write_full_index() -> None:
    with open("full_index.csv", "w", errors="ignore") as csvfile:
        wr = csv.writer(csvfile)
        wr.writerow(["cik", "comnam", "form", "date", "url"])
        with open("master.idx", "r", encoding="latin1") as f:
            for r in f:
                if ".txt" in r:
                    wr.writerow(r.strip().split("|"))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--requests-per-second",
        type=float,
        default=10.0,
        help="Maximum number of requests per second when downloading master index files.",
    )
    parser.add_argument("--sec-name", help="Contact name to include in SEC requests.")
    parser.add_argument("--sec-email", help="Contact email to include in SEC requests.")
    parser.add_argument(
        "--no-progress",
        action="store_true",
        help="Disable progress bars for non-interactive environments.",
    )
    args = parser.parse_args()

    download_master_index(
        args.requests_per_second,
        args.sec_name,
        args.sec_email,
        show_progress=not args.no_progress,
    )
    write_full_index()
