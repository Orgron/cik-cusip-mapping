#!/usr/bin/python
"""Utilities for streaming SEC filing contents."""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Generator, Iterable, Iterator

import requests

from sec_utils import RateLimiter, build_request_headers

ARCHIVES_URL = "https://www.sec.gov/Archives/"


@dataclass
class Filing:
    """Container for SEC filing metadata and content."""

    cik: str
    company_name: str
    form: str
    date: str
    url: str
    accession_number: str
    accession_fragment: str
    content: str

    @property
    def identifier(self) -> str:
        """Legacy-friendly identifier for downstream CSV rows."""

        return f"{self.cik}_{self.date}_{self.accession_fragment}.txt"


def _iter_index_rows(form: str, index_path: Path) -> Iterator[dict[str, str]]:
    with index_path.open("r", newline="") as index_file:
        reader = csv.DictReader(index_file)
        for row in reader:
            if form in row["form"]:
                yield row


def stream_filings(
    form: str,
    requests_per_second: float,
    name: str | None = None,
    email: str | None = None,
    *,
    index_path: Path | str = Path("full_index.csv"),
    session: requests.Session | None = None,
) -> Generator[Filing, None, None]:
    """Yield filings for the requested form directly from EDGAR."""

    headers = build_request_headers(name, email)
    limiter = RateLimiter(requests_per_second)
    index_path = Path(index_path)
    http = session or requests.Session()

    for row in _iter_index_rows(form, index_path):
        cik = row["cik"].strip()
        date = row["date"].strip()
        url = row["url"].strip()
        accession_number = url.split(".")[0].split("/")[-1]
        accession_fragment = accession_number.split("-")[-1]

        try:
            limiter.wait()
            response = http.get(
                f"{ARCHIVES_URL}{url}",
                headers=headers,
                timeout=60,
            )
            response.raise_for_status()
        except Exception as exc:  # pragma: no cover - defensive logging
            print(f"{cik}, {date} failed to download: {exc}")
            continue

        yield Filing(
            cik=cik,
            company_name=row["comnam"].strip(),
            form=row["form"].strip(),
            date=date,
            url=url,
            accession_number=accession_number,
            accession_fragment=accession_fragment,
            content=response.text,
        )


def stream_filings_to_disk(
    form: str,
    output_dir: Path,
    requests_per_second: float,
    name: str | None,
    email: str | None,
    *,
    index_path: Path | str = Path("full_index.csv"),
) -> int:
    """Persist streamed filings to disk for archival purposes."""

    output_dir = Path(output_dir)
    count = 0
    for filing in stream_filings(
        form,
        requests_per_second,
        name,
        email,
        index_path=index_path,
    ):
        year, month = filing.date.split("-")[:2]
        destination = output_dir / f"{year}_{month}"
        destination.mkdir(parents=True, exist_ok=True)
        file_path = destination / filing.identifier
        file_path.write_text(filing.content, errors="ignore")
        count += 1
    return count


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Stream SEC filings from EDGAR.")
    parser.add_argument("form", help="SEC form to stream (e.g. 13D, 13G).")
    parser.add_argument(
        "--output-dir",
        help="Optional directory where streamed filings should be written.",
    )
    parser.add_argument(
        "--requests-per-second",
        type=float,
        default=10.0,
        help="Maximum number of requests per second when downloading filings.",
    )
    parser.add_argument("--sec-name", help="Contact name to include in SEC requests.")
    parser.add_argument("--sec-email", help="Contact email to include in SEC requests.")
    return parser


def main(argv: Iterable[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)

    output_dir = Path(args.output_dir) if args.output_dir else None
    count = 0

    if output_dir is None:
        for filing in stream_filings(
            args.form,
            args.requests_per_second,
            args.sec_name,
            args.sec_email,
        ):
            count += 1
            print(filing.identifier)
    else:
        count = stream_filings_to_disk(
            args.form,
            output_dir,
            args.requests_per_second,
            args.sec_name,
            args.sec_email,
        )

    print(f"Streamed {count} {args.form} filings.")


if __name__ == "__main__":
    main()
