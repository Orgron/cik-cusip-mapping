"""Utilities for streaming SEC filing contents."""

from __future__ import annotations

import csv
import gzip
import logging
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Generator, Iterable, Iterator

import requests

from .filters import coerce_date, is_amended_form, normalize_cik_whitelist
from .sec import RateLimiter, build_request_headers, create_session
from .progress import resolve_tqdm

ARCHIVES_URL = "https://www.sec.gov/Archives/"

logger = logging.getLogger(__name__)


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

    @property
    def filing_url(self) -> str:
        """Reconstruct the public EDGAR index URL for this filing."""

        from .parsing import reconstruct_filing_url

        return reconstruct_filing_url(self.cik, self.accession_number)


def _iter_index_rows(
    form: str,
    index_path: Path,
    *,
    start_date: date | datetime | str | None = None,
    end_date: date | datetime | str | None = None,
    cik_whitelist: Iterable[str | int] | None = None,
    amended_only: bool = False,
) -> Iterator[dict[str, str]]:
    """Yield index rows matching ``form`` from ``index_path`` applying filters."""

    parsed_start = coerce_date(start_date)
    parsed_end = coerce_date(end_date)
    if parsed_start and parsed_end and parsed_start > parsed_end:
        raise ValueError("start_date cannot be after end_date")

    cik_filters = normalize_cik_whitelist(cik_whitelist)

    with index_path.open("r", newline="", encoding="utf-8") as index_file:
        reader = csv.DictReader(index_file)
        for row in reader:
            row_form = row["form"].strip()
            if form not in row_form:
                continue

            if amended_only and not is_amended_form(row_form):
                continue

            if parsed_start or parsed_end:
                row_date = coerce_date(row["date"].strip())
                if parsed_start and row_date < parsed_start:
                    continue
                if parsed_end and row_date > parsed_end:
                    continue

            if cik_filters is not None:
                raw_ciks, numeric_ciks = cik_filters
                row_cik = "".join(row["cik"].split())
                if row_cik not in raw_ciks:
                    try:
                        row_cik_int = int(row_cik)
                    except ValueError:
                        row_cik_int = None
                    if row_cik_int not in numeric_ciks:
                        continue

            yield row


def stream_filings(
    form: str,
    requests_per_second: float,
    name: str | None = None,
    email: str | None = None,
    *,
    index_path: Path | str = Path("full_index.csv"),
    session: requests.Session | None = None,
    show_progress: bool = True,
    progress_desc: str | None = None,
    total_hint: int | None = None,
    use_notebook: bool | None = None,
    start_date: date | datetime | str | None = None,
    end_date: date | datetime | str | None = None,
    cik_whitelist: Iterable[str | int] | None = None,
    amended_only: bool = False,
) -> Generator[Filing, None, None]:
    """Yield filings for the requested form directly from EDGAR."""

    headers = build_request_headers(name, email)
    limiter = RateLimiter(requests_per_second)
    index_path = Path(index_path)
    created_session = session is None
    http = session or create_session()

    description = progress_desc or f"Streaming {form} filings"
    progress_factory = resolve_tqdm(use_notebook)
    progress = (
        progress_factory(
            total=total_hint,
            desc=description,
            unit="filing",
            dynamic_ncols=True,
            mininterval=0.1,
            leave=False,
        )
        if show_progress
        else None
    )
    try:
        for row in _iter_index_rows(
            form,
            index_path,
            start_date=start_date,
            end_date=end_date,
            cik_whitelist=cik_whitelist,
            amended_only=amended_only,
        ):
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
            except requests.HTTPError as exc:  # pragma: no cover - defensive
                response = exc.response
                status = response.status_code if response is not None else "unknown"
                response_url = (
                    response.url if response is not None else f"{ARCHIVES_URL}{url}"
                )
                logger.warning(
                    "Failed to download filing %s on %s (status=%s, url=%s): %s",
                    cik,
                    date,
                    status,
                    response_url,
                    exc,
                )
                continue
            except requests.RequestException as exc:  # pragma: no cover - defensive
                logger.warning(
                    "Error downloading filing %s on %s from %s: %s",
                    cik,
                    date,
                    f"{ARCHIVES_URL}{url}",
                    exc,
                )
                continue

            if progress is not None:
                progress.update(1)
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
    finally:
        if progress is not None:
            progress.close()
        if created_session:
            http.close()


def stream_filings_to_disk(
    form: str,
    output_dir: Path,
    requests_per_second: float,
    name: str | None,
    email: str | None,
    *,
    index_path: Path | str = Path("full_index.csv"),
    session: requests.Session | None = None,
    compress: bool = False,
    start_date: date | datetime | str | None = None,
    end_date: date | datetime | str | None = None,
    cik_whitelist: Iterable[str | int] | None = None,
    amended_only: bool = False,
) -> int:
    """Persist streamed filings to disk for archival purposes."""

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    count = 0
    filings = stream_filings(
        form,
        requests_per_second,
        name,
        email,
        index_path=index_path,
        session=session,
        show_progress=False,
        start_date=start_date,
        end_date=end_date,
        cik_whitelist=cik_whitelist,
        amended_only=amended_only,
    )
    tqdm_factory = resolve_tqdm(None)
    for filing in tqdm_factory(
        filings,
        desc=f"Saving {form} filings",
        unit="filing",
        dynamic_ncols=True,
        mininterval=0.1,
    ):
        year, month = filing.date.split("-")[:2]
        destination = output_dir / f"{year}_{month}"
        destination.mkdir(parents=True, exist_ok=True)
        file_path = destination / filing.identifier
        if compress:
            file_path = file_path.with_suffix(f"{file_path.suffix}.gz")
            with gzip.open(
                file_path,
                "wt",
                encoding="utf-8",
                errors="ignore",
            ) as handle:
                handle.write(filing.content)
        else:
            file_path.write_text(
                filing.content,
                encoding="utf-8",
                errors="ignore",
            )
        count += 1
    return count
