"""Parse CUSIP identifiers from streamed filings."""

from __future__ import annotations

import csv
import html
import re
from collections import Counter, deque
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Deque, Iterable, Iterator, Protocol, Sequence

from tqdm.auto import tqdm

CUSIP_PATTERN = re.compile(r"[0-9A-Z](?:[- ]?[0-9A-Z]){8,10}")
TAG_PATTERN = re.compile(r"<[^>]+>")


@dataclass
class ParsedFiling:
    """Structured result returned by parsing a single filing."""

    identifier: str
    cik: str | None
    cusip: str | None
    form: str | None = None
    filing_date: str | None = None
    accession_number: str | None = None
    url: str | None = None
    company_name: str | None = None
    parse_method: str | None = None


class FilingLike(Protocol):
    """Protocol that describes the minimal attributes required from a filing."""

    identifier: str
    content: str


def _extract_cik(lines: Sequence[str]) -> str | None:
    """Return the CIK value embedded in the filing header, if present."""

    record = False
    for line in lines:
        if "SUBJECT COMPANY" in line:
            record = True
        if "CENTRAL INDEX KEY" in line and record:
            return line.split("\t\t\t")[-1].strip()
    return None


def _normalize_cusip(value: str) -> str | None:
    """Normalize a candidate CUSIP string to an uppercase 9-character token."""

    cleaned = "".join(ch for ch in value.upper() if ch.isalnum())
    if len(cleaned) == 9 and any(ch.isdigit() for ch in cleaned):
        return cleaned
    return None


def _extract_matches(text: str) -> list[str]:
    """Find normalized CUSIP candidates within the provided text block."""

    cleaned = TAG_PATTERN.sub(" ", html.unescape(text))
    upper = cleaned.upper()
    matches: list[str] = []
    for match in CUSIP_PATTERN.finditer(upper):
        normalized = _normalize_cusip(match.group())
        if normalized:
            start, end = match.span()
            context = upper[max(0, start - 16) : end + 16]
            if "IRS NUMBER" in context or "I.R.S" in context:
                continue
            matches.append(normalized)
    return matches


def _extract_cusip(
    lines: Sequence[str], *, debug: bool = False
) -> tuple[str | None, str | None]:
    """Locate the best CUSIP match in the filing and describe the method used."""

    record = False
    matches: list[tuple[str, str]] = []
    candidate_segments: list[str] = []
    document_segments: list[str] = []
    for index, raw_line in enumerate(lines):
        if "<DOCUMENT>" in raw_line:
            record = True
        if not record:
            continue
        document_segments.append(raw_line)
        upper_line = raw_line.upper()
        if "CUSIP" in upper_line:
            window = lines[index : index + 10]
            candidate_segments.append("\n".join(window))
    for segment in candidate_segments:
        for normalized in _extract_matches(segment):
            matches.append((normalized, "window"))
            if debug:
                print("INFO: candidate match", normalized, "from", segment.strip())
    if not matches and document_segments:
        combined = "\n".join(document_segments)
        for normalized in _extract_matches(combined):
            matches.append((normalized, "fallback"))
            if debug:
                print("INFO: fallback match", normalized)
    if not matches:
        return None, None
    winner, _ = Counter(match for match, _ in matches).most_common(1)[0]
    winner_methods = [method for match, method in matches if match == winner]
    parse_method = winner_methods[0] if winner_methods else None
    return winner, parse_method


def parse_text(
    text: str, *, debug: bool = False
) -> tuple[str | None, str | None, str | None]:
    """Parse raw filing text and return the CIK, CUSIP, and parse method."""

    lines = text.splitlines()
    cik = _extract_cik(lines)
    cusip, parse_method = _extract_cusip(lines, debug=debug)
    if debug:
        print(cusip)
    return cik, cusip, parse_method


def parse_file(path: Path | str, *, debug: bool = False) -> ParsedFiling:
    """Parse a filing stored on disk into a :class:`ParsedFiling` record."""

    path = Path(path)
    text = path.read_text(errors="ignore")
    cik, cusip, parse_method = parse_text(text, debug=debug)
    return ParsedFiling(str(path), cik, cusip, parse_method=parse_method)


def parse_filings(
    filings: Iterable[FilingLike],
    *,
    debug: bool = False,
) -> Iterator[ParsedFiling]:
    """Iterate over in-memory filings and yield parsed results."""

    for filing in filings:
        cik, cusip, parse_method = parse_text(filing.content, debug=debug)
        yield ParsedFiling(
            filing.identifier,
            cik,
            cusip,
            form=getattr(filing, "form", None),
            filing_date=getattr(filing, "date", None),
            accession_number=getattr(filing, "accession_number", None),
            url=getattr(filing, "url", None),
            company_name=getattr(filing, "company_name", None),
            parse_method=parse_method,
        )


def parse_filings_concurrently(
    filings: Iterable[FilingLike],
    *,
    debug: bool = False,
    max_queue: int = 32,
    workers: int = 2,
) -> Iterator[ParsedFiling]:
    """Parse filings using a thread pool to overlap I/O and computation."""

    with ThreadPoolExecutor(max_workers=workers) as executor:
        pending: Deque[
            tuple[FilingLike, Future[tuple[str | None, str | None, str | None]]]
        ] = (
            deque()
        )
        for filing in filings:
            future = executor.submit(parse_text, filing.content, debug=debug)
            pending.append((filing, future))
            if len(pending) >= max_queue:
                oldest_filing, oldest_future = pending.popleft()
                cik, cusip, parse_method = oldest_future.result()
                yield ParsedFiling(
                    oldest_filing.identifier,
                    cik,
                    cusip,
                    form=getattr(oldest_filing, "form", None),
                    filing_date=getattr(oldest_filing, "date", None),
                    accession_number=getattr(oldest_filing, "accession_number", None),
                    url=getattr(oldest_filing, "url", None),
                    company_name=getattr(oldest_filing, "company_name", None),
                    parse_method=parse_method,
                )
        while pending:
            filing, future = pending.popleft()
            cik, cusip, parse_method = future.result()
            yield ParsedFiling(
                filing.identifier,
                cik,
                cusip,
                form=getattr(filing, "form", None),
                filing_date=getattr(filing, "date", None),
                accession_number=getattr(filing, "accession_number", None),
                url=getattr(filing, "url", None),
                company_name=getattr(filing, "company_name", None),
                parse_method=parse_method,
            )


def stream_to_csv(
    filings: Iterable[FilingLike],
    csv_path: Path | str,
    *,
    debug: bool = False,
    concurrent: bool = False,
    max_queue: int = 32,
    workers: int = 2,
    events_csv_path: Path | str | None = None,
) -> int:
    """Stream parsed filings to CSV files and optionally emit event logs."""

    csv_path = Path(csv_path)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    if concurrent:
        iterator = parse_filings_concurrently(
            filings,
            debug=debug,
            max_queue=max_queue,
            workers=workers,
        )
    else:
        iterator = parse_filings(filings, debug=debug)
    events_writer = None
    events_handle = None
    if events_csv_path is not None:
        events_path = Path(events_csv_path)
        events_path.parent.mkdir(parents=True, exist_ok=True)
        events_handle = events_path.open("w", newline="")
        events_writer = csv.writer(events_handle)
        events_writer.writerow(
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
    try:
        with csv_path.open("w", newline="") as handle:
            writer = csv.writer(handle)
            for parsed in tqdm(iterator, desc="Parsing filings", unit="filing"):
                writer.writerow([parsed.identifier, parsed.cik, parsed.cusip])
                if events_writer is not None:
                    cusip9 = parsed.cusip or ""
                    events_writer.writerow(
                        [
                            parsed.identifier,
                            parsed.cik or "",
                            parsed.form or "",
                            parsed.filing_date or "",
                            parsed.accession_number or "",
                            parsed.url or "",
                            parsed.company_name or "",
                            cusip9,
                            cusip9[:8] if cusip9 else "",
                            cusip9[:6] if cusip9 else "",
                            parsed.parse_method or "",
                        ]
                    )
                count += 1
    finally:
        if events_handle is not None:
            events_handle.close()
    return count


def _iter_directory_files(path: Path) -> Iterator[Path]:
    """Yield files within ``path`` grouped by subdirectory for reproducible order."""

    yield from sorted(path.glob("*/*"))


def parse_directory(
    directory: Path | str,
    *,
    output_csv: Path | None = None,
    debug: bool = False,
    concurrent: bool = False,
    max_queue: int = 32,
    workers: int = 2,
) -> int:
    """Parse every filing stored in ``directory`` and write results to CSV."""

    directory = Path(directory)
    if not directory.exists():
        raise FileNotFoundError(f"Directory not found: {directory}")
    output_csv = output_csv or Path(f"{directory}.csv")
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    file_paths = list(_iter_directory_files(directory))
    with output_csv.open("w", newline="") as handle:
        writer = csv.writer(handle)
        if concurrent:
            filings = (
                SimpleNamespace(
                    identifier=str(file_path),
                    content=file_path.read_text(errors="ignore"),
                )
                for file_path in file_paths
            )
            iterator = parse_filings_concurrently(
                filings,
                debug=debug,
                max_queue=max_queue,
                workers=workers,
            )
        else:
            iterator = (
                parse_file(file_path, debug=debug)
                for file_path in file_paths
            )
        for parsed in tqdm(iterator, total=len(file_paths), desc="Parsing directory", unit="filing"):
            writer.writerow([parsed.identifier, parsed.cik, parsed.cusip])
            count += 1
    return count
