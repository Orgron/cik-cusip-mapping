"""Parse CUSIP identifiers from streamed filings."""

from __future__ import annotations

import csv
import re
from collections import Counter, deque
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Deque, Iterable, Iterator, Protocol, Sequence

CUSIP_PATTERN = re.compile(
    r"[\( >]*[0-9A-Z]{1}[0-9]{3}[0-9A-Za-z]{2}[- ]*[0-9]{0,2}[- ]*[0-9]{0,1}[\) \n<]*"
)
WORD_PATTERN = re.compile(r"\w+")
IRS_TOKENS = {"IRS", "I.R.S"}


@dataclass
class ParsedFiling:
    identifier: str
    cik: str | None
    cusip: str | None


class FilingLike(Protocol):
    identifier: str
    content: str


def _extract_cik(lines: Sequence[str]) -> str | None:
    record = False
    for line in lines:
        if "SUBJECT COMPANY" in line:
            record = True
        if "CENTRAL INDEX KEY" in line and record:
            return line.split("\t\t\t")[-1].strip()
    return None


def _extract_cusip(lines: Sequence[str], *, debug: bool = False) -> str | None:
    record = False
    matches: list[str] = []
    for line in lines:
        if "<DOCUMENT>" in line:
            record = True
        if record and not any(token in line for token in IRS_TOKENS):
            found = CUSIP_PATTERN.findall(line)
            if found:
                cusip = found[0].strip().strip("<>")
                if debug:
                    print("INFO: added --- ", line, " --- extracted [", cusip, "]")
                matches.append(cusip)
    if not matches:
        return None
    most_common = Counter(matches).most_common(1)[0][0]
    return "".join(WORD_PATTERN.findall(most_common))


def parse_text(text: str, *, debug: bool = False) -> tuple[str | None, str | None]:
    lines = text.splitlines()
    cik = _extract_cik(lines)
    cusip = _extract_cusip(lines, debug=debug)
    if debug:
        print(cusip)
    return cik, cusip


def parse_file(path: Path | str, *, debug: bool = False) -> ParsedFiling:
    path = Path(path)
    text = path.read_text(errors="ignore")
    cik, cusip = parse_text(text, debug=debug)
    return ParsedFiling(str(path), cik, cusip)


def parse_filings(
    filings: Iterable[FilingLike],
    *,
    debug: bool = False,
) -> Iterator[ParsedFiling]:
    for filing in filings:
        cik, cusip = parse_text(filing.content, debug=debug)
        yield ParsedFiling(filing.identifier, cik, cusip)


def parse_filings_concurrently(
    filings: Iterable[FilingLike],
    *,
    debug: bool = False,
    max_queue: int = 32,
    workers: int = 2,
) -> Iterator[ParsedFiling]:
    with ThreadPoolExecutor(max_workers=workers) as executor:
        pending: Deque[tuple[FilingLike, Future[tuple[str | None, str | None]]]] = deque()
        for filing in filings:
            future = executor.submit(parse_text, filing.content, debug=debug)
            pending.append((filing, future))
            if len(pending) >= max_queue:
                oldest_filing, oldest_future = pending.popleft()
                cik, cusip = oldest_future.result()
                yield ParsedFiling(oldest_filing.identifier, cik, cusip)
        while pending:
            filing, future = pending.popleft()
            cik, cusip = future.result()
            yield ParsedFiling(filing.identifier, cik, cusip)


def stream_to_csv(
    filings: Iterable[FilingLike],
    csv_path: Path | str,
    *,
    debug: bool = False,
    concurrent: bool = False,
    max_queue: int = 32,
    workers: int = 2,
) -> int:
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
    with csv_path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        for parsed in iterator:
            writer.writerow([parsed.identifier, parsed.cik, parsed.cusip])
            count += 1
    return count


def _iter_directory_files(path: Path) -> Iterator[Path]:
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
    directory = Path(directory)
    if not directory.exists():
        raise FileNotFoundError(f"Directory not found: {directory}")
    output_csv = output_csv or Path(f"{directory}.csv")
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with output_csv.open("w", newline="") as handle:
        writer = csv.writer(handle)
        if concurrent:
            filings = (
                SimpleNamespace(
                    identifier=str(file_path),
                    content=file_path.read_text(errors="ignore"),
                )
                for file_path in _iter_directory_files(directory)
            )
            iterator: Iterable[ParsedFiling] = parse_filings_concurrently(
                filings,
                debug=debug,
                max_queue=max_queue,
                workers=workers,
            )
        else:
            iterator = (
                parse_file(file_path, debug=debug)
                for file_path in _iter_directory_files(directory)
            )
        for parsed in iterator:
            writer.writerow([parsed.identifier, parsed.cik, parsed.cusip])
            count += 1
    return count
