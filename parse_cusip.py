"""Parse CUSIP identifiers from streamed filings."""

from __future__ import annotations

import argparse
import csv
import re
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator, Protocol, Sequence

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


def stream_to_csv(
    filings: Iterable["FilingLike"],
    csv_path: Path | str,
    *,
    debug: bool = False,
) -> int:
    csv_path = Path(csv_path)
    count = 0
    with csv_path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        for filing in filings:
            cik, cusip = parse_text(filing.content, debug=debug)
            writer.writerow([filing.identifier, cik, cusip])
            count += 1
    return count


class FilingLike(Protocol):
    identifier: str
    content: str


def _iter_directory_files(path: Path) -> Iterator[Path]:
    yield from sorted(path.glob("*/*"))


def parse_directory(
    directory: Path | str,
    *,
    output_csv: Path | None = None,
    debug: bool = False,
) -> int:
    directory = Path(directory)
    output_csv = output_csv or Path(f"{directory}.csv")
    count = 0
    with output_csv.open("w", newline="") as handle:
        writer = csv.writer(handle)
        for file_path in _iter_directory_files(directory):
            parsed = parse_file(file_path, debug=debug)
            writer.writerow([parsed.identifier, parsed.cik, parsed.cusip])
            count += 1
    return count


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Parse CUSIPs from filings stored on disk.",
    )
    parser.add_argument("files", help="Directory containing downloaded filings.")
    parser.add_argument(
        "--output",
        help="Destination CSV file (defaults to <directory>.csv).",
    )
    parser.add_argument("--debug", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)

    directory = Path(args.files)
    if not directory.exists():
        raise ValueError("provide a directory of filings to parse ...")

    output = Path(args.output) if args.output else Path(f"{directory}.csv")
    parse_directory(directory, output_csv=output, debug=args.debug)


if __name__ == "__main__":
    main()
