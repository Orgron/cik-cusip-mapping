"""Harness for comparing extract_cusip output against manual annotations."""

from __future__ import annotations

import json
import re
import sys
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional


# Ensure the repository root (containing ``main.py``) is importable when the
# script is executed directly via ``python sample_analysis/review.py``.
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from main import extract_cusip


@dataclass
class FilingMetadata:
    """Metadata extracted from the SEC header for traceability."""

    filename: str
    cik: Optional[str]
    form_type: Optional[str]
    accession_number: Optional[str]
    filed_as_of_date: Optional[str]


@dataclass
class FilingReview:
    """Comparison record between automated and manual CUSIP values."""

    metadata: FilingMetadata
    extracted_cusip: Optional[str]
    manual_cusip: Optional[str]
    matches: Optional[bool]
    snippets: list[str]


HEADER_PATTERNS: dict[str, re.Pattern[str]] = {
    "accession_number": re.compile(r"ACCESSION\s+NUMBER:\s*([^\r\n]+)", re.IGNORECASE),
    "form_type": re.compile(r"FORM\s+TYPE:\s*([^\r\n]+)", re.IGNORECASE),
    "filed_as_of_date": re.compile(r"FILED\s+AS\s+OF\s+DATE:\s*([^\r\n]+)", re.IGNORECASE),
    # The subject company's CIK appears first in the header; capture the first match.
    "cik": re.compile(r"CENTRAL\s+INDEX\s+KEY:\s*([^\r\n]+)", re.IGNORECASE),
}


def parse_header_metadata(text: str, filename: str) -> FilingMetadata:
    """Parse key header metadata from the raw filing text."""

    values: dict[str, Optional[str]] = {key: None for key in HEADER_PATTERNS}

    for key, pattern in HEADER_PATTERNS.items():
        match = pattern.search(text)
        if match:
            values[key] = match.group(1).strip()

    return FilingMetadata(
        filename=filename,
        cik=values["cik"],
        form_type=values["form_type"],
        accession_number=values["accession_number"],
        filed_as_of_date=values["filed_as_of_date"],
    )


def extract_snippets(text: str, limit: int = 3, context: int = 160) -> list[str]:
    """Return compact snippets surrounding CUSIP markers for manual review."""

    snippets: list[str] = []
    for match in re.finditer(r"CUSIP", text, re.IGNORECASE):
        start = max(0, match.start() - context)
        end = min(len(text), match.end() + context)
        window = text[start:end]
        window = " ".join(window.split())  # Collapse whitespace for readability.
        snippets.append(window)
        if len(snippets) >= limit:
            break

    return snippets


def load_manual_cusips(manual_path: Path) -> dict[str, Optional[str]]:
    """Load the manual CUSIP annotations keyed by filename."""

    with manual_path.open("r", encoding="utf-8") as fh:
        raw = json.load(fh)

    manual: dict[str, Optional[str]] = {}
    for filename, value in raw.items():
        manual[filename] = value if value else None
    return manual


def build_review_records(
    sample_dir: Path,
    manual_map: dict[str, Optional[str]],
) -> Iterable[FilingReview]:
    """Yield comparison records for every filing in the sample corpus."""

    for path in sorted(sample_dir.glob("*.txt")):
        text = path.read_text(encoding="utf-8", errors="ignore")
        metadata = parse_header_metadata(text, path.name)
        extracted = extract_cusip(text)
        manual = manual_map.get(path.name)

        matches: Optional[bool]
        if manual is None and extracted is None:
            matches = True
        elif manual is None:
            matches = False
        elif extracted is None:
            matches = False
        else:
            matches = extracted == manual

        snippets = extract_snippets(text)

        yield FilingReview(
            metadata=metadata,
            extracted_cusip=extracted,
            manual_cusip=manual,
            matches=matches,
            snippets=snippets,
        )


def write_comparison_json(records: Iterable[FilingReview], output_path: Path) -> None:
    """Persist comparison records as JSON for downstream analysis."""

    serialisable = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "records": [
            {
                "metadata": asdict(record.metadata),
                "extracted_cusip": record.extracted_cusip,
                "manual_cusip": record.manual_cusip,
                "matches": record.matches,
                "snippets": record.snippets,
            }
            for record in records
        ],
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(serialisable, indent=2), encoding="utf-8")


def print_summary(records: Iterable[FilingReview]) -> None:
    """Print a human-friendly summary of comparison results."""

    header = f"{'Filename':<15} {'CIK':<12} {'Form':<8} {'Extracted':<12} {'Manual':<12} Match"
    print(header)
    print("-" * len(header))

    for record in records:
        metadata = record.metadata
        extracted = record.extracted_cusip or "-"
        manual = record.manual_cusip or "-"
        match_flag = "yes" if record.matches else "no"
        cik = metadata.cik or "-"
        form = (metadata.form_type or "-").replace("SC ", "")
        print(
            f"{metadata.filename:<15} {cik:<12} {form:<8} {extracted:<12} {manual:<12} {match_flag}"
        )


def main() -> None:
    sample_dir = Path("sample_filings")
    manual_path = Path("sample_analysis/manual_cusips.json")
    output_path = Path("sample_analysis/cusip_comparison.json")

    if not sample_dir.exists():
        raise SystemExit(f"Sample directory not found: {sample_dir}")
    if not manual_path.exists():
        raise SystemExit(f"Manual annotations missing: {manual_path}")

    manual_map = load_manual_cusips(manual_path)
    records = list(build_review_records(sample_dir, manual_map))
    write_comparison_json(records, output_path)
    print_summary(records)


if __name__ == "__main__":
    main()
