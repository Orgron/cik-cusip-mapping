"""Utilities to audit CUSIP extraction on bundled sample filings."""

from __future__ import annotations

import csv
import json
import re
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional

from main import extract_cusip

SAMPLE_DIR = Path("sample_filings")
MANUAL_PATH = SAMPLE_DIR / "manual_cusips.json"
AUDIT_JSON = SAMPLE_DIR / "cusip_audit_results.json"
AUDIT_CSV = SAMPLE_DIR / "cusip_audit_results.csv"


@dataclass
class FilingAuditRow:
    """Single comparison row for a filing."""

    filename: str
    cik: Optional[str]
    form_type: Optional[str]
    accession_number: Optional[str]
    automated_cusip: Optional[str]
    manual_cusip: Optional[str]
    automated_context: Optional[str]
    manual_context: Optional[str]
    matches_manual: bool


_HEADER_PATTERNS = {
    "cik": re.compile(r"CENTRAL INDEX KEY:\s*(\d+)", re.IGNORECASE),
    "form_type": re.compile(r"CONFORMED SUBMISSION TYPE:\s*([A-Z0-9 /]+)", re.IGNORECASE),
    "accession_number": re.compile(r"ACCESSION NUMBER:\s*([0-9-]+)", re.IGNORECASE),
}


def _normalize_cusip(candidate: Optional[str]) -> Optional[str]:
    if not candidate:
        return None
    normalized = re.sub(r"[^A-Z0-9]", "", candidate.upper())
    return normalized or None


def _extract_header_metadata(text: str) -> dict[str, Optional[str]]:
    metadata: dict[str, Optional[str]] = {key: None for key in _HEADER_PATTERNS}
    header_text = text[:5000]
    for key, pattern in _HEADER_PATTERNS.items():
        match = pattern.search(header_text)
        if match:
            metadata[key] = match.group(1).strip()
    return metadata


def _build_fuzzy_pattern(cusip: str) -> re.Pattern[str]:
    parts = [re.escape(char) + r"[\s\-]*" for char in cusip]
    regex = "".join(parts)
    return re.compile(regex, re.IGNORECASE)


def _extract_context(text: str, cusip: Optional[str], window: int = 80) -> Optional[str]:
    normalized = _normalize_cusip(cusip)
    if not normalized:
        return None

    pattern = _build_fuzzy_pattern(normalized)
    match = pattern.search(text)
    if not match:
        return None

    start = max(0, match.start() - window)
    end = min(len(text), match.end() + window)
    snippet = text[start:end]
    snippet = re.sub(r"\s+", " ", snippet)
    return snippet.strip()


def build_audit_rows() -> list[FilingAuditRow]:
    manual_map = json.loads(MANUAL_PATH.read_text()) if MANUAL_PATH.exists() else {}

    rows: list[FilingAuditRow] = []
    for path in sorted(SAMPLE_DIR.glob("*.txt")):
        text = path.read_text(errors="ignore")
        metadata = _extract_header_metadata(text)
        automated = extract_cusip(text)
        manual = manual_map.get(path.name)
        normalized_manual = _normalize_cusip(manual)
        normalized_automated = _normalize_cusip(automated)
        matches = normalized_manual == normalized_automated

        rows.append(
            FilingAuditRow(
                filename=path.name,
                cik=metadata.get("cik"),
                form_type=metadata.get("form_type"),
                accession_number=metadata.get("accession_number"),
                automated_cusip=automated,
                manual_cusip=manual,
                automated_context=_extract_context(text, automated),
                manual_context=_extract_context(text, manual),
                matches_manual=matches,
            )
        )

    return rows


def write_audit_artifacts(rows: list[FilingAuditRow]) -> None:
    AUDIT_JSON.write_text(
        json.dumps([asdict(row) for row in rows], indent=2, sort_keys=False)
    )

    with AUDIT_CSV.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(
            csv_file,
            fieldnames=[
                "filename",
                "cik",
                "form_type",
                "accession_number",
                "automated_cusip",
                "manual_cusip",
                "matches_manual",
                "automated_context",
                "manual_context",
            ],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(asdict(row))


if __name__ == "__main__":
    audit_rows = build_audit_rows()
    write_audit_artifacts(audit_rows)

    mismatches = [row for row in audit_rows if not row.matches_manual]
    print(f"Processed {len(audit_rows)} filings.")
    print(f"Matches: {len(audit_rows) - len(mismatches)}")
    print(f"Mismatches: {len(mismatches)}")

    if mismatches:
        print("\nMISMATCH DETAILS:")
        for row in mismatches:
            print("-" * 80)
            print(
                f"{row.filename} | CIK {row.cik or 'UNKNOWN'} | Form {row.form_type or 'UNKNOWN'}"
            )
            print(f"  Manual:    {row.manual_cusip}")
            print(f"  Automated: {row.automated_cusip}")
            if row.manual_context:
                print(f"  Manual context:    {row.manual_context}")
            if row.automated_context:
                print(f"  Automated context: {row.automated_context}")
