#!/usr/bin/env python3
"""Compare parser output against the manually curated ground truth dataset."""

from __future__ import annotations

import argparse
import csv
import importlib.util
import sys
import types
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


@dataclass
class ComparisonResult:
    """Record describing the comparison outcome for a single filing."""

    filename: str
    expected: str
    observed: str
    parse_method: str | None

    @property
    def matches(self) -> bool:
        return canonical(self.expected) == canonical(self.observed)


def _load_parsing_module(project_root: Path):
    package_name = "cik_cusip_mapping"
    parsing_path = project_root / "src" / package_name / "parsing.py"
    if package_name not in sys.modules:
        package = types.ModuleType(package_name)
        package.__path__ = [str(parsing_path.parent)]
        sys.modules[package_name] = package
    spec = importlib.util.spec_from_file_location(
        f"{package_name}.parsing", parsing_path
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("Unable to load parsing helpers")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def canonical(value: str | None) -> str:
    """Return a canonical representation for comparison."""

    if value is None:
        return ""
    cleaned = value.strip().upper()
    if not cleaned:
        return ""
    if cleaned == "NONE":
        return "NONE"
    tokens: list[str] = []
    seen: set[str] = set()
    for part in cleaned.split(";"):
        normalized = "".join(ch for ch in part if ch.isalnum())
        if not normalized:
            continue
        if normalized not in seen:
            tokens.append(normalized)
            seen.add(normalized)
    tokens.sort()
    return ";".join(tokens)


def load_ground_truth(manual_path: Path) -> list[tuple[str, str]]:
    records: list[tuple[str, str]] = []
    with manual_path.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            filename = row.get("filename", "").strip()
            expected = row.get("cusip", "").strip()
            if not filename or not expected:
                continue
            records.append((filename, expected))
    return records


def compare_filings(
    filings: Iterable[tuple[Path, str]], module
) -> Iterable[ComparisonResult]:
    for path, expected in filings:
        if not path.exists():
            yield ComparisonResult(path.name, expected, "", None)
            continue
        text = path.read_text(encoding="utf-8", errors="ignore")
        _cik, cusip, method = module.parse_text(text)
        yield ComparisonResult(path.name, expected, cusip or "", method)


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Compare parser output against manual ground truth",
    )
    parser.add_argument(
        "--forms-dir",
        type=Path,
        default=Path("form_examples"),
        help="Directory containing filing examples",
    )
    parser.add_argument(
        "--manual-path",
        type=Path,
        default=Path("analysis/manual_input.csv"),
        help="Path to the manual ground-truth CSV",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional limit on the number of filings to compare",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_argument_parser()
    args = parser.parse_args(argv)
    project_root = Path(__file__).resolve().parents[1]
    module = _load_parsing_module(project_root)
    records = load_ground_truth(args.manual_path)
    if args.limit is not None:
        records = records[: args.limit]

    filings = [
        (args.forms_dir / filename, expected) for filename, expected in records
    ]
    mismatches: list[ComparisonResult] = []
    matches = 0
    for result in compare_filings(filings, module):
        if result.matches:
            matches += 1
        else:
            mismatches.append(result)

    total = matches + len(mismatches)
    print(f"Compared filings: {total}")
    print(f"Matches: {matches}")
    print(f"Mismatches: {len(mismatches)}")
    if mismatches:
        print("\nMismatched filings:")
        for mismatch in mismatches:
            observed = mismatch.observed or '""'
            method = mismatch.parse_method or '""'
            print(
                f"- {mismatch.filename}: expected {mismatch.expected}"
                f" | observed {observed} (method={method})"
            )
    return 1 if mismatches else 0


if __name__ == "__main__":
    raise SystemExit(main())
