#!/usr/bin/env python3
"""End-to-end driver for building the CIK to CUSIP mapping."""

import argparse
import os
import subprocess
import sys
from pathlib import Path
from typing import List, Sequence


def run_step(description: str, command: List[str]) -> None:
    """Execute a subprocess command while logging its purpose."""
    print(f"\n=== {description} ===")
    subprocess.run(command, check=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the entire CIK to CUSIP mapping pipeline."
    )
    parser.add_argument(
        "--forms",
        nargs="+",
        default=["13D", "13G"],
        help="List of SEC form types to download and parse (default: 13D 13G).",
    )
    parser.add_argument(
        "--output-root",
        default=".",
        help="Directory where form folders and intermediate CSVs will be stored.",
    )
    parser.add_argument(
        "--output-file",
        default="cik-cusip-maps.csv",
        help="Destination for the final mapping CSV produced by post_proc.py.",
    )
    parser.add_argument(
        "--skip-index",
        action="store_true",
        help="Skip downloading the master index (requires existing full_index.csv).",
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip downloading filings (requires existing downloaded filings).",
    )
    parser.add_argument(
        "--skip-parse",
        action="store_true",
        help="Skip parsing filings (requires existing <folder>.csv files).",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)

    base_path = Path(args.output_root)
    base_path.mkdir(parents=True, exist_ok=True)

    index_path = Path("full_index.csv")
    if args.skip_index:
        if not index_path.exists():
            raise FileNotFoundError(
                "full_index.csv not found. Remove --skip-index or generate the file first."
            )
        print("Skipping master index download; using existing full_index.csv.")
    else:
        run_step("Downloading EDGAR master index", [sys.executable, "dl_idx.py"])

    forms = args.forms
    form_entries = []
    for form in forms:
        folder_path = base_path / form
        folder_path.mkdir(parents=True, exist_ok=True)
        folder_arg = os.path.relpath(folder_path, start=Path.cwd())
        csv_path = f"{folder_arg}.csv"
        form_entries.append((form, folder_arg, csv_path))
    csv_paths = [entry[2] for entry in form_entries]

    if args.skip_download:
        print("Skipping filing downloads.")
    else:
        for form, folder, _ in form_entries:
            run_step(
                f"Downloading {form} filings",
                [sys.executable, "dl.py", form, folder],
            )

    if args.skip_parse:
        print("Skipping CUSIP parsing.")
        for csv_path in csv_paths:
            if not Path(csv_path).exists():
                raise FileNotFoundError(
                    f"Expected CSV {csv_path} not found. Remove --skip-parse or generate it first."
                )
    else:
        for form, folder, _ in form_entries:
            run_step(
                f"Parsing CUSIPs from {form} filings",
                [sys.executable, "parse_cusip.py", folder],
            )

    run_step(
        "Post-processing CUSIP mappings",
        [sys.executable, "post_proc.py", *csv_paths],
    )

    generated_mapping_path = Path("cik-cusip-maps.csv").resolve()
    desired_output = Path(args.output_file).resolve()

    if desired_output != generated_mapping_path:
        desired_output.parent.mkdir(parents=True, exist_ok=True)
        generated_mapping_path.replace(desired_output)
        print(f"Moved final mapping to {desired_output}")
    else:
        print(f"Final mapping written to {generated_mapping_path}")


if __name__ == "__main__":
    main()
