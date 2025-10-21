#!/usr/bin/env python3
"""End-to-end driver for building the CIK to CUSIP mapping."""

from __future__ import annotations

import argparse
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Sequence

import dl
import dl_idx
import parse_cusip


def run_step(description: str, action: Callable[..., None], *args, **kwargs) -> None:
    """Execute a pipeline step while logging its purpose."""

    print(f"\n=== {description} ===")
    action(*args, **kwargs)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the entire CIK to CUSIP mapping pipeline.",
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
        "--requests-per-second",
        type=float,
        default=10.0,
        help="Maximum number of requests per second when downloading from the SEC.",
    )
    parser.add_argument(
        "--sec-name",
        help="Contact name to include in SEC download requests.",
    )
    parser.add_argument(
        "--sec-email",
        help="Contact email to include in SEC download requests.",
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
        run_step(
            "Downloading EDGAR master index",
            _download_master_index,
            args.requests_per_second,
            args.sec_name,
            args.sec_email,
        )

    forms = args.forms
    csv_paths: list[str] = []
    skip_streaming = args.skip_download or args.skip_parse
    for form in forms:
        csv_path = base_path / f"{form}.csv"
        if skip_streaming:
            print(f"Skipping streaming for {form} filings.")
            if not csv_path.exists():
                raise FileNotFoundError(
                    f"Expected CSV {csv_path} not found. Remove --skip-parse/--skip-download or generate it first."
                )
        else:
            run_step(
                f"Streaming {form} filings",
                _stream_form_to_csv,
                form,
                csv_path,
                args.requests_per_second,
                args.sec_name,
                args.sec_email,
            )
        csv_paths.append(str(csv_path))

    run_step(
        "Post-processing CUSIP mappings",
        subprocess.run,
        [sys.executable, "post_proc.py", *csv_paths],
        check=True,
    )

    generated_mapping_path = Path("cik-cusip-maps.csv").resolve()
    desired_output = Path(args.output_file).resolve()

    if desired_output != generated_mapping_path:
        desired_output.parent.mkdir(parents=True, exist_ok=True)
        generated_mapping_path.replace(desired_output)
        print(f"Moved final mapping to {desired_output}")
    else:
        print(f"Final mapping written to {generated_mapping_path}")


def _download_master_index(requests_per_second: float, name: str | None, email: str | None) -> None:
    dl_idx.download_master_index(requests_per_second, name, email)
    dl_idx.write_full_index()


def _stream_form_to_csv(
    form: str,
    csv_path: Path,
    requests_per_second: float,
    name: str | None,
    email: str | None,
) -> None:
    filings = dl.stream_filings(
        form,
        requests_per_second,
        name,
        email,
    )
    parse_cusip.stream_to_csv(filings, csv_path)


if __name__ == "__main__":
    main()
