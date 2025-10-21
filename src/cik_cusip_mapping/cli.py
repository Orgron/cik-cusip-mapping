"""Console entry points for the cik_cusip_mapping package."""

from __future__ import annotations

import argparse
from pathlib import Path

from . import pipeline


def run_pipeline_cli() -> None:
    """Entry point that mirrors the run_pipeline() function."""

    parser = argparse.ArgumentParser(
        description="Run the CIK to CUSIP mapping pipeline using the library API.",
    )
    parser.add_argument(
        "--forms",
        nargs="+",
        default=["13D", "13G"],
        help="List of SEC form types to download and parse.",
    )
    parser.add_argument(
        "--output-root",
        default=".",
        help="Directory where intermediate CSVs and the final mapping should be written.",
    )
    parser.add_argument(
        "--output-file",
        default="cik-cusip-maps.csv",
        help="Destination filename for the final mapping (relative to output-root by default).",
    )
    parser.add_argument(
        "--requests-per-second",
        type=float,
        default=10.0,
        help="Maximum number of requests per second when downloading from EDGAR.",
    )
    parser.add_argument("--sec-name", help="Contact name to include in SEC requests.")
    parser.add_argument("--sec-email", help="Contact email to include in SEC requests.")
    parser.add_argument(
        "--skip-index",
        action="store_true",
        help="Skip downloading the master index (requires an existing full_index.csv).",
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip downloading filings (requires existing per-form CSVs).",
    )
    parser.add_argument(
        "--skip-parse",
        action="store_true",
        help="Skip parsing filings (requires existing per-form CSVs).",
    )
    parser.add_argument(
        "--index-path",
        help="Optional custom path to an existing full_index.csv when --skip-index is used.",
    )
    parser.add_argument(
        "--disable-concurrency",
        action="store_true",
        help="Disable concurrent parsing if you prefer sequential processing.",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable verbose parsing diagnostics.",
    )

    args = parser.parse_args()

    result = pipeline.run_pipeline(
        forms=args.forms,
        output_root=Path(args.output_root),
        output_file=Path(args.output_file),
        requests_per_second=args.requests_per_second,
        sec_name=args.sec_name,
        sec_email=args.sec_email,
        skip_index=args.skip_index,
        skip_download=args.skip_download,
        skip_parse=args.skip_parse,
        index_path=Path(args.index_path) if args.index_path else None,
        concurrent_parsing=not args.disable_concurrency,
        debug=args.debug,
    )

    output_path = Path(args.output_file)
    if not output_path.is_absolute():
        output_path = Path(args.output_root) / output_path
    print(f"Generated {len(result)} CIK/CUSIP mappings at {output_path}")
