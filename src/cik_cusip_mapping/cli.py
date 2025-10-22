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
        "--emit-dynamics",
        dest="emit_dynamics",
        action="store_true",
        default=True,
        help="Generate per-form events and aggregated dynamics tables (default).",
    )
    parser.add_argument(
        "--no-emit-dynamics",
        dest="emit_dynamics",
        action="store_false",
        help="Disable per-form event and dynamics outputs.",
    )
    parser.add_argument(
        "--events-output-root",
        default=".",
        help="Directory where per-form filing event CSVs should be written.",
    )
    parser.add_argument(
        "--dynamics-output-file",
        default="cik-cusip-dynamics.csv",
        help="Destination filename for aggregated dynamics metrics (relative to output-root by default).",
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
        "--parsing-workers",
        type=int,
        default=2,
        help="Number of worker threads to use when concurrent parsing is enabled.",
    )
    parser.add_argument(
        "--parsing-max-queue",
        type=int,
        default=32,
        help="Maximum number of pending filings to keep queued during concurrent parsing.",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable verbose parsing diagnostics.",
    )
    parser.add_argument(
        "--show-progress",
        dest="show_progress",
        action="store_true",
        default=True,
        help="Display tqdm progress bars while downloading and parsing (default).",
    )
    parser.add_argument(
        "--no-show-progress",
        dest="show_progress",
        action="store_false",
        help="Disable tqdm progress bars, useful for non-interactive environments.",
    )

    args = parser.parse_args()

    mapping, dynamics = pipeline.run_pipeline(
        forms=args.forms,
        output_root=Path(args.output_root),
        output_file=Path(args.output_file),
        emit_dynamics=args.emit_dynamics,
        events_output_root=Path(args.events_output_root),
        dynamics_output_file=Path(args.dynamics_output_file),
        requests_per_second=args.requests_per_second,
        sec_name=args.sec_name,
        sec_email=args.sec_email,
        skip_index=args.skip_index,
        skip_download=args.skip_download,
        skip_parse=args.skip_parse,
        index_path=Path(args.index_path) if args.index_path else None,
        concurrent_parsing=not args.disable_concurrency,
        debug=args.debug,
        parsing_workers=args.parsing_workers,
        parsing_max_queue=args.parsing_max_queue,
        show_progress=args.show_progress,
    )

    output_path = Path(args.output_file)
    if not output_path.is_absolute():
        output_path = Path(args.output_root) / output_path
    print(f"Generated {len(mapping)} CIK/CUSIP mappings at {output_path}")

    if args.emit_dynamics and dynamics is not None:
        dynamics_path = Path(args.dynamics_output_file)
        if not dynamics_path.is_absolute():
            dynamics_path = Path(args.output_root) / dynamics_path
        print(f"Aggregated {len(dynamics)} dynamics rows at {dynamics_path}")
