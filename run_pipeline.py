#!/usr/bin/env python3
"""End-to-end driver for building the CIK to CUSIP mapping."""

import argparse
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator, List, Optional, Sequence, Tuple

try:
    from tqdm import tqdm
except ImportError:  # pragma: no cover - fallback when tqdm isn't installed
    tqdm = None  # type: ignore


@dataclass
class ProgressIterator:
    iterable: Iterable[Tuple[str, str, str]]
    desc: str
    total: int
    enabled: bool

    def __post_init__(self) -> None:
        self._bar = None
        if self.enabled and tqdm is not None:
            self._bar = tqdm(self.iterable, total=self.total, desc=self.desc, unit="form")
            self._iterator = iter(self._bar)
        else:
            self._iterator = iter(self.iterable)

    def __iter__(self) -> "ProgressIterator":
        return self

    def __next__(self) -> Tuple[str, str, str]:
        return next(self._iterator)

    def write(self, message: str) -> None:
        if self._bar is not None:
            self._bar.write(message)
        else:
            print(f"\n{message}")

    def close(self) -> None:
        if self._bar is not None:
            self._bar.close()


def run_step(
    description: str, command: List[str], progress: Optional[ProgressIterator] = None
) -> None:
    """Execute a subprocess command while logging its purpose."""
    message = f"=== {description} ==="
    if progress is not None:
        progress.write(message)
    else:
        print(f"\n{message}")
    subprocess.run(command, check=True)


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
    parser.add_argument(
        "--no-progress",
        action="store_true",
        help="Disable progress bars for non-interactive environments.",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> None:
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
        download_flags = [
            "--requests-per-second",
            str(args.requests_per_second),
        ]
        if args.sec_name:
            download_flags.extend(["--sec-name", args.sec_name])
        if args.sec_email:
            download_flags.extend(["--sec-email", args.sec_email])
        if args.no_progress:
            download_flags.append("--no-progress")
        run_step(
            "Downloading EDGAR master index",
            [sys.executable, "dl_idx.py", *download_flags],
        )

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
        download_flags = [
            "--requests-per-second",
            str(args.requests_per_second),
        ]
        if args.sec_name:
            download_flags.extend(["--sec-name", args.sec_name])
        if args.sec_email:
            download_flags.extend(["--sec-email", args.sec_email])
        if args.no_progress:
            download_flags.append("--no-progress")
        download_progress = ProgressIterator(
            form_entries,
            desc="Downloading filings",
            total=len(form_entries),
            enabled=not args.no_progress,
        )
        for form, folder, _ in download_progress:
            run_step(
                f"Downloading {form} filings",
                [sys.executable, "dl.py", form, folder, *download_flags],
                progress=download_progress,
            )
        download_progress.close()

    if args.skip_parse:
        print("Skipping CUSIP parsing.")
        for csv_path in csv_paths:
            if not Path(csv_path).exists():
                raise FileNotFoundError(
                    f"Expected CSV {csv_path} not found. Remove --skip-parse or generate it first."
                )
    else:
        parse_flags: List[str] = []
        if args.no_progress:
            parse_flags.append("--no-progress")
        parse_progress = ProgressIterator(
            form_entries,
            desc="Parsing filings",
            total=len(form_entries),
            enabled=not args.no_progress,
        )
        for form, folder, _ in parse_progress:
            run_step(
                f"Parsing CUSIPs from {form} filings",
                [sys.executable, "parse_cusip.py", folder, *parse_flags],
                progress=parse_progress,
            )
        parse_progress.close()

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
