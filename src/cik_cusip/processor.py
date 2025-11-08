"""Main processing orchestration for CUSIP extraction."""

import csv
import os
import time

from .cusip import extract_cusip
from .index import download_indices, parse_index
from .rate_limiter import RateLimiter
from .session import create_session
from .utils import load_cik_filter


def process_filings(
    index_dir: str,
    output_csv: str,
    forms: tuple = ("13D", "13G"),
    sec_name: str = None,
    sec_email: str = None,
    requests_per_second: float = 10.0,
    skip_index_download: bool = False,
    start_year: int = None,
    start_quarter: int = 1,
    end_year: int = None,
    end_quarter: int = None,
    cik_filter_file: str = None,
):
    """
    Main function to process SEC filings and extract CUSIPs.

    Args:
        index_dir: Directory to save/load index files
        output_csv: Path to save the results CSV
        forms: Tuple of form types to process (default: ("13D", "13G"))
        sec_name: Your name for SEC User-Agent (or set SEC_NAME env var)
        sec_email: Your email for SEC headers (or set SEC_EMAIL env var)
        requests_per_second: Rate limit for SEC requests (default: 10)
        skip_index_download: If True, skip downloading indices if they exist
        start_year: Starting year for indices (default: 1993)
        start_quarter: Starting quarter (1-4, default: 1)
        end_year: Ending year for indices (default: current year)
        end_quarter: Ending quarter (default: current quarter)
        cik_filter_file: Optional path to text file with CIKs to filter (one per line)
    """
    # Get SEC credentials from env vars if not provided
    sec_name = sec_name or os.environ.get("SEC_NAME")
    sec_email = sec_email or os.environ.get("SEC_EMAIL")

    if not sec_name or not sec_email:
        raise ValueError(
            "SEC credentials required. Provide sec_name and sec_email, "
            "or set SEC_NAME and SEC_EMAIL environment variables."
        )

    # Load CIK filter if provided
    cik_filter = None
    if cik_filter_file:
        cik_filter = load_cik_filter(cik_filter_file)
        print(f"Loaded {len(cik_filter)} CIKs from filter file: {cik_filter_file}")

    # Create session and rate limiter
    session = create_session(sec_name, sec_email)
    rate_limiter = RateLimiter(requests_per_second)

    try:
        # Step 1: Download indices
        index_paths = download_indices(
            index_dir,
            session,
            start_year=start_year,
            start_quarter=start_quarter,
            end_year=end_year,
            end_quarter=end_quarter,
            skip_if_exists=skip_index_download,
        )

        if not index_paths:
            print("No index files found or downloaded!")
            return

        # Step 2: Parse all indices for target forms
        print(f"\nParsing {len(index_paths)} indices for forms: {forms}")
        entries = []
        for index_path in index_paths:
            index_entries = parse_index(index_path, forms)
            entries.extend(index_entries)
        print(f"Found {len(entries)} filings to process across all indices")

        # Step 2.5: Apply CIK filter if provided
        if cik_filter:
            original_count = len(entries)
            entries = [entry for entry in entries if entry["cik"] in cik_filter]
            print(
                f"Filtered to {len(entries)} filings matching {len(cik_filter)} CIKs (removed {original_count - len(entries)} filings)"
            )

        # Step 3: Process each filing
        results = []
        failed_count = 0
        start_time = time.time()

        os.makedirs(os.path.dirname(output_csv), exist_ok=True)

        print(f"\nProcessing filings (rate limited to {requests_per_second} req/s)...")
        print()  # Empty line for progress bar

        for i, entry in enumerate(entries, 1):
            # Rate limit
            rate_limiter.acquire()

            try:
                # Download filing
                response = session.get(entry["url"])
                response.raise_for_status()

                # Parse CUSIP from filing text
                cusip = extract_cusip(response.text)

                if cusip:
                    results.append(
                        {
                            "cik": entry["cik"],
                            "company_name": entry["company_name"],
                            "form": entry["form"],
                            "date": entry["date"],
                            "cusip": cusip,
                            "accession_number": entry["accession_number"],
                        }
                    )
                    status = f"✓ CUSIP: {cusip}"
                else:
                    failed_count += 1
                    status = "✗ No CUSIP found"

            except Exception as e:
                failed_count += 1
                status = f"✗ Error: {str(e)[:30]}"

            # Calculate progress and ETA
            elapsed = time.time() - start_time
            progress_pct = (i / len(entries)) * 100
            if i > 0:
                avg_time_per_filing = elapsed / i
                remaining = len(entries) - i
                eta_seconds = avg_time_per_filing * remaining
                eta_str = f"{int(eta_seconds // 60)}m {int(eta_seconds % 60)}s"
            else:
                eta_str = "calculating..."

            # Format company name (truncate if too long)
            company = entry["company_name"][:30]

            # Print progress bar (overwrite previous line)
            progress_line = (
                f"\r[{i}/{len(entries)}] {progress_pct:5.1f}% | "
                f"ETA: {eta_str:>8} | "
                f"Success: {len(results)} | Failed: {failed_count} | "
                f"Latest: {company} - {status}"
            )
            print(progress_line, end="", flush=True)

        # Print newline after progress bar completes
        print()

        # Step 4: Write results to CSV
        print(f"\nWriting {len(results)} results to {output_csv}")
        with open(output_csv, "w", newline="", encoding="utf-8") as f:
            if results:
                writer = csv.DictWriter(
                    f,
                    fieldnames=[
                        "cik",
                        "company_name",
                        "form",
                        "date",
                        "cusip",
                        "accession_number",
                    ],
                )
                writer.writeheader()
                writer.writerows(results)

        print(
            f"✓ Complete! Extracted {len(results)} CUSIPs from {len(entries)} filings"
        )
        if len(entries) > 0:
            print(
                f"  Success: {len(results)} | Failed: {failed_count} | Success rate: {len(results) / len(entries) * 100:.1f}%"
            )

    finally:
        session.close()


def download_filing_txt(
    accession_number: str,
    output_path: str,
    cik: str = None,
    sec_name: str = None,
    sec_email: str = None,
) -> str:
    """
    Download a filing in text format given its accession number and CIK.

    Args:
        accession_number: SEC accession number (e.g., 0001234567-12-000001)
        output_path: Path to save the downloaded filing
        cik: CIK number (required for downloading)
        sec_name: Your name for SEC User-Agent (or set SEC_NAME env var)
        sec_email: Your email for SEC headers (or set SEC_EMAIL env var)

    Returns:
        Path to the downloaded file
    """
    # Get SEC credentials from env vars if not provided
    sec_name = sec_name or os.environ.get("SEC_NAME")
    sec_email = sec_email or os.environ.get("SEC_EMAIL")

    if not sec_name or not sec_email:
        raise ValueError(
            "SEC credentials required. Provide sec_name and sec_email, "
            "or set SEC_NAME and SEC_EMAIL environment variables."
        )

    if not cik:
        raise ValueError(
            "CIK is required to download a filing. "
            "Provide the CIK parameter."
        )

    # Construct URL
    # Format: https://www.sec.gov/Archives/edgar/data/{cik}/{accession_number}.txt
    url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_number}.txt"

    # Create session
    session = create_session(sec_name, sec_email)

    try:
        print(f"Downloading filing from {url}...")
        response = session.get(url)
        response.raise_for_status()

        # Create output directory if needed
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

        # Write to file
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(response.text)

        print(f"Filing saved to {output_path}")
        return output_path

    finally:
        session.close()
