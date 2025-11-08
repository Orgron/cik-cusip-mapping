"""SEC EDGAR index download and parsing."""

import os
import re
import time
from datetime import datetime
from typing import Optional

import requests


def download_index(
    output_path: str,
    session: requests.Session,
    year: int,
    quarter: int,
    skip_if_exists: bool = True,
) -> Optional[str]:
    """
    Download SEC master index file for a specific year and quarter.

    Args:
        output_path: Path to save the index file
        session: Requests session with SEC headers
        year: Year to download (e.g., 2024)
        quarter: Quarter to download (1-4)
        skip_if_exists: If True, skip download if file already exists

    Returns:
        Path to the downloaded index file, or None if failed
    """
    if skip_if_exists and os.path.exists(output_path):
        print(f"Index already exists at {output_path}, skipping download")
        return output_path

    url = (
        f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{quarter}/master.idx"
    )

    print(f"Downloading index from {url}...")
    try:
        response = session.get(url)
        response.raise_for_status()

        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(response.text)

        print(f"Index downloaded to {output_path}")
        return output_path
    except requests.HTTPError as e:
        if e.response.status_code == 404:
            print(f"Index not found for {year} Q{quarter} (404)")
            return None
        raise


def download_indices(
    output_dir: str,
    session: requests.Session,
    start_year: int = None,
    start_quarter: int = 1,
    end_year: int = None,
    end_quarter: int = None,
    skip_if_exists: bool = True,
) -> list[str]:
    """
    Download multiple SEC master index files for a range of years/quarters.

    Args:
        output_dir: Directory to save index files
        session: Requests session with SEC headers
        start_year: Starting year (default: 1993)
        start_quarter: Starting quarter (1-4, default: 1)
        end_year: Ending year (default: current year)
        end_quarter: Ending quarter (default: current quarter)
        skip_if_exists: If True, skip download if file already exists

    Returns:
        List of paths to downloaded index files
    """
    # Default to all available indices (1993 to current)
    if start_year is None:
        start_year = 1993

    if end_year is None:
        end_year = datetime.now().year
        end_quarter = (datetime.now().month - 1) // 3 + 1
    elif end_quarter is None:
        end_quarter = 4

    print(
        f"\nDownloading indices from {start_year} Q{start_quarter} to {end_year} Q{end_quarter}..."
    )

    index_paths = []
    os.makedirs(output_dir, exist_ok=True)

    for year in range(start_year, end_year + 1):
        for quarter in range(1, 5):
            # Skip quarters before start
            if year == start_year and quarter < start_quarter:
                continue
            # Skip quarters after end
            if year == end_year and quarter > end_quarter:
                continue

            output_path = os.path.join(output_dir, f"master_{year}_Q{quarter}.idx")

            result = download_index(output_path, session, year, quarter, skip_if_exists)
            if result:
                index_paths.append(result)

            # Small delay between downloads to be respectful
            if not (skip_if_exists and os.path.exists(output_path)):
                time.sleep(0.1)

    print(f"Downloaded {len(index_paths)} index files")
    return index_paths


def parse_index(index_path: str, forms: tuple = ("13D", "13G")) -> list[dict]:
    """
    Parse SEC index file and extract entries for specified forms.

    Args:
        index_path: Path to the index file
        forms: Tuple of form types to extract (e.g., ("13D", "13G"))

    Returns:
        List of dicts with keys: cik, company_name, form, date, url, accession_number
    """
    entries = []

    with open(index_path, "r", encoding="utf-8") as f:
        # Skip header lines (first 11 lines are header)
        for _ in range(11):
            next(f, None)

        for line in f:
            line = line.strip()
            if not line:
                continue

            # Parse fixed-width format: CIK|Company Name|Form Type|Date Filed|Filename
            parts = line.split("|")
            if len(parts) != 5:
                continue

            cik, company_name, form_type, date, filename = parts

            # Normalize form type (remove SC prefix and /A suffix for matching)
            normalized_form = form_type.replace("SC ", "").split("/")[0].strip()

            if normalized_form in forms:
                url = f"https://www.sec.gov/Archives/{filename.strip()}"
                accession_number = extract_accession_number(url)

                entries.append(
                    {
                        "cik": cik.strip(),
                        "company_name": company_name.strip(),
                        "form": form_type.strip(),
                        "date": date.strip(),
                        "url": url,
                        "accession_number": accession_number,
                    }
                )

    return entries


def extract_accession_number(url: str) -> Optional[str]:
    """
    Extract accession number from SEC filing URL.

    Args:
        url: SEC filing URL (e.g., https://www.sec.gov/Archives/edgar/data/1234567/0001234567-12-000001.txt)

    Returns:
        Accession number (e.g., 0001234567-12-000001) or None if not found
    """
    # Pattern for accession number: NNNNNNNNNN-NN-NNNNNN
    # Example: 0001234567-12-000001
    match = re.search(r'(\d{10}-\d{2}-\d{6})', url)
    if match:
        return match.group(1)
    return None
