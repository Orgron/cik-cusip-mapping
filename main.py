#!/usr/bin/env python3
"""
Simple SEC 13D/13G CUSIP Extractor

Downloads SEC index, extracts CUSIP identifiers from 13D and 13G forms,
and writes results to CSV. Respects SEC rate limits and authorization requirements.
"""

import csv
import os
import re
import time
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Optional
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class RateLimiter:
    """Token bucket rate limiter for SEC API requests."""

    def __init__(self, requests_per_second: float = 10.0):
        self.rate = requests_per_second
        self.tokens = requests_per_second
        self.max_tokens = requests_per_second
        self.last_update = time.time()
        self.lock = Lock()

    def acquire(self):
        """Block until a request token is available."""
        with self.lock:
            while True:
                now = time.time()
                elapsed = now - self.last_update
                self.tokens = min(self.max_tokens, self.tokens + elapsed * self.rate)
                self.last_update = now

                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return

                sleep_time = (1.0 - self.tokens) / self.rate
                time.sleep(sleep_time)


def create_session(sec_name: str, sec_email: str) -> requests.Session:
    """
    Create a requests session with proper SEC headers and retry logic.

    Args:
        sec_name: Your name for SEC User-Agent header
        sec_email: Your email for SEC User-Agent header

    Returns:
        Configured requests.Session
    """
    session = requests.Session()

    # Configure retries with exponential backoff
    retry_strategy = Retry(
        total=5,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    # Set SEC-compliant headers
    session.headers.update({
        "User-Agent": f"CIK-CUSIP-Mapping/2.0 {sec_name} {sec_email}",
        "From": sec_email,
    })

    return session


def download_index(output_path: str, session: requests.Session, skip_if_exists: bool = True) -> str:
    """
    Download SEC master index file.

    Args:
        output_path: Path to save the index file
        session: Requests session with SEC headers
        skip_if_exists: If True, skip download if file already exists

    Returns:
        Path to the downloaded index file
    """
    if skip_if_exists and os.path.exists(output_path):
        print(f"Index already exists at {output_path}, skipping download")
        return output_path

    # Download the current quarter's master index
    # You can modify this to download multiple quarters if needed
    current_year = datetime.now().year
    current_quarter = (datetime.now().month - 1) // 3 + 1

    url = f"https://www.sec.gov/Archives/edgar/full-index/{current_year}/QTR{current_quarter}/master.idx"

    print(f"Downloading index from {url}...")
    response = session.get(url)
    response.raise_for_status()

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(response.text)

    print(f"Index downloaded to {output_path}")
    return output_path


def parse_index(index_path: str, forms: tuple = ("13D", "13G")) -> list[dict]:
    """
    Parse SEC index file and extract entries for specified forms.

    Args:
        index_path: Path to the index file
        forms: Tuple of form types to extract (e.g., ("13D", "13G"))

    Returns:
        List of dicts with keys: cik, company_name, form, date, url
    """
    entries = []

    with open(index_path, 'r', encoding='utf-8') as f:
        # Skip header lines (first 11 lines are header)
        for _ in range(11):
            next(f, None)

        for line in f:
            line = line.strip()
            if not line:
                continue

            # Parse fixed-width format: CIK|Company Name|Form Type|Date Filed|Filename
            parts = line.split('|')
            if len(parts) != 5:
                continue

            cik, company_name, form_type, date, filename = parts

            # Normalize form type (remove SC prefix and /A suffix for matching)
            normalized_form = form_type.replace('SC ', '').split('/')[0].strip()

            if normalized_form in forms:
                entries.append({
                    'cik': cik.strip(),
                    'company_name': company_name.strip(),
                    'form': form_type.strip(),
                    'date': date.strip(),
                    'url': f"https://www.sec.gov/Archives/{filename.strip()}",
                })

    return entries


def extract_cusip(text: str) -> Optional[str]:
    """
    Extract CUSIP identifier from SEC filing text.

    Uses a window-based approach looking for explicit CUSIP markers,
    with fallback to document-wide search.

    Args:
        text: Raw filing text

    Returns:
        CUSIP string if found, None otherwise
    """
    # Clean HTML entities and tags
    text = re.sub(r'&[a-z]+;', ' ', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', ' ', text)

    # CUSIP pattern: 8-10 alphanumeric characters with at least 5 digits
    cusip_pattern = r'\b[A-Z0-9]{8,10}\b'

    # Window method: Look for explicit CUSIP markers
    cusip_markers = [
        r'CUSIP\s+(?:NO\.?|NUMBER|#)',
        r'CUSIP:',
        r'\bCUSIP\b',
    ]

    for marker in cusip_markers:
        matches = list(re.finditer(marker, text, re.IGNORECASE))
        if not matches:
            continue

        # Get context around the marker
        for match in matches:
            start = max(0, match.start() - 500)
            end = min(len(text), match.end() + 500)
            window = text[start:end]

            # Find CUSIP candidates in window
            candidates = re.findall(cusip_pattern, window)

            for candidate in candidates:
                if is_valid_cusip(candidate):
                    return candidate

    # Fallback: Search entire document
    candidates = re.findall(cusip_pattern, text)

    # Score candidates
    scored = []
    for candidate in candidates:
        if not is_valid_cusip(candidate):
            continue

        score = 0
        # Prefer candidates with letters (more specific than all digits)
        if re.search(r'[A-Z]', candidate):
            score += 10
        # Prefer 9-character CUSIPs
        if len(candidate) == 9:
            score += 5

        scored.append((score, candidate))

    if scored:
        scored.sort(reverse=True)
        return scored[0][1]

    return None


def is_valid_cusip(candidate: str) -> bool:
    """
    Validate if a candidate string is likely a CUSIP.

    Args:
        candidate: String to validate

    Returns:
        True if likely a valid CUSIP
    """
    # Length check
    if len(candidate) < 8 or len(candidate) > 10:
        return False

    # Must be alphanumeric only
    if not candidate.isalnum():
        return False

    # Must have at least 5 digits (CUSIPs are not all letters)
    digit_count = sum(1 for c in candidate if c.isdigit())
    if digit_count < 5:
        return False

    # Exclude common false positives
    false_positives = [
        r'^0+$',  # All zeros
        r'^\d{5}-?\d{4}$',  # Zip codes
        r'FILE',  # Filename patterns
        r'PAGE',
        r'TABLE',
    ]

    for pattern in false_positives:
        if re.match(pattern, candidate):
            return False

    return True


def process_filings(
    index_path: str,
    output_csv: str,
    forms: tuple = ("13D", "13G"),
    sec_name: str = None,
    sec_email: str = None,
    requests_per_second: float = 10.0,
    skip_index_download: bool = False,
):
    """
    Main function to process SEC filings and extract CUSIPs.

    Args:
        index_path: Path to save/load the index file
        output_csv: Path to save the results CSV
        forms: Tuple of form types to process (default: ("13D", "13G"))
        sec_name: Your name for SEC User-Agent (or set SEC_NAME env var)
        sec_email: Your email for SEC headers (or set SEC_EMAIL env var)
        requests_per_second: Rate limit for SEC requests (default: 10)
        skip_index_download: If True, skip downloading index if it exists
    """
    # Get SEC credentials from env vars if not provided
    sec_name = sec_name or os.environ.get('SEC_NAME')
    sec_email = sec_email or os.environ.get('SEC_EMAIL')

    if not sec_name or not sec_email:
        raise ValueError(
            "SEC credentials required. Provide sec_name and sec_email, "
            "or set SEC_NAME and SEC_EMAIL environment variables."
        )

    # Create session and rate limiter
    session = create_session(sec_name, sec_email)
    rate_limiter = RateLimiter(requests_per_second)

    try:
        # Step 1: Download index
        if not skip_index_download or not os.path.exists(index_path):
            download_index(index_path, session, skip_if_exists=skip_index_download)

        # Step 2: Parse index for target forms
        print(f"\nParsing index for forms: {forms}")
        entries = parse_index(index_path, forms)
        print(f"Found {len(entries)} filings to process")

        # Step 3: Process each filing
        results = []

        os.makedirs(os.path.dirname(output_csv), exist_ok=True)

        print(f"\nProcessing filings (rate limited to {requests_per_second} req/s)...")
        for i, entry in enumerate(entries, 1):
            # Rate limit
            rate_limiter.acquire()

            try:
                # Download filing
                response = session.get(entry['url'])
                response.raise_for_status()

                # Parse CUSIP from filing text
                cusip = extract_cusip(response.text)

                if cusip:
                    results.append({
                        'cik': entry['cik'],
                        'company_name': entry['company_name'],
                        'form': entry['form'],
                        'date': entry['date'],
                        'cusip': cusip,
                    })
                    print(f"[{i}/{len(entries)}] {entry['cik']} - {entry['form']} - CUSIP: {cusip}")
                else:
                    print(f"[{i}/{len(entries)}] {entry['cik']} - {entry['form']} - No CUSIP found")

            except Exception as e:
                print(f"[{i}/{len(entries)}] {entry['cik']} - {entry['form']} - Error: {e}")
                continue

        # Step 4: Write results to CSV
        print(f"\nWriting {len(results)} results to {output_csv}")
        with open(output_csv, 'w', newline='', encoding='utf-8') as f:
            if results:
                writer = csv.DictWriter(f, fieldnames=['cik', 'company_name', 'form', 'date', 'cusip'])
                writer.writeheader()
                writer.writerows(results)

        print(f"✓ Complete! Extracted {len(results)} CUSIPs from {len(entries)} filings")
        print(f"  Success rate: {len(results)/len(entries)*100:.1f}%")

    finally:
        session.close()


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Extract CUSIPs from SEC 13D/13G filings')
    parser.add_argument('--index', default='data/master.idx', help='Path to index file')
    parser.add_argument('--output', default='data/cusips.csv', help='Path to output CSV')
    parser.add_argument('--skip-index', action='store_true', help='Skip index download if exists')
    parser.add_argument('--sec-name', help='Your name for SEC User-Agent')
    parser.add_argument('--sec-email', help='Your email for SEC headers')
    parser.add_argument('--rate', type=float, default=10.0, help='Requests per second (default: 10)')

    args = parser.parse_args()

    process_filings(
        index_path=args.index,
        output_csv=args.output,
        forms=("13D", "13G"),
        sec_name=args.sec_name,
        sec_email=args.sec_email,
        requests_per_second=args.rate,
        skip_index_download=args.skip_index,
    )
