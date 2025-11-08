"""SEC EDGAR session management."""

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


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
    session.headers.update(
        {
            "User-Agent": f"CIK-CUSIP-Mapping/2.0 {sec_name} {sec_email}",
            "From": sec_email,
        }
    )

    return session
