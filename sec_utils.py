"""Shared utilities for interacting with the SEC EDGAR endpoints."""

from __future__ import annotations

import time
from typing import Dict, Optional


class RateLimiter:
    """Simple token bucket limiting requests per second."""

    def __init__(self, requests_per_second: float) -> None:
        if requests_per_second <= 0:
            raise ValueError("requests_per_second must be greater than zero")
        self._min_interval = 1.0 / requests_per_second
        self._last_request: Optional[float] = None

    def wait(self) -> None:
        """Sleep until another request is permitted."""
        now = time.perf_counter()
        if self._last_request is None:
            self._last_request = now
            return

        elapsed = now - self._last_request
        sleep_for = self._min_interval - elapsed
        if sleep_for > 0:
            time.sleep(sleep_for)
            now = time.perf_counter()
        self._last_request = now


def build_request_headers(name: Optional[str], email: Optional[str]) -> Dict[str, str]:
    """Construct SEC-compliant request headers using optional identifiers."""
    identifier_parts = ["CIK-CUSIP-Mapping/1.0"]
    if name:
        identifier_parts.append(name.strip())
    if email:
        identifier_parts.append(email.strip())

    headers: Dict[str, str] = {"User-Agent": " ".join(identifier_parts)}
    if email:
        headers["From"] = email.strip()
    return headers
