"""Rate limiting for SEC API requests."""

import time
from threading import Lock


class RateLimiter:
    """Token bucket rate limiter for SEC API requests."""

    def __init__(self, requests_per_second: float = 10.0):
        """
        Initialize rate limiter.

        Args:
            requests_per_second: Maximum number of requests per second
        """
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
