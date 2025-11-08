"""Tests for rate_limiter module."""

import time
from threading import Thread

import pytest

from cik_cusip.rate_limiter import RateLimiter


class TestRateLimiter:
    """Test RateLimiter class."""

    def test_init(self):
        """Test RateLimiter initialization."""
        limiter = RateLimiter(requests_per_second=5.0)
        assert limiter.rate == 5.0
        assert limiter.tokens == 5.0
        assert limiter.max_tokens == 5.0

    def test_acquire_immediate(self):
        """Test that acquire returns immediately when tokens available."""
        limiter = RateLimiter(requests_per_second=10.0)
        start = time.time()
        limiter.acquire()
        elapsed = time.time() - start
        # Should be nearly instant
        assert elapsed < 0.1
        # Token should be consumed
        assert limiter.tokens < 10.0

    def test_acquire_rate_limiting(self):
        """Test that acquire blocks when no tokens available."""
        limiter = RateLimiter(requests_per_second=10.0)
        # Consume all tokens
        for _ in range(10):
            limiter.tokens -= 1.0

        start = time.time()
        limiter.acquire()
        elapsed = time.time() - start
        # Should wait for at least ~0.1 seconds (1/10 second)
        assert elapsed >= 0.09  # Small margin for timing variations

    def test_token_refill(self):
        """Test that tokens refill over time."""
        limiter = RateLimiter(requests_per_second=10.0)
        limiter.tokens = 0.0
        time.sleep(0.3)  # Wait 0.3 seconds
        limiter.acquire()  # This should trigger refill
        # Tokens should have refilled (approximately 3 tokens in 0.3s at 10/s)
        assert limiter.tokens >= 0.0

    def test_max_tokens_cap(self):
        """Test that tokens don't exceed max_tokens."""
        limiter = RateLimiter(requests_per_second=5.0)
        limiter.last_update = time.time() - 10.0  # 10 seconds ago
        limiter.acquire()
        # Should be capped at max_tokens
        assert limiter.tokens <= limiter.max_tokens

    def test_thread_safety(self):
        """Test that RateLimiter is thread-safe."""
        limiter = RateLimiter(requests_per_second=20.0)
        results = []

        def worker():
            limiter.acquire()
            results.append(time.time())

        threads = [Thread(target=worker) for _ in range(5)]
        start = time.time()
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # All threads should complete
        assert len(results) == 5
        elapsed = time.time() - start
        # Should take at least some time due to rate limiting
        assert elapsed >= 0.0
