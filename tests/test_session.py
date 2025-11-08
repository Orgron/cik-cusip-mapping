"""Tests for session module."""

import pytest
import requests

from cik_cusip.session import create_session


class TestCreateSession:
    """Test create_session function."""

    def test_create_session_returns_session(self):
        """Test that create_session returns a requests.Session."""
        session = create_session("Test User", "test@example.com")
        assert isinstance(session, requests.Session)

    def test_session_has_correct_headers(self):
        """Test that session has correct SEC headers."""
        session = create_session("Jane Doe", "jane@example.com")
        assert "User-Agent" in session.headers
        assert "CIK-CUSIP-Mapping/2.0" in session.headers["User-Agent"]
        assert "Jane Doe" in session.headers["User-Agent"]
        assert "jane@example.com" in session.headers["User-Agent"]
        assert session.headers["From"] == "jane@example.com"

    def test_session_has_retry_adapter(self):
        """Test that session has retry adapter configured."""
        session = create_session("Test", "test@example.com")
        # Check that https adapter is configured
        adapter = session.get_adapter("https://www.sec.gov")
        assert adapter is not None
        assert adapter.max_retries.total == 5
