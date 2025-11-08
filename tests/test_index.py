"""Tests for index module."""

import os
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
import requests

from cik_cusip.index import (
    download_index,
    download_indices,
    extract_accession_number,
    parse_index,
)


class TestExtractAccessionNumber:
    """Test extract_accession_number function."""

    def test_extract_from_standard_url(self):
        """Test extraction from standard SEC URL."""
        url = "https://www.sec.gov/Archives/edgar/data/1234567/0001234567-12-000001.txt"
        result = extract_accession_number(url)
        assert result == "0001234567-12-000001"

    def test_extract_from_url_with_path(self):
        """Test extraction from URL with additional path components."""
        url = "https://www.sec.gov/Archives/edgar/data/789/0000000789-23-001234.txt"
        result = extract_accession_number(url)
        assert result == "0000000789-23-001234"

    def test_no_accession_in_url(self):
        """Test that None is returned when no accession number found."""
        url = "https://www.sec.gov/Archives/edgar/data/1234567/index.html"
        result = extract_accession_number(url)
        assert result is None

    def test_malformed_url(self):
        """Test handling of malformed URLs."""
        url = "not-a-valid-url"
        result = extract_accession_number(url)
        assert result is None


class TestDownloadIndex:
    """Test download_index function."""

    @patch("cik_cusip.index.requests.Session")
    def test_download_index_success(self, mock_session_class):
        """Test successful index download."""
        mock_session = Mock()
        mock_response = Mock()
        mock_response.text = "Index content\nline2\nline3"
        mock_response.raise_for_status = Mock()
        mock_session.get.return_value = mock_response

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = os.path.join(tmpdir, "test.idx")
            result = download_index(output_path, mock_session, 2024, 1, skip_if_exists=False)

            assert result == output_path
            assert os.path.exists(output_path)
            with open(output_path) as f:
                content = f.read()
                assert content == "Index content\nline2\nline3"

    @patch("cik_cusip.index.requests.Session")
    def test_download_index_skip_existing(self, mock_session_class):
        """Test that existing index is skipped."""
        mock_session = Mock()

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = os.path.join(tmpdir, "test.idx")
            # Create existing file
            with open(output_path, "w") as f:
                f.write("existing content")

            result = download_index(output_path, mock_session, 2024, 1, skip_if_exists=True)

            assert result == output_path
            # Should not have called session.get
            mock_session.get.assert_not_called()
            # Content should be unchanged
            with open(output_path) as f:
                assert f.read() == "existing content"

    @patch("cik_cusip.index.requests.Session")
    def test_download_index_404(self, mock_session_class):
        """Test handling of 404 response."""
        mock_session = Mock()
        mock_response = Mock()
        mock_response.status_code = 404
        http_error = requests.HTTPError()
        http_error.response = mock_response
        mock_session.get.side_effect = http_error

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = os.path.join(tmpdir, "test.idx")
            result = download_index(output_path, mock_session, 2024, 1, skip_if_exists=False)

            assert result is None


class TestParseIndex:
    """Test parse_index function."""

    def test_parse_index_basic(self):
        """Test basic index parsing."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".idx") as f:
            # Write header (11 lines)
            for i in range(11):
                f.write(f"Header line {i}\n")
            # Write valid entries
            f.write("1234567|ACME CORP|13D|2024-01-15|edgar/data/1234567/0001234567-24-000001.txt\n")
            f.write("7654321|XYZ INC|SC 13G|2024-01-16|edgar/data/7654321/0007654321-24-000002.txt\n")
            f.write("9999999|OTHER CO|8-K|2024-01-17|edgar/data/9999999/0009999999-24-000003.txt\n")
            temp_path = f.name

        try:
            entries = parse_index(temp_path, forms=("13D", "13G"))

            assert len(entries) == 2
            assert entries[0]["cik"] == "1234567"
            assert entries[0]["company_name"] == "ACME CORP"
            assert entries[0]["form"] == "13D"
            assert entries[0]["date"] == "2024-01-15"
            assert "0001234567-24-000001" in entries[0]["url"]
            assert entries[0]["accession_number"] == "0001234567-24-000001"

            assert entries[1]["cik"] == "7654321"
            assert entries[1]["form"] == "SC 13G"
            assert entries[1]["accession_number"] == "0007654321-24-000002"
        finally:
            Path(temp_path).unlink()

    def test_parse_index_amended_forms(self):
        """Test parsing amended forms (13D/A, 13G/A)."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".idx") as f:
            for i in range(11):
                f.write(f"Header line {i}\n")
            f.write("1234567|ACME CORP|SC 13D/A|2024-01-15|edgar/data/1234567/0001234567-24-000001.txt\n")
            f.write("7654321|XYZ INC|13G/A|2024-01-16|edgar/data/7654321/0007654321-24-000002.txt\n")
            temp_path = f.name

        try:
            entries = parse_index(temp_path, forms=("13D", "13G"))

            # Should match both amended forms
            assert len(entries) == 2
            assert entries[0]["form"] == "SC 13D/A"
            assert entries[1]["form"] == "13G/A"
        finally:
            Path(temp_path).unlink()

    def test_parse_index_empty(self):
        """Test parsing index with no matching entries."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".idx") as f:
            for i in range(11):
                f.write(f"Header line {i}\n")
            f.write("1234567|ACME CORP|10-K|2024-01-15|edgar/data/1234567/0001234567-24-000001.txt\n")
            temp_path = f.name

        try:
            entries = parse_index(temp_path, forms=("13D", "13G"))
            assert len(entries) == 0
        finally:
            Path(temp_path).unlink()


class TestDownloadIndices:
    """Test download_indices function."""

    @patch("cik_cusip.index.download_index")
    @patch("cik_cusip.index.time.sleep")
    def test_download_indices_range(self, mock_sleep, mock_download):
        """Test downloading indices for a year range."""
        mock_download.return_value = "/tmp/index.idx"
        mock_session = Mock()

        result = download_indices(
            "/tmp/indices",
            mock_session,
            start_year=2023,
            start_quarter=3,
            end_year=2024,
            end_quarter=1,
            skip_if_exists=True,
        )

        # Should download 2023 Q3, Q4, 2024 Q1 = 3 indices
        assert mock_download.call_count == 3
        assert len(result) == 3
