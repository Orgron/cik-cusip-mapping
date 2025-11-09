"""Tests for processor module."""

import csv
import os
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from cik_cusip.processor import download_filing_txt, process_filings


class TestDownloadFilingTxt:
    """Test download_filing_txt function."""

    @patch("cik_cusip.processor.create_session")
    def test_download_filing_txt_success(self, mock_create_session):
        """Test successful filing download."""
        mock_session = Mock()
        mock_response = Mock()
        mock_response.text = "Filing content here"
        mock_response.raise_for_status = Mock()
        mock_session.get.return_value = mock_response
        mock_create_session.return_value = mock_session

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = os.path.join(tmpdir, "filing.txt")
            result = download_filing_txt(
                accession_number="0001234567-12-000001",
                output_path=output_path,
                cik="813828",
                sec_name="Test",
                sec_email="test@example.com",
            )

            assert result == output_path
            assert os.path.exists(output_path)
            with open(output_path) as f:
                assert f.read() == "Filing content here"

            # Verify correct URL was called
            expected_url = "https://www.sec.gov/Archives/edgar/data/813828/0001234567-12-000001.txt"
            mock_session.get.assert_called_once()
            actual_url = mock_session.get.call_args[0][0]
            assert actual_url == expected_url

    @patch("cik_cusip.processor.create_session")
    def test_download_filing_txt_no_credentials(self, mock_create_session):
        """Test that error is raised when credentials missing."""
        with pytest.raises(ValueError, match="SEC credentials required"):
            download_filing_txt(
                accession_number="0001234567-12-000001",
                output_path="output.txt",
                cik="813828",
            )

    @patch("cik_cusip.processor.create_session")
    def test_download_filing_txt_env_vars(self, mock_create_session):
        """Test using environment variables for credentials."""
        mock_session = Mock()
        mock_response = Mock()
        mock_response.text = "Content"
        mock_response.raise_for_status = Mock()
        mock_session.get.return_value = mock_response
        mock_create_session.return_value = mock_session

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.dict(os.environ, {"SEC_NAME": "EnvName", "SEC_EMAIL": "env@example.com"}):
                output_path = os.path.join(tmpdir, "filing.txt")
                result = download_filing_txt(
                    accession_number="0001234567-12-000001",
                    output_path=output_path,
                    cik="813828",
                )

                assert result == output_path
                mock_create_session.assert_called_once_with("EnvName", "env@example.com")

    def test_download_filing_txt_no_cik(self):
        """Test that error is raised when CIK is missing."""
        with pytest.raises(ValueError, match="CIK is required"):
            download_filing_txt(
                accession_number="0001234567-12-000001",
                output_path="output.txt",
                sec_name="Test",
                sec_email="test@example.com",
            )


class TestProcessFilings:
    """Test process_filings function."""

    @patch("cik_cusip.processor.download_indices")
    @patch("cik_cusip.processor.parse_index")
    @patch("cik_cusip.processor.create_session")
    @patch("cik_cusip.processor.extract_cusip")
    def test_process_filings_basic(
        self, mock_extract, mock_session, mock_parse, mock_download
    ):
        """Test basic filing processing."""
        # Setup mocks
        mock_download.return_value = ["/tmp/index1.idx"]
        mock_parse.return_value = [
            {
                "cik": "1234567",
                "company_name": "ACME CORP",
                "form": "13D",
                "date": "2024-01-15",
                "url": "https://www.sec.gov/test.txt",
                "accession_number": "0001234567-24-000001",
            }
        ]

        mock_session_instance = Mock()
        mock_response = Mock()
        mock_response.text = "Filing text with CUSIP"
        mock_response.raise_for_status = Mock()
        mock_session_instance.get.return_value = mock_response
        mock_session_instance.close = Mock()
        mock_session.return_value = mock_session_instance

        mock_extract.return_value = "68389X105"

        with tempfile.TemporaryDirectory() as tmpdir:
            output_csv = os.path.join(tmpdir, "output.csv")
            index_dir = os.path.join(tmpdir, "indices")

            process_filings(
                index_dir=index_dir,
                output_csv=output_csv,
                sec_name="Test",
                sec_email="test@example.com",
                requests_per_second=100,  # Fast for testing
                skip_index_download=False,
            )

            # Verify CSV was created
            assert os.path.exists(output_csv)

            # Read and verify CSV content
            with open(output_csv, newline="") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                assert len(rows) == 1
                assert rows[0]["cik"] == "1234567"
                assert rows[0]["company_name"] == "ACME CORP"
                assert rows[0]["form"] == "13D"
                assert rows[0]["date"] == "2024-01-15"
                assert rows[0]["cusip"] == "68389X105"
                assert rows[0]["accession_number"] == "0001234567-24-000001"

    @patch("cik_cusip.processor.download_indices")
    def test_process_filings_no_credentials(self, mock_download):
        """Test that error is raised when credentials missing."""
        with pytest.raises(ValueError, match="SEC credentials required"):
            process_filings(
                index_dir="/tmp/indices",
                output_csv="/tmp/output.csv",
            )

    @patch("cik_cusip.processor.download_indices")
    @patch("cik_cusip.processor.parse_index")
    @patch("cik_cusip.processor.create_session")
    @patch("cik_cusip.processor.load_cik_filter")
    @patch("cik_cusip.processor.extract_cusip")
    def test_process_filings_with_cik_filter(
        self, mock_extract, mock_load_filter, mock_session, mock_parse, mock_download
    ):
        """Test filing processing with CIK filter."""
        # Setup mocks
        mock_download.return_value = ["/tmp/index1.idx"]
        mock_parse.return_value = [
            {
                "cik": "1234567",
                "company_name": "ACME CORP",
                "form": "13D",
                "date": "2024-01-15",
                "url": "https://www.sec.gov/test1.txt",
                "accession_number": "0001234567-24-000001",
            },
            {
                "cik": "7654321",
                "company_name": "XYZ INC",
                "form": "13G",
                "date": "2024-01-16",
                "url": "https://www.sec.gov/test2.txt",
                "accession_number": "0007654321-24-000002",
            },
        ]

        mock_load_filter.return_value = {"1234567"}  # Only filter for first CIK

        mock_session_instance = Mock()
        mock_response = Mock()
        mock_response.text = "Filing text"
        mock_response.raise_for_status = Mock()
        mock_session_instance.get.return_value = mock_response
        mock_session_instance.close = Mock()
        mock_session.return_value = mock_session_instance

        mock_extract.return_value = "68389X105"

        with tempfile.TemporaryDirectory() as tmpdir:
            output_csv = os.path.join(tmpdir, "output.csv")
            index_dir = os.path.join(tmpdir, "indices")
            filter_file = os.path.join(tmpdir, "ciks.txt")

            # Create filter file
            with open(filter_file, "w") as f:
                f.write("1234567\n")

            process_filings(
                index_dir=index_dir,
                output_csv=output_csv,
                sec_name="Test",
                sec_email="test@example.com",
                requests_per_second=100,
                cik_filter_file=filter_file,
            )

            # Verify only 1 filing was processed (the filtered one)
            with open(output_csv, newline="") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                assert len(rows) == 1
                assert rows[0]["cik"] == "1234567"

    @patch("cik_cusip.processor.download_indices")
    @patch("cik_cusip.processor.parse_index")
    @patch("cik_cusip.processor.create_session")
    @patch("cik_cusip.processor.extract_cusip")
    def test_process_filings_skip_existing(
        self, mock_extract, mock_session, mock_parse, mock_download
    ):
        """Test filing processing with skip_existing flag."""
        # Setup mocks
        mock_download.return_value = ["/tmp/index1.idx"]
        mock_parse.return_value = [
            {
                "cik": "1234567",
                "company_name": "ACME CORP",
                "form": "13D",
                "date": "2024-01-15",
                "url": "https://www.sec.gov/test1.txt",
                "accession_number": "0001234567-24-000001",
            },
            {
                "cik": "7654321",
                "company_name": "XYZ INC",
                "form": "13G",
                "date": "2024-01-16",
                "url": "https://www.sec.gov/test2.txt",
                "accession_number": "0007654321-24-000002",
            },
        ]

        mock_session_instance = Mock()
        mock_response = Mock()
        mock_response.text = "Filing text with CUSIP"
        mock_response.raise_for_status = Mock()
        mock_session_instance.get.return_value = mock_response
        mock_session_instance.close = Mock()
        mock_session.return_value = mock_session_instance

        mock_extract.return_value = "88888X888"

        with tempfile.TemporaryDirectory() as tmpdir:
            output_csv = os.path.join(tmpdir, "output.csv")
            index_dir = os.path.join(tmpdir, "indices")

            # Create existing CSV with one filing already processed
            os.makedirs(os.path.dirname(output_csv), exist_ok=True)
            with open(output_csv, "w", newline="", encoding="utf-8") as f:
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
                writer.writerow(
                    {
                        "cik": "1234567",
                        "company_name": "ACME CORP",
                        "form": "13D",
                        "date": "2024-01-15",
                        "cusip": "68389X105",
                        "accession_number": "0001234567-24-000001",
                    }
                )

            process_filings(
                index_dir=index_dir,
                output_csv=output_csv,
                sec_name="Test",
                sec_email="test@example.com",
                requests_per_second=100,
                skip_existing=True,
            )

            # Verify CSV has 2 rows: 1 existing + 1 new
            with open(output_csv, newline="") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                assert len(rows) == 2

                # First row should be the existing one
                assert rows[0]["cik"] == "1234567"
                assert rows[0]["accession_number"] == "0001234567-24-000001"
                assert rows[0]["cusip"] == "68389X105"

                # Second row should be the new one
                assert rows[1]["cik"] == "7654321"
                assert rows[1]["accession_number"] == "0007654321-24-000002"
                assert rows[1]["cusip"] == "88888X888"

            # Verify that only 1 filing was downloaded (the new one)
            assert mock_session_instance.get.call_count == 1

    @patch("cik_cusip.processor.download_indices")
    @patch("cik_cusip.processor.parse_index")
    @patch("cik_cusip.processor.create_session")
    @patch("cik_cusip.processor.extract_cusip")
    def test_process_filings_skip_existing_all_processed(
        self, mock_extract, mock_session, mock_parse, mock_download
    ):
        """Test skip_existing when all filings are already processed."""
        # Setup mocks
        mock_download.return_value = ["/tmp/index1.idx"]
        mock_parse.return_value = [
            {
                "cik": "1234567",
                "company_name": "ACME CORP",
                "form": "13D",
                "date": "2024-01-15",
                "url": "https://www.sec.gov/test1.txt",
                "accession_number": "0001234567-24-000001",
            },
        ]

        mock_session_instance = Mock()
        mock_session_instance.close = Mock()
        mock_session.return_value = mock_session_instance

        with tempfile.TemporaryDirectory() as tmpdir:
            output_csv = os.path.join(tmpdir, "output.csv")
            index_dir = os.path.join(tmpdir, "indices")

            # Create existing CSV with the same filing already processed
            os.makedirs(os.path.dirname(output_csv), exist_ok=True)
            with open(output_csv, "w", newline="", encoding="utf-8") as f:
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
                writer.writerow(
                    {
                        "cik": "1234567",
                        "company_name": "ACME CORP",
                        "form": "13D",
                        "date": "2024-01-15",
                        "cusip": "68389X105",
                        "accession_number": "0001234567-24-000001",
                    }
                )

            process_filings(
                index_dir=index_dir,
                output_csv=output_csv,
                sec_name="Test",
                sec_email="test@example.com",
                requests_per_second=100,
                skip_existing=True,
            )

            # Verify CSV still has 1 row (the existing one)
            with open(output_csv, newline="") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                assert len(rows) == 1
                assert rows[0]["cik"] == "1234567"
                assert rows[0]["cusip"] == "68389X105"

            # Verify that no filings were downloaded
            assert mock_session_instance.get.call_count == 0
