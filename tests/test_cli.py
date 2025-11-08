"""Tests for CLI module."""

import os
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from cik_cusip.cli import cli


class TestExtractCommand:
    """Test extract command."""

    @patch("cik_cusip.cli.process_filings")
    def test_extract_with_credentials(self, mock_process):
        """Test extract command with credentials provided."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "extract",
                "--sec-name",
                "Test User",
                "--sec-email",
                "test@example.com",
            ],
        )

        assert result.exit_code == 0
        mock_process.assert_called_once()

    def test_extract_without_credentials(self):
        """Test extract command fails without credentials."""
        runner = CliRunner()
        result = runner.invoke(cli, ["extract"])

        assert result.exit_code == 1
        assert "SEC credentials required" in result.output

    @patch("cik_cusip.cli.process_filings")
    @patch.dict(os.environ, {"SEC_NAME": "EnvName", "SEC_EMAIL": "env@test.com"})
    def test_extract_with_env_credentials(self, mock_process):
        """Test extract command with environment variable credentials."""
        runner = CliRunner()
        result = runner.invoke(cli, ["extract"])

        assert result.exit_code == 0
        # Verify it was called with env vars
        call_args = mock_process.call_args
        assert call_args[1]["sec_name"] == "EnvName"
        assert call_args[1]["sec_email"] == "env@test.com"

    @patch("cik_cusip.cli.process_filings")
    def test_extract_with_all_flag(self, mock_process):
        """Test extract command with --all flag."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "extract",
                "--all",
                "--sec-name",
                "Test",
                "--sec-email",
                "test@example.com",
            ],
        )

        assert result.exit_code == 0
        call_args = mock_process.call_args
        assert call_args[1]["start_year"] == 1993

    @patch("cik_cusip.cli.process_filings")
    def test_extract_with_year_range(self, mock_process):
        """Test extract command with year range."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "extract",
                "--start-year",
                "2020",
                "--end-year",
                "2024",
                "--sec-name",
                "Test",
                "--sec-email",
                "test@example.com",
            ],
        )

        assert result.exit_code == 0
        call_args = mock_process.call_args
        assert call_args[1]["start_year"] == 2020
        assert call_args[1]["end_year"] == 2024


class TestDownloadCommand:
    """Test download command."""

    @patch("cik_cusip.cli.download_filing_txt")
    def test_download_with_credentials(self, mock_download):
        """Test download command with credentials."""
        mock_download.return_value = "0001234567-12-000001.txt"
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "download",
                "813828",
                "0001234567-12-000001",
                "--sec-name",
                "Test",
                "--sec-email",
                "test@example.com",
            ],
        )

        assert result.exit_code == 0
        assert "Successfully downloaded" in result.output
        mock_download.assert_called_once()

    def test_download_without_credentials(self):
        """Test download command fails without credentials."""
        runner = CliRunner()
        result = runner.invoke(cli, ["download", "813828", "0001234567-12-000001"])

        assert result.exit_code == 1
        assert "SEC credentials required" in result.output

    @patch("cik_cusip.cli.download_filing_txt")
    def test_download_with_custom_output(self, mock_download):
        """Test download command with custom output path."""
        mock_download.return_value = "myfile.txt"
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "download",
                "813828",
                "0001234567-12-000001",
                "-o",
                "myfile.txt",
                "--sec-name",
                "Test",
                "--sec-email",
                "test@example.com",
            ],
        )

        assert result.exit_code == 0
        call_args = mock_download.call_args
        assert call_args[1]["output_path"] == "myfile.txt"


class TestCliVersion:
    """Test CLI version command."""

    def test_version(self):
        """Test version flag."""
        runner = CliRunner()
        result = runner.invoke(cli, ["--version"])

        assert result.exit_code == 0
        assert "2.0.0" in result.output
