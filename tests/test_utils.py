"""Tests for utils module."""

import tempfile
from pathlib import Path

import pytest

from cik_cusip.utils import load_cik_filter


class TestLoadCikFilter:
    """Test load_cik_filter function."""

    def test_load_cik_filter(self):
        """Test loading CIKs from a file."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".txt") as f:
            f.write("1234567\n")
            f.write("9876543\n")
            f.write("0001234567\n")
            f.write("\n")  # Empty line
            f.write("  7654321  \n")  # With whitespace
            temp_path = f.name

        try:
            ciks = load_cik_filter(temp_path)
            assert isinstance(ciks, set)
            assert len(ciks) == 4
            assert "1234567" in ciks
            assert "9876543" in ciks
            assert "0001234567" in ciks
            assert "7654321" in ciks
        finally:
            Path(temp_path).unlink()

    def test_load_empty_file(self):
        """Test loading from an empty file."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".txt") as f:
            temp_path = f.name

        try:
            ciks = load_cik_filter(temp_path)
            assert isinstance(ciks, set)
            assert len(ciks) == 0
        finally:
            Path(temp_path).unlink()

    def test_load_with_only_whitespace(self):
        """Test loading file with only whitespace."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".txt") as f:
            f.write("   \n")
            f.write("\n")
            f.write("\t\n")
            temp_path = f.name

        try:
            ciks = load_cik_filter(temp_path)
            assert isinstance(ciks, set)
            assert len(ciks) == 0
        finally:
            Path(temp_path).unlink()
