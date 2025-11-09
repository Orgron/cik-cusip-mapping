"""Tests for cusip module."""

import pytest

from cik_cusip.cusip import extract_cusip, is_valid_cusip


class TestIsValidCusip:
    """Test is_valid_cusip function."""

    def test_valid_9_char_alphanumeric(self):
        """Test valid 9-character alphanumeric CUSIP."""
        assert is_valid_cusip("68389X105", strict=True)
        assert is_valid_cusip("037833100", strict=True)

    def test_valid_8_char(self):
        """Test valid 8-character CUSIP."""
        assert is_valid_cusip("68389X10", strict=True)

    def test_invalid_too_short(self):
        """Test that too short strings are invalid."""
        assert not is_valid_cusip("1234567", strict=True)

    def test_invalid_too_long(self):
        """Test that too long strings are invalid."""
        assert not is_valid_cusip("12345678901", strict=True)

    def test_invalid_non_alphanumeric(self):
        """Test that non-alphanumeric strings are invalid."""
        assert not is_valid_cusip("123-45-678", strict=True)

    def test_invalid_too_few_digits(self):
        """Test that strings with too few digits are invalid."""
        assert not is_valid_cusip("ABCDEFGHI", strict=True)

    def test_invalid_all_zeros(self):
        """Test that all zeros is invalid."""
        assert not is_valid_cusip("000000000", strict=True)

    def test_invalid_zip_code(self):
        """Test that zip codes are rejected."""
        assert not is_valid_cusip("12345", strict=True)

    def test_invalid_phone_number(self):
        """Test that 10-digit phone numbers are rejected."""
        assert not is_valid_cusip("5551234567", strict=True)

    def test_invalid_date_format(self):
        """Test that dates in YYYYMMDD format are rejected."""
        assert not is_valid_cusip("20240115", strict=True)
        assert not is_valid_cusip("19990101", strict=True)

    def test_lenient_mode(self):
        """Test lenient validation mode for labeled candidates."""
        # In lenient mode, some strict checks are relaxed
        assert is_valid_cusip("123456789", strict=False)
        # Lenient mode is used when CUSIP is explicitly labeled,
        # so we trust the label more than strict validation

    def test_valid_12_char_with_country_code(self):
        """Test valid 12-character CUSIP with country code prefix."""
        assert is_valid_cusip("US37247D1063", strict=True)
        assert is_valid_cusip("CA1234567890", strict=False)

    def test_invalid_12_char_no_country_code(self):
        """Test that 12-character strings without country code are invalid."""
        assert not is_valid_cusip("123456789012", strict=True)  # All digits, no country code


class TestExtractCusip:
    """Test extract_cusip function."""

    def test_extract_with_cusip_label(self):
        """Test extraction when CUSIP is labeled."""
        text = """
        <SEC-HEADER>Header content</SEC-HEADER>
        <DOCUMENT>
        CUSIP: 68389X105
        Other content here
        </DOCUMENT>
        """
        result = extract_cusip(text)
        assert result == "68389X105"

    def test_extract_with_cusip_number_label(self):
        """Test extraction with CUSIP NUMBER label."""
        text = """
        <SEC-HEADER>Header</SEC-HEADER>
        CUSIP NUMBER: 037833100
        """
        result = extract_cusip(text)
        assert result == "037833100"

    def test_extract_from_html(self):
        """Test extraction from HTML content."""
        text = """
        </SEC-HEADER>
        <html>
        <body>
        <p>CUSIP: <b>68389X105</b></p>
        </body>
        </html>
        """
        result = extract_cusip(text)
        assert result == "68389X105"

    def test_extract_no_cusip(self):
        """Test that None is returned when no CUSIP found."""
        text = """
        <SEC-HEADER>Header</SEC-HEADER>
        This document has no CUSIP identifier.
        """
        result = extract_cusip(text)
        assert result is None

    def test_skip_sec_header(self):
        """Test that SEC header is skipped to avoid false positives."""
        text = """
        <SEC-HEADER>
        FILE NUMBER: 123456789
        </SEC-HEADER>
        <DOCUMENT>
        CUSIP: 68389X105
        </DOCUMENT>
        """
        result = extract_cusip(text)
        # Should find the real CUSIP, not the file number in header
        assert result == "68389X105"

    def test_extract_with_entities(self):
        """Test extraction with HTML entities."""
        text = """
        </SEC-HEADER>
        CUSIP&nbsp;Number:&nbsp;68389X105
        """
        result = extract_cusip(text)
        assert result == "68389X105"

    def test_extract_prefers_labeled(self):
        """Test that labeled CUSIPs are preferred over unlabeled."""
        text = """
        </SEC-HEADER>
        Some text here with data.
        CUSIP: 68389X105
        Other content: 123456789
        """
        result = extract_cusip(text)
        assert result == "68389X105"

    def test_extract_12_char_cusip_with_country_code(self):
        """Test extraction of 12-character CUSIP with country code."""
        text = """
        </SEC-HEADER>
        <DOCUMENT>
        CUSIP Number: US37247D1063
        </DOCUMENT>
        """
        result = extract_cusip(text)
        assert result == "US37247D1063"

    def test_extract_12_char_cusip_real_example(self):
        """Test extraction from real AMVESCAP filing."""
        text = """
        <SEC-HEADER>Header content</SEC-HEADER>
                                  US37247D1063
--------------------------------------------------------------------------------
                                 (CUSIP Number)

CUSIP No.         US37247D1063

Item 2(e)    CUSIP Number:
             US37247D1063
        """
        result = extract_cusip(text)
        assert result == "US37247D1063"
