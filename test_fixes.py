#!/usr/bin/env python3
"""Quick test of the CUSIP extraction fixes."""

from main import extract_cusip, is_valid_cusip

def test_skip_header():
    """Test that SEC header is skipped."""
    text = """
    <SEC-HEADER>
    IRS NUMBER: 841280679
    DATE OF NAME CHANGE: 19960928
    </SEC-HEADER>
    <DOCUMENT>
    CUSIP NO. 140065103
    </DOCUMENT>
    """
    result = extract_cusip(text)
    assert result == "140065103", f"Expected 140065103 but got {result}"
    print("✓ Test 1: Skip header - PASS")

def test_numeric_nine_digit():
    """Test extraction of all-numeric 9-digit CUSIPs."""
    test_cases = [
        ("CUSIP: 140065103", "140065103"),
        ("CUSIP Number: 292758109", "292758109"),
        ("903236107\n(CUSIP Number)", "903236107"),
    ]

    for text, expected in test_cases:
        result = extract_cusip(text)
        assert result == expected, f"For '{text}', expected {expected} but got {result}"
    print("✓ Test 2: Numeric 9-digit CUSIPs - PASS")

def test_reject_dates():
    """Test that date-like numbers are rejected in strict mode."""
    dates = ["20060601", "19960928", "20070702"]
    for date in dates:
        assert not is_valid_cusip(date, strict=True), f"Date {date} should be rejected"
    print("✓ Test 3: Reject dates in strict mode - PASS")

def test_reject_phone_numbers():
    """Test that 10-digit phone numbers are rejected."""
    phones = ["6106691000", "3103958005"]
    for phone in phones:
        assert not is_valid_cusip(phone, strict=True), f"Phone {phone} should be rejected"
    print("✓ Test 4: Reject phone numbers - PASS")

def test_zip_code_fix():
    """Test that 9-digit CUSIPs are not rejected as zip codes."""
    # These should be valid
    assert is_valid_cusip("140065103", strict=False)
    assert is_valid_cusip("292758109", strict=False)
    # Zip codes should be rejected
    assert not is_valid_cusip("12345", strict=True)
    print("✓ Test 5: Zip code pattern fix - PASS")

def test_real_world_samples():
    """Test extraction from real sample filings."""
    # From 1004740.txt
    text1 = """
    </SEC-HEADER>
    <DOCUMENT>
    (Title of Class of Security)

    140065103
    --------------
    (CUSIP Number)
    """
    assert extract_cusip(text1) == "140065103"

    # From 8504.txt
    text2 = """
    </SEC-HEADER>
    <DOCUMENT>
    292758109
    ---------
    (CUSIP Number)
    """
    assert extract_cusip(text2) == "292758109"

    # Alphanumeric CUSIP
    text3 = "CUSIP: 86722Q207"
    assert extract_cusip(text3) == "86722Q207"

    print("✓ Test 6: Real-world samples - PASS")

if __name__ == "__main__":
    print("Running CUSIP extraction fix tests...\n")
    try:
        test_skip_header()
        test_numeric_nine_digit()
        test_reject_dates()
        test_reject_phone_numbers()
        test_zip_code_fix()
        test_real_world_samples()
        print("\n✅ All tests passed!")
    except AssertionError as e:
        print(f"\n❌ Test failed: {e}")
        exit(1)
