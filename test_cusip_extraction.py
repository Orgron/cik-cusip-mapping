#!/usr/bin/env python3
"""Test script to validate CUSIP extraction against ground truth."""

import csv
import html
import re
from pathlib import Path


def extract_cusip(filing_text):
    """
    Extract CUSIP from SEC 13D or 13G filing text.
    Handles both traditional text/HTML formats and modern XML formats.
    Supports variable-length CUSIPs (8-10 characters).

    Args:
        filing_text (str): The text content of the SEC filing

    Returns:
        str: The CUSIP number if found, None otherwise
    """
    if not filing_text:
        return None

    # Remove SEC header to avoid false matches in cryptographic keys
    header_end = filing_text.find("</SEC-HEADER>")
    if header_end != -1:
        filing_text = filing_text[header_end:]

    # Decode HTML entities to normalize text
    filing_text = html.unescape(filing_text)

    # Focus on main document section to avoid page headers
    doc_start = filing_text.find("<DOCUMENT>")
    if doc_start != -1:
        # Extract main document section (before repeated page patterns)
        main_section = filing_text[doc_start:doc_start + 50000]  # First 50k chars of main doc
    else:
        main_section = filing_text[:50000]  # First 50k chars if no <DOCUMENT> tag

    def clean_cusip(cusip_raw):
        """Remove spaces, dashes, and other separators from CUSIP"""
        # \s matches all whitespace including \xa0 (non-breaking space)
        return re.sub(r"[\s\-]+", "", cusip_raw)

    def is_valid_cusip(cusip_str):
        """Validate CUSIP: 8-10 alphanumeric chars, letters are uppercase
        Note: Standard CUSIPs are 9 chars, but older/newer filings may use 8 or 10."""
        if not (8 <= len(cusip_str) <= 10) or not cusip_str.isalnum():
            return False
        # Check that any letters are uppercase (digits don't have case)
        # Must have at least 5 digits to avoid matching random words
        digit_count = sum(c.isdigit() for c in cusip_str)
        if digit_count < 5:
            return False
        return all(c.isupper() or c.isdigit() for c in cusip_str)

    # Pattern 0: Check for explicit NONE or NOT APPLICABLE
    none_pattern = r"\(CUSIP\s+Number\)[^\w]*NONE(?:\s|<|$)"
    if re.search(none_pattern, main_section, re.IGNORECASE):
        return None

    # Pattern 1: XML tags (for modern filings 2023+) - support 8-10 chars
    xml_patterns = [
        r"<issuerCusip>([A-Z0-9]{8,10})</issuerCusip>",
        r"<issuerCUSIP>([A-Z0-9]{8,10})</issuerCUSIP>",
        r"<cusip>([A-Z0-9]{8,10})</cusip>",
        r"<CUSIP>([A-Z0-9]{8,10})</CUSIP>",
    ]

    for pattern in xml_patterns:
        match = re.search(pattern, filing_text, re.IGNORECASE)
        if match:
            cusip_cleaned = clean_cusip(match.group(1))
            if is_valid_cusip(cusip_cleaned):
                return cusip_cleaned

    # Pattern 2: Number directly before (CUSIP Number) label
    # This pattern captures cases like "21111310\n(CUSIP Number)" or "95805V108 </B></P> ... <B>(CUSIP Number)"
    # First try with minimal separation (newlines, tags, whitespace only)
    pattern_before_label_strict = r"([A-Z0-9](?:[\s\-]*[A-Z0-9]){7,9})[\s\r\n\<\>\/=\'\";:]*\(CUSIP\s+Number\)"
    matches = re.finditer(pattern_before_label_strict, main_section, re.IGNORECASE)

    for match in matches:
        cusip_raw = match.group(1).strip()
        # Check if it contains "NOT APPLICABLE" or similar
        if re.search(r'NOT?\s*APPLIC', cusip_raw, re.IGNORECASE):
            continue
        cusip_cleaned = clean_cusip(cusip_raw)
        if is_valid_cusip(cusip_cleaned):
            return cusip_cleaned

    # Pattern 2b: Allow more HTML content but still within ~200 chars
    pattern_before_label_loose = r"([A-Z0-9](?:[\s\-]*[A-Z0-9]){7,9})[\s\S]{0,200}?\(CUSIP\s+Number\)"
    matches = re.finditer(pattern_before_label_loose, main_section, re.IGNORECASE)

    for match in matches:
        cusip_raw = match.group(1).strip()
        # Check if it contains "NOT APPLICABLE" or similar
        if re.search(r'NOT?\s*APPLIC', cusip_raw, re.IGNORECASE):
            continue
        cusip_cleaned = clean_cusip(cusip_raw)
        if is_valid_cusip(cusip_cleaned):
            return cusip_cleaned

    # Pattern 3: Standalone CUSIP near (CUSIP Number) label - PRIORITIZE THIS
    # This is more reliable than other patterns as it looks for the standard label format
    cusip_label_pattern = r"\(CUSIP\s+Number\)"
    label_matches = re.finditer(cusip_label_pattern, main_section, re.IGNORECASE)

    for label_match in label_matches:
        # Increased window to 500 chars for HTML-heavy documents
        start_pos = max(0, label_match.start() - 500)
        context = main_section[start_pos : label_match.start()]

        # Find potential CUSIPs with spaces or dashes (support 8-10 chars)
        potential_cusips = re.findall(
            r"(?:>|\s|^)([A-Z0-9][A-Z0-9\s\-]{6,12}[A-Z0-9])(?:<|\s|$)", context
        )

        for cusip_raw in reversed(potential_cusips):
            # Skip if it looks like "NOT APPLICABLE"
            if re.search(r'NOT?\s*APPLIC', cusip_raw, re.IGNORECASE):
                continue
            cusip_cleaned = clean_cusip(cusip_raw)
            if is_valid_cusip(cusip_cleaned):
                return cusip_cleaned

    # Pattern 4: Number followed by CUSIP label (larger window for HTML)
    pattern_number_first = (
        r"([A-Z0-9][\sA-Z0-9\-]{6,14})[\s\n\r\<\>]+[^\(]{0,500}?\(CUSIP\s+Number\)"
    )
    matches = re.finditer(pattern_number_first, main_section, re.IGNORECASE | re.DOTALL)

    for match in matches:
        cusip_raw = match.group(1).strip()
        # Check if it contains "NOT APPLICABLE" or similar
        if re.search(r'NOT?\s*APPLIC', cusip_raw, re.IGNORECASE):
            continue
        cusip_cleaned = clean_cusip(cusip_raw)
        if is_valid_cusip(cusip_cleaned):
            return cusip_cleaned

    # Pattern 5: CUSIP label followed by number (handles spaces and dashes)
    # These patterns are LESS reliable as they may match page headers
    # Only "(CUSIP Number)" format is included here for safety
    patterns_label_first = [
        r"\(CUSIP\s+Number\)\s*[:\-]?\s*([A-Z0-9](?:[\s\-]*[A-Z0-9]){7,9})(?:\s|<|$|\n|[^A-Z0-9])",
    ]

    for pattern in patterns_label_first:
        matches = re.finditer(pattern, main_section, re.IGNORECASE)
        for match in matches:
            cusip_raw = match.group(1).strip()
            # Check if it contains "NOT APPLICABLE" or similar
            if re.search(r'NOT?\s*APPLIC', cusip_raw, re.IGNORECASE):
                continue
            cusip_cleaned = clean_cusip(cusip_raw)
            if is_valid_cusip(cusip_cleaned):
                return cusip_cleaned

    # Pattern 6: Other CUSIP label formats (fallback, may match page headers)
    patterns_label_fallback = [
        r"CUSIP\s+(?:Number|No\.?|#)\s*[:\-]?\s*([A-Z0-9](?:[\s\-]*[A-Z0-9]){7,9})(?:\s|<|$|\n|[^A-Z0-9])",
        r"CUSIP\s*[:\-]\s*([A-Z0-9](?:[\s\-]*[A-Z0-9]){7,9})(?:\s|<|$|\n|[^A-Z0-9])",
    ]

    for pattern in patterns_label_fallback:
        matches = re.finditer(pattern, main_section, re.IGNORECASE)
        for match in matches:
            cusip_raw = match.group(1).strip()
            # Check if it contains "NOT APPLICABLE" or similar
            if re.search(r'NOT?\s*APPLIC', cusip_raw, re.IGNORECASE):
                continue
            cusip_cleaned = clean_cusip(cusip_raw)
            if is_valid_cusip(cusip_cleaned):
                return cusip_cleaned

    # Pattern 7: CUSIP No. in page headers (last resort fallback)
    # Only use if nothing found in main section
    header_pattern = r"CUSIP\s+No\.?\s*[:\-]?\s*([A-Z0-9](?:[\s\-]*[A-Z0-9]){7,9})(?:\s|<|$|\n|[^A-Z0-9])"
    matches = re.finditer(header_pattern, filing_text, re.IGNORECASE)

    for match in matches:
        cusip_raw = match.group(1).strip()
        # Skip if it contains "NOT APPLICABLE" or similar
        if re.search(r'NOT?\s*APPLIC', cusip_raw, re.IGNORECASE):
            continue
        cusip_cleaned = clean_cusip(cusip_raw)
        if is_valid_cusip(cusip_cleaned):
            return cusip_cleaned

    return None


def load_ground_truth(csv_path):
    """Load ground truth data from CSV."""
    ground_truth = {}
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            ground_truth[row['filename']] = row['cusip']
    return ground_truth


def test_extraction(form_examples_dir, ground_truth_csv):
    """Test CUSIP extraction on all form examples and report results."""
    ground_truth = load_ground_truth(ground_truth_csv)

    results = {
        'passed': [],
        'failed': [],
        'missing_files': []
    }

    total_files = len(ground_truth)

    for filename, expected_cusip in ground_truth.items():
        filepath = Path(form_examples_dir) / filename

        if not filepath.exists():
            results['missing_files'].append(filename)
            continue

        # Read file content
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()

        # Extract CUSIP
        extracted_cusip = extract_cusip(content)

        # Compare with expected
        # Handle multiple CUSIPs (semicolon-separated)
        expected_cusips = expected_cusip.split(';')

        # Normalize comparison
        match = False
        if extracted_cusip:
            # Check if extracted matches any of the expected
            match = extracted_cusip in expected_cusips
        elif expected_cusip == 'NONE':
            # If expected is NONE, extracted should also be None
            match = extracted_cusip is None

        if match:
            results['passed'].append({
                'filename': filename,
                'expected': expected_cusip,
                'extracted': extracted_cusip
            })
        else:
            results['failed'].append({
                'filename': filename,
                'expected': expected_cusip,
                'extracted': extracted_cusip
            })

    # Print results
    print("=" * 80)
    print(f"CUSIP EXTRACTION TEST RESULTS")
    print("=" * 80)
    print(f"Total files: {total_files}")
    print(f"Passed: {len(results['passed'])}")
    print(f"Failed: {len(results['failed'])}")
    print(f"Missing files: {len(results['missing_files'])}")
    print(f"Success rate: {len(results['passed']) / total_files * 100:.1f}%")
    print()

    if results['failed']:
        print("=" * 80)
        print("FAILED CASES:")
        print("=" * 80)
        for case in results['failed']:
            print(f"\nFile: {case['filename']}")
            print(f"  Expected: {case['expected']}")
            print(f"  Extracted: {case['extracted']}")

    if results['missing_files']:
        print("\n" + "=" * 80)
        print("MISSING FILES:")
        print("=" * 80)
        for filename in results['missing_files']:
            print(f"  {filename}")

    return results


if __name__ == '__main__':
    form_examples_dir = '/home/user/cik-cusip-mapping/form_examples'
    ground_truth_csv = '/home/user/cik-cusip-mapping/analysis/manual_input.csv'

    results = test_extraction(form_examples_dir, ground_truth_csv)
