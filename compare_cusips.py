#!/usr/bin/env python3
"""
Compare CUSIP extraction from automated parser vs manual extraction.
This script will help identify discrepancies and improve the parser.
"""

import os
import re
from pathlib import Path
from main import extract_cusip

def manual_extract_cusip(text: str) -> str | None:
    """
    Manually extract CUSIP using common patterns found in SEC filings.
    This serves as ground truth for comparison.
    """
    # Skip SEC header to avoid false positives
    header_end = re.search(r'</SEC-HEADER>|<DOCUMENT>', text, re.IGNORECASE)
    if header_end:
        text = text[header_end.end():]

    # Look for explicit CUSIP labels with various formats
    # Support both regular CUSIPs and hyphenated ones (e.g., 68338A-10-7)
    patterns = [
        r'(?:CUSIP|Cusip)\s*(?:NO\.?|NUMBER|No\.|#|:)?\s*[:\s]*([A-Z0-9]{6,10})',
        r'(?:CUSIP|Cusip)\s*(?:NO\.?|NUMBER|No\.|#|:)?\s*[:\s]*([A-Z0-9-]{8,12})',  # With hyphens
        r'(?:CUSIP|Cusip)\s+([A-Z0-9]{9})',
        r'Cusip\s*#\s*([A-Z0-9]{6,10})',
    ]

    for pattern in patterns:
        matches = re.findall(pattern, text, re.IGNORECASE)
        for match in matches:
            # Clean up the match
            cusip = match.strip()
            # Remove hyphens for standardization
            cusip_no_hyphen = cusip.replace('-', '')

            # CUSIPs are typically 9 characters (sometimes 8-10) after removing hyphens
            if 8 <= len(cusip_no_hyphen) <= 10 and cusip_no_hyphen.isalnum():
                # Check if it has enough digits (at least 5)
                digit_count = sum(1 for c in cusip_no_hyphen if c.isdigit())
                if digit_count >= 5:
                    # Return the version with hyphens if that's how it appeared
                    return cusip

    return None


def compare_all_filings():
    """Compare automated vs manual CUSIP extraction for all sample filings."""
    sample_dir = Path("sample_filings")

    if not sample_dir.exists():
        print(f"Error: {sample_dir} does not exist!")
        return

    results = []
    mismatches = []

    # Get all .txt files in sample_filings
    files = sorted(sample_dir.glob("*.txt"))

    print(f"Analyzing {len(files)} sample filings...\n")
    print(f"{'File':<20} {'Auto Extract':<15} {'Manual Extract':<15} {'Match':<10}")
    print("=" * 65)

    for file_path in files:
        filename = file_path.name

        # Read the file
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            text = f.read()

        # Extract using both methods
        auto_cusip = extract_cusip(text)
        manual_cusip = manual_extract_cusip(text)

        # Normalize None values for comparison
        auto_str = auto_cusip if auto_cusip else "NOT FOUND"
        manual_str = manual_cusip if manual_cusip else "NOT FOUND"

        # Check if they match
        match = auto_cusip == manual_cusip
        match_str = "✓" if match else "✗ MISMATCH"

        # Store result
        results.append({
            'file': filename,
            'auto': auto_cusip,
            'manual': manual_cusip,
            'match': match
        })

        # Track mismatches
        if not match:
            mismatches.append({
                'file': filename,
                'file_path': str(file_path),
                'auto': auto_cusip,
                'manual': manual_cusip,
                'text_snippet': text[:2000]  # First 2000 chars for debugging
            })

        # Print result
        print(f"{filename:<20} {auto_str:<15} {manual_str:<15} {match_str:<10}")

    # Print summary
    print("\n" + "=" * 65)
    total = len(results)
    matches = sum(1 for r in results if r['match'])
    print(f"\nSummary:")
    print(f"  Total files: {total}")
    print(f"  Matches: {matches} ({matches/total*100:.1f}%)")
    print(f"  Mismatches: {len(mismatches)} ({len(mismatches)/total*100:.1f}%)")

    # Show detailed mismatch information
    if mismatches:
        print(f"\n\nDetailed Mismatch Analysis:")
        print("=" * 65)
        for i, mismatch in enumerate(mismatches, 1):
            print(f"\n{i}. {mismatch['file']}")
            print(f"   Automated: {mismatch['auto']}")
            print(f"   Manual:    {mismatch['manual']}")

            # Show context around CUSIP mentions
            text = mismatch['text_snippet']
            cusip_mentions = []
            for match in re.finditer(r'(?:CUSIP|Cusip)[^\n]{0,100}', text, re.IGNORECASE):
                cusip_mentions.append(match.group(0))

            if cusip_mentions:
                print(f"   Context:")
                for mention in cusip_mentions[:3]:  # Show first 3 mentions
                    print(f"     - {mention.strip()}")

    return results, mismatches


if __name__ == "__main__":
    results, mismatches = compare_all_filings()
