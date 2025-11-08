"""CUSIP extraction and validation from SEC filings."""

import re
from typing import Optional


def extract_cusip(text: str) -> Optional[str]:
    """
    Extract CUSIP identifier from SEC filing text.

    Uses a window-based approach looking for explicit CUSIP markers,
    with fallback to document-wide search.

    Args:
        text: Raw filing text

    Returns:
        CUSIP string if found, None otherwise
    """
    # Skip the SEC header to avoid false positives from IRS numbers, dates, etc.
    # The actual document starts after </SEC-HEADER> or <DOCUMENT>
    header_end_markers = [r"</SEC-HEADER>", r"<DOCUMENT>"]
    for marker in header_end_markers:
        match = re.search(marker, text, re.IGNORECASE)
        if match:
            # Search from after the header
            text = text[match.end() :]
            break

    # Clean HTML entities and tags
    # First, preserve CUSIPs with spaces/entities by normalizing them
    # Pattern for CUSIP with spaces: "518439 10 4" or "518439&nbsp;10&nbsp;4"
    text = re.sub(r'(\d{6})[\s&nbsp;]+(\d{1,2})[\s&nbsp;]+(\d)', r'\1\2\3', text, flags=re.IGNORECASE)

    # Now clean remaining HTML entities and tags
    text = re.sub(r"&[a-z]+;", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)

    # CUSIP pattern: 8-10 alphanumeric characters (but will validate separately)
    cusip_pattern = r"\b[A-Z0-9]{8,10}\b"

    # Window method: Look for explicit CUSIP markers
    cusip_markers = [
        r"CUSIP\s+(?:NO\.?|NUMBER|#)",
        r"CUSIP:",
        r"\bCUSIP\b",
        r"Cusip\s+#",
        r"\(CUSIP\s+Number\)",
    ]

    for marker in cusip_markers:
        matches = list(re.finditer(marker, text, re.IGNORECASE))
        if not matches:
            continue

        # Get context around the marker (look ahead more than behind)
        for match in matches:
            start = max(0, match.start() - 100)
            end = min(len(text), match.end() + 200)
            window = text[start:end]

            # Find CUSIP candidates in window
            candidates = re.findall(cusip_pattern, window)

            # Try candidates in order of appearance
            # Use lenient validation since we have an explicit label
            for candidate in candidates:
                if is_valid_cusip(candidate, strict=False):
                    return candidate

    # Fallback: Search entire document
    candidates = re.findall(cusip_pattern, text)

    # Score candidates
    scored = []
    for candidate in candidates:
        # Use strict validation for unlabeled candidates
        if not is_valid_cusip(candidate, strict=True):
            continue

        score = 0
        # Prefer candidates with letters (more specific than all digits)
        if re.search(r"[A-Z]", candidate):
            score += 10
        # Prefer 9-character CUSIPs
        if len(candidate) == 9:
            score += 5

        scored.append((score, candidate))

    if scored:
        scored.sort(reverse=True)
        return scored[0][1]

    return None


def is_valid_cusip(candidate: str, strict: bool = True) -> bool:
    """
    Validate if a candidate string is likely a CUSIP.

    Args:
        candidate: String to validate
        strict: If True, apply strict validation (for unlabeled candidates)
               If False, use lenient validation (for labeled candidates)

    Returns:
        True if likely a valid CUSIP
    """
    # Length check
    if len(candidate) < 8 or len(candidate) > 10:
        return False

    # Must be alphanumeric only
    if not candidate.isalnum():
        return False

    # Must have at least 5 digits (CUSIPs are not all letters)
    digit_count = sum(1 for c in candidate if c.isdigit())
    if digit_count < 5:
        return False

    # Exclude common false positives (always applied)
    common_false_positives = [
        r"^0+$",  # All zeros
        r"^\d{5}$",  # 5-digit zip codes
        r"^\d{5}-\d{4}$",  # 9-digit zip codes with hyphen (12345-6789)
        r"FILE",  # Filename patterns
        r"PAGE",
        r"TABLE",
    ]

    for pattern in common_false_positives:
        if re.match(pattern, candidate):
            return False

    # Strict validation for unlabeled candidates (document-wide search)
    if strict:
        strict_false_positives = [
            r"^\d{10}$",  # 10-digit numbers (likely phone numbers, file numbers, etc.)
            r"^(19|20)\d{6}$",  # Dates in YYYYMMDD format (1900s-2000s)
            r"^\d{8}$",  # 8-digit all-numeric (often dates, file numbers)
        ]

        for pattern in strict_false_positives:
            if re.match(pattern, candidate):
                return False

        # Additional heuristic: CUSIPs typically have a mix of letters and numbers
        # If it's all digits and 9 characters, be more cautious
        if candidate.isdigit() and len(candidate) == 9:
            # Could be valid, but needs to pass extra checks
            # Reject if it looks like a date or sequential number
            if candidate.startswith(("19", "20")):  # Likely a date
                return False
            # Check if it's too sequential or repetitive
            if len(set(candidate)) < 4:  # Less than 4 unique digits
                return False

    # Lenient validation for labeled candidates
    # If we found it near a CUSIP label, trust the label more
    # Just do basic sanity checks
    else:
        # Still reject obvious non-CUSIPs
        if candidate.isdigit() and len(candidate) == 10:  # 10-digit phone numbers
            return False

    return True
