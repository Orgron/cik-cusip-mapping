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
    # First, normalize CUSIPs with various separators (dashes, spaces, &nbsp;)
    # IMPORTANT: Apply patterns in specific order, most specific first, to avoid
    # partial matches that could destroy valid CUSIPs

    # Pattern 1: CUSIPs with dashes in various formats
    # Use word boundaries to prevent matching partial CUSIPs with following text
    # 1a: Standard format with digits: "80004C-10-1" → "80004C101"
    text = re.sub(r'\b([A-Z0-9]{6})[\s\-]+(\d{1,2})[\s\-]+(\d)\b', r'\1\2\3', text, flags=re.IGNORECASE)
    # 1b: With letters at end: "461148-AA6" → "461148AA6"
    text = re.sub(r'\b([A-Z0-9]{6})[\s\-]+([A-Z0-9]{2,3})\b', r'\1\2', text, flags=re.IGNORECASE)
    # 1c: Unusual positions: "922-57T-202" → "92257T202"
    # Must have word boundaries to avoid matching "106\n\nChe"
    text = re.sub(r'\b([A-Z0-9]{3})[\s\-]+([A-Z0-9]{3})[\s\-]+([A-Z0-9]{3})\b', r'\1\2\3', text, flags=re.IGNORECASE)

    # Pattern 2: CUSIPs with spaces/&nbsp; - e.g., "518439 10 4" or "563 118 108"
    # Already handled by Pattern 1c for 3-3-3 format
    text = re.sub(r'\b([A-Z0-9]{6})[\s&nbsp;]+([A-Z0-9]{2})[\s&nbsp;]+([A-Z0-9])\b', r'\1\2\3', text, flags=re.IGNORECASE)

    # Pattern 3: Remove parentheses around CUSIPs - e.g., "(736420100)" → "736420100"
    text = re.sub(r'\(([A-Z0-9]{8,10})\)', r'\1', text)

    # Pattern 4: Separate CUSIPs from form types like "13G/A", "13D", "SC 13G"
    # e.g., "82257T20213G/A" → "82257T202 13G/A"
    text = re.sub(r'([A-Z0-9]{9})(\d{1,2}[A-Z](/[A-Z])?)', r'\1 \2', text)

    # Now clean remaining HTML entities and tags
    text = re.sub(r"&[a-z]+;", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)

    # CUSIP pattern: 8-10 alphanumeric characters
    # IMPORTANT: Must contain at least one digit to avoid matching English words
    # like "WASHINGTON", "remainder", etc.
    # This pattern requires at least one digit anywhere in the string
    cusip_pattern = r"\b(?=\w*\d)[A-Z0-9]{8,10}\b"

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

        # Get context around the marker
        for match in matches:
            # Try both forward and backward windows
            # Collect all candidates with their relative positions
            all_candidates = []

            # Forward window (200 chars)
            forward_start = match.end()
            forward_end = min(len(text), match.end() + 200)
            forward_window = text[forward_start:forward_end]
            for m in re.finditer(cusip_pattern, forward_window, re.IGNORECASE):
                distance = m.start()  # Distance from marker end
                all_candidates.append((distance, m.group(), 'forward'))

            # Backward window (300 chars - increased to catch CUSIPs in previous table rows)
            backward_start = max(0, match.start() - 300)
            backward_end = match.start()
            backward_window = text[backward_start:backward_end]
            for m in re.finditer(cusip_pattern, backward_window, re.IGNORECASE):
                distance = (backward_end - backward_start) - m.end()  # Distance from marker start
                all_candidates.append((distance, m.group(), 'backward'))

            # Sort by distance (closest first)
            all_candidates.sort(key=lambda x: x[0])

            # Try candidates in order of proximity
            for _, candidate, _ in all_candidates:
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
    # Note: We allow 10-digit numeric CUSIPs here because leading zeros are valid
    # (e.g., "0462220109") and when explicitly labeled, we trust the label

    return True
