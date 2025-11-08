"""Utility functions for CIK-CUSIP mapping."""


def load_cik_filter(cik_filter_file: str) -> set:
    """
    Load CIK filter from a text file.

    Args:
        cik_filter_file: Path to text file containing CIKs (one per line)

    Returns:
        Set of CIKs to filter for
    """
    ciks = set()
    with open(cik_filter_file, "r", encoding="utf-8") as f:
        for line in f:
            cik = line.strip()
            if cik:
                # Normalize CIK to 10-digit format with leading zeros
                ciks.add(cik)
    return ciks
