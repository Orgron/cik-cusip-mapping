from __future__ import annotations

from cik_cusip_mapping import parsing


def build_text(body: str) -> str:
    header = "SUBJECT COMPANY\nCENTRAL INDEX KEY\t\t\t0000000000\n"
    return header + "<DOCUMENT>\n" + body + "\n</DOCUMENT>"


def test_parse_text_prefers_lettered_cusip_over_numeric_header():
    body = """CUSIP Number 95805V108\nCUSIP No.: 900435108\nCUSIP No.: 900435108"""
    text = build_text(body)
    cik, cusip, method = parsing.parse_text(text)
    assert cik == "0000000000"
    assert cusip == "95805V108"
    assert method == "window"


def test_parse_text_ignores_po_box_tokens():
    body = """P.O. Box CB-13136\nCUSIP\n928957109"""
    text = build_text(body)
    cik, cusip, _method = parsing.parse_text(text)
    assert cusip == "928957109"


def test_parse_text_collects_multiple_cusips():
    body = (
        "CUSIP No. 72200Y508\nCUSIP No. 72200T509\nCUSIP No. 72201E402"
    )
    text = build_text(body)
    _cik, cusip, _method = parsing.parse_text(text)
    assert cusip == "72200Y508;72200T509;72201E402"


def test_parse_text_skips_document_filenames():
    body = (
        "<head><title>Example (W0010741).DOC</title>\n"
        "CUSIP Number 40421W106"
    )
    text = build_text(body)
    _cik, cusip, _method = parsing.parse_text(text)
    assert cusip == "40421W106"
