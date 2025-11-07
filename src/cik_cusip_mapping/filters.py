"""Shared helpers for applying filing filters."""

from __future__ import annotations

import datetime as _dt
from collections.abc import Iterable
from typing import Any


def coerce_date(value: Any | None) -> _dt.date | None:
    """Convert ``value`` to a :class:`datetime.date` when possible."""

    if value is None:
        return None
    if isinstance(value, _dt.datetime):
        return value.date()
    if isinstance(value, _dt.date):
        return value
    if isinstance(value, str):
        return _dt.date.fromisoformat(value)
    raise TypeError(f"Unsupported date value: {value!r}")


def normalize_cik_whitelist(
    values: Iterable[str | int] | None,
) -> tuple[set[str], set[int]] | None:
    """Return sets of raw and numeric CIKs suitable for filtering."""

    if values is None:
        return None

    raw: set[str] = set()
    numeric: set[int] = set()
    for item in values:
        text = "".join(str(item).split())
        if not text:
            continue
        raw.add(text)
        if text.isdigit():
            numeric.add(int(text))
    return raw, numeric


def is_amended_form(form_value: str) -> bool:
    """Return ``True`` when ``form_value`` represents an amendment."""

    normalized = form_value.strip().upper()
    return normalized.endswith("A")
