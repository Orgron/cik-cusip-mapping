"""Post-processing helpers for the parsed CIK to CUSIP mappings."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd


def postprocess_mappings(
    csv_paths: Iterable[Path | str],
    *,
    output: Path | str | None = None,
    valid_lengths: Iterable[int] | None = None,
    forbidden_prefixes: Iterable[str] | None = None,
) -> pd.DataFrame:
    """Combine per-form CSV files into the final mapping DataFrame."""

    length_whitelist = set(valid_lengths or {6, 8, 9})
    excluded_prefixes = {prefix.upper() for prefix in (forbidden_prefixes or {"000000", "0001PT"})}
    frames: list[pd.DataFrame] = []
    for csv_path in csv_paths:
        path = Path(csv_path)
        frame = pd.read_csv(
            path,
            names=["filename", "cik", "cusip"],
            encoding="utf-8",
        ).dropna()
        if frame.empty:
            continue
        frame["leng"] = frame.cusip.map(len)
        frame = frame[frame.leng.isin(length_whitelist)]
        if frame.empty:
            continue
        frame["cusip"] = frame.cusip.str.upper()
        frame["cusip6"] = frame.cusip.str[:6]
        frame = frame[~frame.cusip6.str.upper().isin(excluded_prefixes)]
        if frame.empty:
            continue
        frame["cusip8"] = frame.cusip.str[:8]
        frame["cik"] = pd.to_numeric(frame.cik)
        frames.append(frame[["cik", "cusip6", "cusip8"]])

    if frames:
        result = pd.concat(frames, ignore_index=True).drop_duplicates()
    else:
        result = pd.DataFrame(columns=["cik", "cusip6", "cusip8"])

    if output is not None:
        output_path = Path(output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        result.to_csv(output_path, index=False, encoding="utf-8")

    return result


def build_cusip_dynamics(
    events_paths: Iterable[Path | str],
    *,
    output: Path | str | None = None,
) -> pd.DataFrame:
    """Aggregate filing-level events into CUSIP dynamics metrics."""

    frames: list[pd.DataFrame] = []
    for csv_path in events_paths:
        path = Path(csv_path)
        if not path.exists():
            continue
        frame = pd.read_csv(path, dtype=str, encoding="utf-8")
        if frame.empty:
            continue
        frames.append(frame)

    if not frames:
        result = pd.DataFrame(
            columns=
            [
                "cik",
                "cusip6",
                "cusip8",
                "cusip9",
                "first_seen",
                "last_seen",
                "filings_count",
                "forms",
                "months_active",
                "most_recent_accession",
                "most_recent_form",
                "most_recent_filing_date",
                "valid_check_digit",
                "parse_methods",
                "fallback_filings",
            ]
        )
    else:
        combined = pd.concat(frames, ignore_index=True)
        combined["cik"] = pd.to_numeric(combined["cik"], errors="coerce")
        combined["filing_date"] = pd.to_datetime(
            combined["filing_date"], errors="coerce"
        )
        combined = combined.dropna(subset=["cik", "cusip8", "filing_date"])
        if combined.empty:
            result = pd.DataFrame(
                columns=
                [
                    "cik",
                    "cusip6",
                    "cusip8",
                    "cusip9",
                    "first_seen",
                    "last_seen",
                    "filings_count",
                    "forms",
                    "months_active",
                    "most_recent_accession",
                    "most_recent_form",
                    "most_recent_filing_date",
                    "valid_check_digit",
                    "parse_methods",
                    "fallback_filings",
                ]
            )
        else:
            combined["cik"] = combined["cik"].astype(int)
            combined["cusip8"] = combined["cusip8"].astype(str)
            combined["accession_number"] = combined["accession_number"].fillna("").astype(str)
            combined["form"] = combined["form"].fillna("").astype(str)
            combined["cusip6"] = combined["cusip6"].fillna("").astype(str)
            combined["cusip9"] = combined["cusip9"].fillna("").astype(str)

            records: list[dict[str, object]] = []
            grouped = combined.sort_values("filing_date").groupby(
                ["cik", "cusip8"], sort=True
            )
            for (cik, cusip8), group in grouped:
                group = group.copy()
                group = group.sort_values(
                    ["filing_date", "accession_number"],
                    ascending=[True, True],
                )
                filing_dates = group["filing_date"]
                first_seen = filing_dates.iloc[0]
                last_seen = filing_dates.iloc[-1]
                months_active = int(
                    filing_dates.dt.to_period("M").dropna().nunique()
                )
                forms = sorted(
                    {
                        str(form)
                        for form in group["form"].dropna().astype(str)
                        if str(form)
                    }
                )
                most_recent = group.sort_values(
                    ["filing_date", "accession_number"],
                    ascending=[False, False],
                ).iloc[0]
                cusip6 = next(
                    (value for value in group["cusip6"].dropna() if value),
                    "",
                )
                cusip9 = next(
                    (value for value in group["cusip9"].dropna() if value),
                    "",
                )
                methods = sorted(
                    {
                        str(method)
                        for method in group["parse_method"].dropna().astype(str)
                        if str(method)
                    }
                )
                fallback_count = sum(
                    method != "window"
                    for method in group["parse_method"].fillna("")
                )
                valid_check_digit = _has_valid_cusip_check_digit(cusip9)

                records.append(
                    {
                        "cik": int(cik),
                        "cusip6": cusip6,
                        "cusip8": cusip8,
                        "cusip9": cusip9,
                        "first_seen": first_seen.date().isoformat(),
                        "last_seen": last_seen.date().isoformat(),
                        "filings_count": int(len(group)),
                        "forms": ";".join(forms),
                        "months_active": months_active,
                        "most_recent_accession": most_recent.get(
                            "accession_number", ""
                        ),
                        "most_recent_form": most_recent.get("form", ""),
                        "most_recent_filing_date": most_recent["filing_date"].date().isoformat(),
                        "valid_check_digit": valid_check_digit,
                        "parse_methods": ";".join(methods),
                        "fallback_filings": int(fallback_count),
                    }
                )

            result = pd.DataFrame.from_records(records)
            if not result.empty:
                result = result.sort_values(["cik", "cusip8"]).reset_index(drop=True)

    if output is not None:
        output_path = Path(output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        result.to_csv(output_path, index=False, encoding="utf-8")

    return result


def _has_valid_cusip_check_digit(cusip: str | None) -> bool:
    """Return ``True`` when ``cusip`` has a valid Modulus-10 check digit."""

    if not cusip or len(cusip) != 9 or not cusip[:-1].isalnum():
        return False
    body = cusip.upper()[:-1]
    try:
        values = [_character_to_cusip_value(ch) for ch in body]
    except ValueError:
        return False

    total = 0
    for index, value in enumerate(values, start=1):
        if index % 2 == 0:
            value *= 2
        total += value // 10 + value % 10
    check_digit = (10 - (total % 10)) % 10
    return cusip[-1] == str(check_digit)


def _character_to_cusip_value(character: str) -> int:
    """Return the integer value associated with a CUSIP character."""

    if character.isdigit():
        return int(character)
    if character.isalpha():
        return ord(character.upper()) - 55
    raise ValueError(f"Invalid CUSIP character: {character!r}")
