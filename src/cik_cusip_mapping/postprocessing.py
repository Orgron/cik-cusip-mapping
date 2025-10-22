"""Post-processing helpers for the parsed CIK to CUSIP mappings."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

import polars as pl


def postprocess_mapping_from_events(
    events_paths: Iterable[Path | str],
    *,
    output: Path | str | None = None,
    valid_lengths: Iterable[int] | None = None,
    forbidden_prefixes: Iterable[str] | None = None,
) -> pl.DataFrame:
    """Derive the deduplicated CIK to CUSIP mapping directly from events."""

    frames: list[pl.DataFrame] = []
    for csv_path in events_paths:
        path = Path(csv_path)
        if not path.exists():
            continue
        frame = pl.read_csv(path, infer_schema_length=0)
        if frame.height == 0:
            continue
        frames.append(frame)

    length_whitelist = set(valid_lengths or {6, 8, 9})
    excluded_prefixes = {
        prefix.upper() for prefix in (forbidden_prefixes or {"000000", "0001PT"})
    }

    if not frames:
        result = pl.DataFrame({"cik": [], "cusip6": [], "cusip8": []})
    else:
        combined = pl.concat(frames, how="vertical_relaxed")
        combined = combined.drop_nulls(subset=["cik", "cusip8"])
        if combined.height == 0:
            result = pl.DataFrame({"cik": [], "cusip6": [], "cusip8": []})
        else:
            combined = combined.with_columns(
                [
                    pl.col("cik").cast(pl.Int64, strict=False),
                    pl.col("cusip8").cast(pl.Utf8).str.to_uppercase(),
                ]
            ).drop_nulls(subset=["cik"]).with_columns(
                pl.col("cik").cast(pl.Int64)
            )
            combined = combined.filter(pl.col("cusip8").str.len_chars() == 8)
            if combined.height == 0:
                result = pl.DataFrame({"cik": [], "cusip6": [], "cusip8": []})
            else:
                # Apply length whitelist to the source (cusip9 preferred, else cusip8)
                combined = combined.with_columns(
                    pl.coalesce([pl.col("cusip9"), pl.col("cusip8")])
                    .cast(pl.Utf8)
                    .str.len_chars()
                    .alias("_length")
                ).filter(pl.col("_length").is_in(list(length_whitelist)))
                combined = combined.drop("_length")
                if combined.height == 0:
                    result = pl.DataFrame({"cik": [], "cusip6": [], "cusip8": []})
                else:
                    # Normalize/derive cusip6
                    has_cusip6 = "cusip6" in combined.columns
                    if not has_cusip6:
                        combined = combined.with_columns(pl.lit("").alias("cusip6"))
                    combined = combined.with_columns(
                        pl.when(pl.col("cusip6").cast(pl.Utf8).str.len_chars() != 6)
                        .then(pl.col("cusip8").str.slice(0, 6))
                        .otherwise(pl.col("cusip6").cast(pl.Utf8))
                        .str.to_uppercase()
                        .alias("cusip6")
                    )
                    combined = combined.filter(
                        ~pl.col("cusip6").str.to_uppercase().is_in(excluded_prefixes)
                    )
                    if combined.height == 0:
                        result = pl.DataFrame({"cik": [], "cusip6": [], "cusip8": []})
                    else:
                        result = (
                            combined.select(["cik", "cusip6", "cusip8"]) 
                            .unique()
                            .sort(["cik", "cusip8"]) 
                        )

    if output is not None:
        output_path = Path(output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        result.write_csv(output_path)

    return result


def build_cusip_dynamics(
    events_paths: Iterable[Path | str],
    *,
    output: Path | str | None = None,
) -> pl.DataFrame:
    """Aggregate filing-level events into CUSIP dynamics metrics using Polars."""

    frames: list[pl.DataFrame] = []
    for csv_path in events_paths:
        path = Path(csv_path)
        if not path.exists():
            continue
        frame = pl.read_csv(path, infer_schema_length=0)
        if frame.height == 0:
            continue
        frames.append(frame)

    empty_schema = {
        "cik": pl.Int64,
        "cusip6": pl.Utf8,
        "cusip8": pl.Utf8,
        "cusip9": pl.Utf8,
        "first_seen": pl.Utf8,
        "last_seen": pl.Utf8,
        "filings_count": pl.Int64,
        "forms": pl.Utf8,
        "months_active": pl.Int64,
        "most_recent_accession": pl.Utf8,
        "most_recent_form": pl.Utf8,
        "most_recent_filing_date": pl.Utf8,
        "valid_check_digit": pl.Boolean,
        "parse_methods": pl.Utf8,
        "fallback_filings": pl.Int64,
    }

    if not frames:
        result = pl.DataFrame({name: [] for name in empty_schema.keys()})
    else:
        combined = pl.concat(frames, how="vertical_relaxed").with_columns(
            [
                pl.col("cik").cast(pl.Int64, strict=False),
                pl.col("filing_date").str.strptime(pl.Date, strict=False),
                pl.col("accession_number").cast(pl.Utf8).fill_null(""),
                pl.col("form").cast(pl.Utf8).fill_null(""),
                pl.col("cusip6").cast(pl.Utf8).fill_null(""),
                pl.col("cusip8").cast(pl.Utf8),
                pl.col("cusip9").cast(pl.Utf8).fill_null(""),
                pl.col("parse_method").cast(pl.Utf8),
            ]
        ).drop_nulls(subset=["cik", "cusip8", "filing_date"]).with_columns(
            pl.col("cik").cast(pl.Int64)
        )

        if combined.height == 0:
            result = pl.DataFrame({name: [] for name in empty_schema.keys()})
        else:
            # Sort so that group-wise first/last reflect chronological order
            combined = combined.sort(["filing_date", "accession_number"]) 

            grouped = combined.group_by(["cik", "cusip8"], maintain_order=True).agg(
                [
                    pl.col("filing_date").first().alias("first_seen"),
                    pl.col("filing_date").last().alias("last_seen"),
                    pl.count().alias("filings_count"),
                    pl.col("form").unique().sort().alias("forms_list"),
                    pl.col("filing_date")
                    .dt.strftime("%Y-%m")
                    .n_unique()
                    .alias("months_active"),
                    pl.col("accession_number").last().alias("most_recent_accession"),
                    pl.col("form").last().alias("most_recent_form"),
                    pl.col("filing_date").last().alias("most_recent_filing_date"),
                    pl.col("cusip6").filter(pl.col("cusip6") != "").first().alias("cusip6_opt"),
                    pl.col("cusip9").filter(pl.col("cusip9") != "").first().alias("cusip9_opt"),
                    pl.col("parse_method").unique().sort().alias("parse_methods_list"),
                    pl.col("parse_method").ne("window").sum().alias("fallback_filings"),
                ]
            )

            result = (
                grouped.with_columns(
                    [
                        pl.col("forms_list").list.join(";").alias("forms"),
                        pl.col("parse_methods_list").list.join(";").alias(
                            "parse_methods"
                        ),
                        pl.col("cusip6_opt").fill_null("").alias("cusip6"),
                        pl.col("cusip9_opt").fill_null("").alias("cusip9"),
                    ]
                )
                .with_columns(
                    [
                        pl.col("first_seen").dt.strftime("%Y-%m-%d"),
                        pl.col("last_seen").dt.strftime("%Y-%m-%d"),
                        pl.col("most_recent_filing_date").dt.strftime("%Y-%m-%d"),
                        pl.col("cusip9").map_elements(
                            _has_valid_cusip_check_digit, return_dtype=pl.Boolean
                        ).alias("valid_check_digit"),
                    ]
                )
                .select(
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
                .sort(["cik", "cusip8"]) 
            )

    if output is not None:
        output_path = Path(output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        result.write_csv(output_path)

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
