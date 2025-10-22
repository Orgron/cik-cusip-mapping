"""Post-processing helpers for the parsed CIK to CUSIP mappings."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd


def postprocess_mappings(
    csv_paths: Iterable[Path | str],
    *,
    output: Path | str | None = None,
) -> pd.DataFrame:
    """Combine per-form CSV files into the final mapping DataFrame."""

    frames: list[pd.DataFrame] = []
    for csv_path in csv_paths:
        path = Path(csv_path)
        frame = pd.read_csv(path, names=["filename", "cik", "cusip"]).dropna()
        if frame.empty:
            continue
        frame["leng"] = frame.cusip.map(len)
        frame = frame[frame.leng.isin({6, 8, 9})]
        if frame.empty:
            continue
        frame["cusip6"] = frame.cusip.str[:6]
        frame = frame[~frame.cusip6.isin({"000000", "0001pt"})]
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
        result.to_csv(output_path, index=False)

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
        frame = pd.read_csv(path, dtype=str)
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
                    }
                )

            result = pd.DataFrame.from_records(records)
            if not result.empty:
                result = result.sort_values(["cik", "cusip8"]).reset_index(drop=True)

    if output is not None:
        output_path = Path(output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        result.to_csv(output_path, index=False)

    return result
