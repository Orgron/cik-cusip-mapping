"""Post-processing helpers for the parsed CIK to CUSIP mappings."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

try:  # pragma: no cover - exercised when pandas is unavailable in the test environment
    import pandas as pd
except ModuleNotFoundError:  # pragma: no cover - fallback for minimal environments
    pd = None  # type: ignore[assignment]


def postprocess_mappings(
    csv_paths: Iterable[Path | str],
    *,
    output: Path | str | None = None,
) -> pd.DataFrame:
    """Combine per-form CSV files into the final mapping DataFrame."""

    if pd is None:  # pragma: no cover - defensive guard for minimal environments
        raise RuntimeError("pandas is required to post-process CIK/CUSIP mappings")

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
