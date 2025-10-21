"""High-level pipeline orchestration helpers."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Sequence

if TYPE_CHECKING:  # pragma: no cover - typing helper
    import pandas as pd

from . import indexing, parsing, postprocessing, streaming


def run_pipeline(
    forms: Sequence[str] = ("13D", "13G"),
    *,
    output_root: Path | str = Path("."),
    output_file: Path | str = Path("cik-cusip-maps.csv"),
    requests_per_second: float = 10.0,
    sec_name: str | None = None,
    sec_email: str | None = None,
    skip_index: bool = False,
    skip_download: bool = False,
    skip_parse: bool = False,
    index_path: Path | str | None = None,
    concurrent_parsing: bool = True,
    debug: bool = False,
) -> "pd.DataFrame":
    """Run the end-to-end CIK to CUSIP mapping pipeline."""

    base_path = Path(output_root)
    base_path.mkdir(parents=True, exist_ok=True)

    resolved_index_path = Path(index_path) if index_path else base_path / "full_index.csv"
    master_path = base_path / "master.idx"

    if skip_index:
        if not resolved_index_path.exists():
            raise FileNotFoundError(
                f"full_index.csv not found. Remove skip_index or generate {resolved_index_path} first."
            )
    else:
        indexing.download_master_index(
            requests_per_second,
            sec_name,
            sec_email,
            output_path=master_path,
        )
        indexing.write_full_index(master_path=master_path, output_path=resolved_index_path)

    csv_paths: list[Path] = []
    skip_streaming = skip_download or skip_parse
    for form in forms:
        csv_path = base_path / f"{form}.csv"
        if skip_streaming:
            if not csv_path.exists():
                raise FileNotFoundError(
                    f"Expected CSV {csv_path} not found. Remove skip flags or generate it first."
                )
        else:
            filings = streaming.stream_filings(
                form,
                requests_per_second,
                sec_name,
                sec_email,
                index_path=resolved_index_path,
            )
            parsing.stream_to_csv(
                filings,
                csv_path,
                debug=debug,
                concurrent=concurrent_parsing,
            )
        csv_paths.append(csv_path)

    output_path = Path(output_file)
    if not output_path.is_absolute():
        output_path = base_path / output_path

    result = postprocessing.postprocess_mappings(csv_paths, output=output_path)
    return result
