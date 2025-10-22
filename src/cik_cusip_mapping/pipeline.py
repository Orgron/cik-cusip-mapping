"""High-level pipeline orchestration helpers."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Sequence

import requests

if TYPE_CHECKING:  # pragma: no cover - typing helper
    import pandas as pd

from . import indexing, parsing, postprocessing, streaming
from .sec import create_session


def run_pipeline(
    forms: Sequence[str] = ("13D", "13G"),
    *,
    output_root: Path | str = Path("."),
    output_file: Path | str = Path("cik-cusip-maps.csv"),
    emit_dynamics: bool = True,
    events_output_root: Path | str = Path("."),
    dynamics_output_file: Path | str = Path("cik-cusip-dynamics.csv"),
    requests_per_second: float = 10.0,
    sec_name: str | None = None,
    sec_email: str | None = None,
    skip_index: bool = False,
    skip_download: bool = False,
    skip_parse: bool = False,
    index_path: Path | str | None = None,
    concurrent_parsing: bool = True,
    debug: bool = False,
    parsing_workers: int = 2,
    parsing_max_queue: int = 32,
    show_progress: bool = True,
    session: requests.Session | None = None,
) -> tuple["pd.DataFrame", "pd.DataFrame | None"]:
    """Run the end-to-end CIK to CUSIP mapping pipeline."""

    base_path = Path(output_root)
    base_path.mkdir(parents=True, exist_ok=True)

    resolved_index_path = (
        Path(index_path) if index_path else base_path / "full_index.csv"
    )
    master_path = base_path / "master.idx"

    created_session = session is None
    http_session = session or create_session()

    try:
        if skip_index:
            if not resolved_index_path.exists():
                raise FileNotFoundError(
                    "full_index.csv not found. Remove skip_index or generate"
                    f" {resolved_index_path} first."
                )
        else:
            indexing.download_master_index(
                requests_per_second,
                sec_name,
                sec_email,
                output_path=master_path,
                session=http_session,
            )
            indexing.write_full_index(
                master_path=master_path, output_path=resolved_index_path
            )

        csv_paths: list[Path] = []
        events_paths: list[Path] = []
        skip_streaming = skip_download or skip_parse
        events_base_path = Path(events_output_root)
        if not events_base_path.is_absolute():
            events_base_path = base_path / events_base_path
        if emit_dynamics:
            events_base_path.mkdir(parents=True, exist_ok=True)
        for form in forms:
            csv_path = base_path / f"{form}.csv"
            events_path = (
                events_base_path / f"{form}_events.csv" if emit_dynamics else None
            )
            if skip_streaming:
                if not csv_path.exists():
                    raise FileNotFoundError(
                        f"Expected CSV {csv_path} not found. Remove skip flags or generate it first."
                    )
                if (
                    emit_dynamics
                    and events_path is not None
                    and not events_path.exists()
                ):
                    raise FileNotFoundError(
                        f"Expected events CSV {events_path} not found. Remove skip flags or generate it first."
                    )
            else:
                filings = streaming.stream_filings(
                    form,
                    requests_per_second,
                    sec_name,
                    sec_email,
                    index_path=resolved_index_path,
                    session=http_session,
                    show_progress=show_progress,
                    progress_desc=f"Streaming {form} filings",
                )
                parsing.stream_to_csv(
                    filings,
                    csv_path,
                    debug=debug,
                    concurrent=concurrent_parsing,
                    events_csv_path=events_path,
                    max_queue=parsing_max_queue,
                    workers=parsing_workers,
                    show_progress=show_progress,
                )
            csv_paths.append(csv_path)
            if events_path is not None:
                events_paths.append(events_path)

        output_path = Path(output_file)
        if not output_path.is_absolute():
            output_path = base_path / output_path

        mapping = postprocessing.postprocess_mappings(
            csv_paths, output=output_path
        )

        dynamics = None
        if emit_dynamics:
            dynamics_output_path = Path(dynamics_output_file)
            if not dynamics_output_path.is_absolute():
                dynamics_output_path = base_path / dynamics_output_path
            dynamics = postprocessing.build_cusip_dynamics(
                events_paths, output=dynamics_output_path
            )

        return mapping, dynamics
    finally:
        if created_session:
            http_session.close()
