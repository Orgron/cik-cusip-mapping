"""High-level pipeline orchestration helpers."""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING, Sequence

import requests
import polars as pl

if TYPE_CHECKING:  # pragma: no cover - typing helper
    import polars as pl

from . import indexing, parsing, postprocessing, streaming
from .sec import create_session


def _count_event_rows(events_path: Path | str) -> int:
    """Return the number of event rows in ``events_path`` excluding the header."""

    path = Path(events_path)
    if not path.exists():
        return 0
    with path.open("r", encoding="utf-8") as handle:
        rows = sum(1 for _ in handle)
    return max(rows - 1, 0)


def count_index_rows(form: str, index_path: Path | str) -> int:
    """Return the number of index rows matching ``form`` using Polars."""

    path = Path(index_path)
    if not path.exists():
        return 0
    lf = pl.scan_csv(str(path))
    out = (
        lf.filter(pl.col("form").str.contains(form))
        .select(pl.len())
        .collect()
        .item()
    )
    return int(out)


def count_index_rows_multi(
    forms: Sequence[str], index_path: Path | str
) -> dict[str, int]:
    """Return counts for each form in ``forms`` via a single Polars scan."""

    path = Path(index_path)
    if not path.exists():
        return {form: 0 for form in forms}
    lf = pl.scan_csv(str(path))
    # Build a single lazy plan computing all counts, then collect once.
    exprs = [
        pl.col("form").str.contains(form).sum().alias(form) for form in forms
    ]
    out = lf.select(exprs).collect()
    return {name: int(out[name][0]) for name in out.columns}


def run_pipeline(
    forms: Sequence[str] = ("13D", "13G"),
    *,
    output_root: Path | str = Path("."),
    output_file: Path | str = Path("cik-cusip-maps.csv"),
    write_final_mapping: bool = False,
    emit_dynamics: bool = True,
    events_output_root: Path | str = Path("."),
    dynamics_output_file: Path | str = Path("cik-cusip-dynamics.csv"),
    requests_per_second: float = 10.0,
    sec_name: str | None = None,
    sec_email: str | None = None,
    skip_index: bool = False,
    skip_download: bool = False,
    skip_parse: bool = False,
    skip_existing_events: bool = False,
    index_path: Path | str | None = None,
    concurrent_parsing: bool = True,
    debug: bool = False,
    parsing_workers: int = 2,
    parsing_max_queue: int = 32,
    show_progress: bool = True,
    use_notebook: bool | None = None,
    session: requests.Session | None = None,
) -> tuple["pl.DataFrame", "pl.DataFrame | None", dict[str, int]]:
    """Run the end-to-end CIK to CUSIP mapping pipeline."""

    env_sec_name = os.getenv("SEC_NAME")
    env_sec_email = os.getenv("SEC_EMAIL")

    resolved_sec_name = sec_name or env_sec_name
    resolved_sec_email = sec_email or env_sec_email

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
                resolved_sec_name,
                resolved_sec_email,
                output_path=master_path,
                session=http_session,
                show_progress=show_progress,
                use_notebook=use_notebook,
            )
            indexing.write_full_index(
                master_path=master_path,
                output_path=resolved_index_path,
                show_progress=show_progress,
                use_notebook=use_notebook,
            )

        events_paths: list[Path] = []
        skip_streaming = skip_download or skip_parse
        events_base_path = Path(events_output_root)
        if not events_base_path.is_absolute():
            events_base_path = base_path / events_base_path
        events_base_path.mkdir(parents=True, exist_ok=True)
        # Estimate totals quickly using a single Polars scan when index exists.
        form_totals: dict[str, int | None]
        if resolved_index_path.exists():
            try:
                form_totals = count_index_rows_multi(forms, resolved_index_path)
            except Exception:
                # Fall back to unknown totals if anything goes wrong
                form_totals = {form: None for form in forms}
        else:
            form_totals = {form: None for form in forms}
        events_counts: dict[str, int] = {}
        for form in forms:
            events_path = events_base_path / f"{form}_events.csv"
            if skip_streaming:
                if not events_path.exists():
                    raise FileNotFoundError(
                        f"Expected events CSV {events_path} not found. Remove skip flags or generate it first."
                    )
                events_counts[form] = _count_event_rows(events_path)
            else:
                if skip_existing_events and events_path.exists():
                    existing_rows = _count_event_rows(events_path)
                    if existing_rows > 0:
                        events_counts[form] = existing_rows
                        events_paths.append(events_path)
                        continue
                # Keep streaming progress quiet and parsing progress enabled to
                # avoid double progress bars (maintains CLI/test expectations).
                parsing_show_progress = show_progress
                stream_show_progress = show_progress and not parsing_show_progress

                filings = streaming.stream_filings(
                    form,
                    requests_per_second,
                    resolved_sec_name,
                    resolved_sec_email,
                    index_path=resolved_index_path,
                    session=http_session,
                    show_progress=stream_show_progress,
                    progress_desc=f"Streaming {form} filings",
                    total_hint=form_totals.get(form),
                    use_notebook=use_notebook,
                )
                events_counts[form] = parsing.stream_events_to_csv(
                    filings,
                    events_path,
                    debug=debug,
                    concurrent=concurrent_parsing,
                    max_queue=parsing_max_queue,
                    workers=parsing_workers,
                    show_progress=parsing_show_progress,
                    total_hint=form_totals.get(form),
                    use_notebook=use_notebook,
                )
            events_paths.append(events_path)

        output_path = Path(output_file)
        if not output_path.is_absolute():
            output_path = base_path / output_path

        mapping = postprocessing.postprocess_mapping_from_events(
            events_paths, output=output_path if write_final_mapping else None
        )

        dynamics = None
        if emit_dynamics:
            dynamics_output_path = Path(dynamics_output_file)
            if not dynamics_output_path.is_absolute():
                dynamics_output_path = base_path / dynamics_output_path
            dynamics = postprocessing.build_cusip_dynamics(
                events_paths, output=dynamics_output_path
            )

        return mapping, dynamics, events_counts
    finally:
        if created_session:
            http_session.close()
