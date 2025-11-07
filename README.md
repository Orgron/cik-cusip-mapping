# cik-cusip mapping

This project produces an open CIK → CUSIP mapping by downloading the SEC EDGAR master index, streaming 13D/13G filings, parsing CUSIP identifiers, and post-processing the results into a deduplicated table.

## Installation

The repository is now structured as an installable package. Install it in editable mode while developing, or as a standard dependency in downstream projects:

```
pip install -e .            # editable install for local development
# or
pip install cik-cusip-mapping
```

The package targets **Python 3.12** and depends on `pandas` for post-processing.

## SEC usage etiquette

Please follow the [SEC fair access policies](https://www.sec.gov/os/webmaster-fair-access).
Always identify yourself via the `User-Agent` and `From` headers, which you can
provide through `run_pipeline()` or the CLI via `--sec-name`/`--sec-email`. When
these options are omitted the pipeline will fall back to the `SEC_NAME` and
`SEC_EMAIL` environment variables if they are set. The default rate limit is 10
requests per second; adjust `requests_per_second` to a lower value if you are
running large historical backfills or operating from a shared IP address.

## Library usage

The full pipeline is exposed as a composable function. After installation, orchestrate a run entirely from Python:

```python
from pathlib import Path

from cik_cusip_mapping import create_session, run_pipeline

session = create_session()
try:
    mapping, dynamics, events_counts = run_pipeline(
        forms=("13D", "13G"),
        output_root=Path("data"),
        requests_per_second=5,
        sec_name="Jane Doe",
        sec_email="jane@example.com",
        show_progress=False,
        write_final_mapping=True,
        session=session,
    )
finally:
    session.close()

print(mapping.head())
print(events_counts)
```

The function returns a `pandas.DataFrame` with the columns `cik`, `cusip6`, and `cusip8`, the optional dynamics DataFrame (or `None` when disabled), and a dictionary summarising how many filing events were written per form. Disk output now consists of per-form events CSVs (e.g. `13D_events.csv`); a consolidated mapping CSV is only written when `write_final_mapping=True`.

When rerunning the pipeline you can reuse existing per-form CSVs by passing `skip_existing_events=True` (or `--skip-parsed-forms` via the CLI). The pipeline will reuse prior results when the CSV already contains event rows, skipping redundant downloads while keeping row counts and post-processing outputs accurate.

Behind the scenes the package exposes dedicated primitives for each stage:

* `download_master_index()` and `write_full_index()` handle EDGAR index collection.
* `stream_filings()` yields filing metadata and text while honouring the SEC rate limit.
* `parse_filings_concurrently()` parses filings in parallel with downloading to keep the network and CPU busy simultaneously.
* `stream_events_to_csv()` writes per-form events CSVs with derived CUSIP details.
* `postprocess_mapping_from_events()` derives the final mapping directly from those events.

You can reuse a single `requests.Session` across stages to benefit from
connection pooling and automatic retry/backoff logic:

```python
from cik_cusip_mapping import create_session, parsing, streaming

session = create_session()
try:
    filings = streaming.stream_filings(
        "13D",
        requests_per_second=5,
        name="Jane Doe",
        email="jane@example.com",
        session=session,
        show_progress=False,
    )
    parsing.stream_events_to_csv(
        filings,
        "13D_events.csv",
        concurrent=True,
        workers=4,
        max_queue=64,
        show_progress=False,
    )
finally:
    session.close()
```

### Batch workflows

The streaming helpers also support disk-first workflows when you prefer to
persist filings locally before parsing:

```python
from pathlib import Path

from cik_cusip_mapping import create_session, parsing, streaming

session = create_session()
try:
    count = streaming.stream_filings_to_disk(
        "13G",
        Path("raw_filings"),
        requests_per_second=5,
        name="Jane Doe",
        email="jane@example.com",
        session=session,
        compress=True,
    )
    print(f"Downloaded {count} filings")
    parsing.parse_directory(
        Path("raw_filings"),
        output_csv=Path("parsed.csv"),
        concurrent=True,
        workers=4,
        glob_pattern="**/*.txt.gz",
        show_progress=False,
    )
finally:
    session.close()
```

`parse_directory` accepts a configurable glob pattern, so nested directory
structures (or compressed `.gz` files) are easy to handle.

`postprocess_mapping_from_events()` exposes the same
filtering controls as the legacy CSV workflow, while `build_cusip_dynamics()` now emits
`valid_check_digit`, `parse_methods`, and `fallback_filings` columns to help
with downstream quality filtering. The per-event `parse_method` field
distinguishes the primary window-based extraction from fallback heuristics so
you can decide which events to trust.

### Event CSV schema and URL reconstruction

Every per-form events file contains the header:

```
cik,form,filing_date,accession_number,company_name,cusip9,cusip8,cusip6,parse_method
```

The combination of `cik` and `accession_number` uniquely identifies each
filing. Use `cusip9`, `cusip8`, and `cusip6` to derive mappings or aggregate
filing histories. When you need to link back to EDGAR, reconstruct the filing
index URL on-demand:

```python
from cik_cusip_mapping import reconstruct_filing_url

url = reconstruct_filing_url("0000123456", "0000123456-23-000001")
print(url)  # https://www.sec.gov/Archives/edgar/data/123456/000012345623000001/0000123456-23-000001-index.html
```

## Command-line entry points

Lightweight console commands are available once the package is installed:

```
cik-cusip-run-pipeline --help
```

Each command delegates to the corresponding library function, providing convenient access for quick experiments while keeping the core implementation import-friendly. Use `--parsing-workers`, `--parsing-max-queue`, and the `--no-progress`/`--quiet-progress` flags to tune parsing throughput or disable progress bars across indexing, streaming, and parsing when running in non-interactive environments. Pass `--use-notebook` (or `--no-use-notebook`) to override the auto-detected tqdm widget consistently for every stage. By default the pipeline presents a single parsing progress bar so streaming work does not flicker or nest multiple displays. Supply `--write-final-mapping` if you want a consolidated `cik-cusip-maps.csv` alongside the per-form events outputs.

## Running the automated tests

Install the development requirements and execute pytest:

```
pip install -r requirements-dev.txt
pytest
```

The test suite exercises the library APIs directly, including the new concurrent parsing hand-off, so regressions are caught as the architecture evolves.

## Obtaining the mapping

If you simply need the latest mapping, the repository still publishes a generated `cik-cusip-maps.csv`. You may also install the package and call `run_pipeline()` to refresh the dataset yourself—remember to pass `write_final_mapping=True` (or `--write-final-mapping` via the CLI) if you want a consolidated mapping on disk; otherwise only per-form events CSVs are written. Downstream users remain responsible for any business-specific rules (for example, interpolating or extrapolating the validity window for each CUSIP).
