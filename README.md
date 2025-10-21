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

## Library usage

The full pipeline is exposed as a composable function. After installation, orchestrate a run entirely from Python:

```python
from pathlib import Path

from cik_cusip_mapping import run_pipeline

df = run_pipeline(
    forms=("13D", "13G"),
    output_root=Path("data"),
    requests_per_second=5,
    sec_name="Jane Doe",
    sec_email="jane@example.com",
)

print(df.head())
```

The function returns a `pandas.DataFrame` with the columns `cik`, `cusip6`, and `cusip8`, and also writes the result to `output_root / "cik-cusip-maps.csv"` (or to a custom path supplied via `output_file`).

Behind the scenes the package exposes dedicated primitives for each stage:

* `download_master_index()` and `write_full_index()` handle EDGAR index collection.
* `stream_filings()` yields filing metadata and text while honouring the SEC rate limit.
* `parse_filings_concurrently()` parses filings in parallel with downloading to keep the network and CPU busy simultaneously.
* `postprocess_mappings()` combines per-form CSVs into the final mapping DataFrame.

These functions can be mixed and matched to build bespoke workflows without shelling out to subprocesses.

## Command-line entry points

Lightweight console commands are available once the package is installed:

```
cik-cusip-run-pipeline --help
```

Each command delegates to the corresponding library function, providing convenient access for quick experiments while keeping the core implementation import-friendly.

## Running the automated tests

Install the development requirements and execute pytest:

```
pip install -r requirements-dev.txt
pytest
```

The test suite exercises the library APIs directly, including the new concurrent parsing hand-off, so regressions are caught as the architecture evolves.

## Obtaining the mapping

If you simply need the latest mapping, the repository still publishes a generated `cik-cusip-maps.csv`. You may also install the package and call `run_pipeline()` to refresh the dataset yourself. Downstream users remain responsible for any business-specific rules (for example, interpolating or extrapolating the validity window for each CUSIP).
