# CIK-CUSIP Mapping

A professional CLI tool for extracting CUSIP identifiers from SEC 13D and 13G filings.

## What it does

This tool:
1. Downloads SEC EDGAR master indices (current quarter or historical data from 1993-present)
2. Filters for 13D and 13G forms
3. Downloads each filing
4. Parses CUSIP identifiers from the filing text
5. Writes results to a CSV file with accession numbers
6. Provides a convenient command to download individual filings by CIK and accession number

All while respecting SEC rate limits and authorization requirements.

## Installation

### Requirements

- Python 3.9 or higher
- `requests` and `click` libraries

### Install the package

```bash
pip install -e .
```

This installs the `cik-cusip` command globally on your system.

### Development installation

For development with testing tools:

```bash
pip install -e ".[dev]"
```

## SEC Usage Etiquette

**IMPORTANT**: You must follow the [SEC fair access policies](https://www.sec.gov/os/webmaster-fair-access).

You must identify yourself via proper User-Agent and From headers. The tool requires:
- Your name
- Your email address

You can provide these via:
1. Command-line arguments: `--sec-name "Your Name" --sec-email "your@email.com"`
2. Environment variables: `SEC_NAME` and `SEC_EMAIL`

The default rate limit is 10 requests per second. Use `--rate` to adjust if needed.

## Usage

### Command Line Interface

After installation, use the `cik-cusip` command:

#### Extract CUSIPs from filings

Basic usage (downloads current quarter only):

```bash
cik-cusip extract --sec-name "Jane Doe" --sec-email "jane@example.com"
```

Download all historical indices (1993 to present):

```bash
cik-cusip extract --all --sec-name "Jane Doe" --sec-email "jane@example.com"
```

Download specific year range:

```bash
cik-cusip extract \
  --start-year 2020 \
  --end-year 2024 \
  --sec-name "Jane Doe" \
  --sec-email "jane@example.com"
```

Download specific quarter range:

```bash
cik-cusip extract \
  --start-year 2023 \
  --start-quarter 3 \
  --end-year 2024 \
  --end-quarter 2 \
  --sec-name "Jane Doe" \
  --sec-email "jane@example.com"
```

With additional options:

```bash
cik-cusip extract \
  --index-dir data/indices \
  --output data/cusips.csv \
  --skip-index \
  --skip-existing \
  --rate 5 \
  --all \
  --sec-name "Jane Doe" \
  --sec-email "jane@example.com"
```

Skip re-processing already-extracted filings (incremental updates):

```bash
cik-cusip extract \
  --skip-existing \
  --all \
  --sec-name "Jane Doe" \
  --sec-email "jane@example.com"
```

Filter for specific CIKs only:

```bash
# Create a file with CIKs (one per line)
echo "1234567" > my_ciks.txt
echo "9876543" >> my_ciks.txt

cik-cusip extract \
  --cik-filter my_ciks.txt \
  --all \
  --sec-name "Jane Doe" \
  --sec-email "jane@example.com"
```

Using environment variables:

```bash
export SEC_NAME="Jane Doe"
export SEC_EMAIL="jane@example.com"
cik-cusip extract --all
```

#### Download a specific filing by CIK and accession number

Download a filing in text format (requires both CIK and accession number):

```bash
cik-cusip download 813828 0001104659-06-026838 \
  --sec-name "Jane Doe" \
  --sec-email "jane@example.com"
```

With custom output path:

```bash
cik-cusip download 813828 0001104659-06-026838 \
  -o my-filing.txt \
  --sec-name "Jane Doe" \
  --sec-email "jane@example.com"
```

Using environment variables:

```bash
export SEC_NAME="Jane Doe"
export SEC_EMAIL="jane@example.com"
cik-cusip download 813828 0001104659-06-026838
```

### As a Python Library

```python
from cik_cusip import process_filings, download_filing_txt

# Process filings and extract CUSIPs (current quarter only)
process_filings(
    index_dir='data/indices',
    output_csv='data/cusips.csv',
    forms=('13D', '13G'),
    sec_name='Jane Doe',
    sec_email='jane@example.com',
    requests_per_second=10.0,
    skip_index_download=True,  # Skip if indices already exist
)

# Download all historical data (1993-present)
process_filings(
    index_dir='data/indices',
    output_csv='data/cusips.csv',
    forms=('13D', '13G'),
    sec_name='Jane Doe',
    sec_email='jane@example.com',
    start_year=1993,  # All historical data
)

# Download specific year range
process_filings(
    index_dir='data/indices',
    output_csv='data/cusips.csv',
    forms=('13D', '13G'),
    sec_name='Jane Doe',
    sec_email='jane@example.com',
    start_year=2020,
    end_year=2024,
)

# Filter for specific CIKs only
process_filings(
    index_dir='data/indices',
    output_csv='data/cusips.csv',
    forms=('13D', '13G'),
    sec_name='Jane Doe',
    sec_email='jane@example.com',
    cik_filter_file='my_ciks.txt',  # Only process these CIKs
    start_year=2020,
    end_year=2024,
)

# Skip already-processed filings (incremental updates)
process_filings(
    index_dir='data/indices',
    output_csv='data/cusips.csv',
    forms=('13D', '13G'),
    sec_name='Jane Doe',
    sec_email='jane@example.com',
    skip_existing=True,  # Skip filings already in the output CSV
    start_year=2020,
    end_year=2024,
)

# Download a specific filing by CIK and accession number
download_filing_txt(
    cik='813828',
    accession_number='0001104659-06-026838',
    output_path='filing.txt',
    sec_name='Jane Doe',
    sec_email='jane@example.com',
)
```

Or use individual functions:

```python
from cik_cusip import (
    create_session,
    download_indices,
    parse_index,
    extract_cusip,
    extract_accession_number,
)

# Create SEC-compliant session
session = create_session('Jane Doe', 'jane@example.com')

# Download multiple indices (2020-2024)
index_paths = download_indices(
    'data/indices',
    session,
    start_year=2020,
    end_year=2024,
    skip_if_exists=True
)

# Parse all indices for 13D/13G forms
all_entries = []
for index_path in index_paths:
    entries = parse_index(index_path, forms=('13D', '13G'))
    all_entries.extend(entries)

# Process individual filing
for entry in all_entries[:10]:  # First 10 as example
    response = session.get(entry['url'])
    cusip = extract_cusip(response.text)
    accession = entry['accession_number']
    print(f"{entry['cik']}: CUSIP={cusip}, Accession={accession}")
```

## Output

The tool generates a CSV file with the following columns:

- `cik`: Central Index Key (SEC identifier)
- `company_name`: Company name from filing
- `form`: Form type (13D, SC 13D, 13G, SC 13G, etc.)
- `date`: Filing date
- `cusip`: Extracted CUSIP identifier (8-10 characters)
- `accession_number`: SEC accession number for the filing

Example output:

```csv
cik,company_name,form,date,cusip,accession_number
0001234567,EXAMPLE CORP,SC 13D,2024-01-15,12345678,0001234567-24-000001
0007654321,ANOTHER COMPANY,SC 13G,2024-01-20,87654321,0007654321-24-000002
```

## Command Line Options

### Extract command

```
cik-cusip extract [OPTIONS]

Options:
  --index-dir PATH          Directory for index files (default: data/indices)
  --output PATH             Path to output CSV (default: data/cusips.csv)
  --skip-index              Skip index download if files exist
  --skip-existing           Skip forms that are already in the output CSV
  --sec-name TEXT           Your name for SEC User-Agent (or set SEC_NAME env var)
  --sec-email TEXT          Your email for SEC headers (or set SEC_EMAIL env var)
  --rate FLOAT              Requests per second (default: 10.0)
  --cik-filter PATH         Path to text file with CIKs to filter (one per line)
  --all                     Download all available indices (1993 to present)
  --start-year INTEGER      Starting year (default: current year if not --all)
  --start-quarter [1|2|3|4] Starting quarter (default: 1)
  --end-year INTEGER        Ending year (default: current year)
  --end-quarter [1|2|3|4]   Ending quarter (default: current quarter)
  --help                    Show this message and exit
```

### Download command

```
cik-cusip download [OPTIONS] CIK ACCESSION_NUMBER

Arguments:
  CIK                  Company's Central Index Key (e.g., 813828)
  ACCESSION_NUMBER     SEC accession number (e.g., 0001104659-06-026838)

Options:
  -o, --output PATH    Output file path (default: {accession_number}.txt)
  --sec-name TEXT      Your name for SEC User-Agent (or set SEC_NAME env var)
  --sec-email TEXT     Your email for SEC headers (or set SEC_EMAIL env var)
  --help               Show this message and exit
```

## Project Structure

```
cik-cusip-mapping/
├── src/
│   └── cik_cusip/
│       ├── __init__.py          # Package exports
│       ├── cli.py               # CLI commands (extract, download)
│       ├── rate_limiter.py      # Rate limiting for SEC requests
│       ├── session.py           # SEC-compliant session creation
│       ├── index.py             # Index download and parsing
│       ├── cusip.py             # CUSIP extraction and validation
│       ├── processor.py         # Main processing orchestration
│       └── utils.py             # Utility functions
├── tests/
│   ├── test_cli.py              # CLI tests
│   ├── test_rate_limiter.py     # Rate limiter tests
│   ├── test_session.py          # Session tests
│   ├── test_index.py            # Index tests
│   ├── test_cusip.py            # CUSIP extraction tests
│   ├── test_processor.py        # Processor tests
│   └── test_utils.py            # Utility tests
├── pyproject.toml               # Package configuration
├── README.md                    # This file
└── AGENTS.md                    # Guide for AI agents
```

## How It Works

### 1. Index Download
Downloads SEC EDGAR master index files for the specified time range (current quarter by default, or all historical data from 1993-present with `--all`), which contains metadata for all filings.

### 2. Index Parsing
Parses all downloaded index files and filters for 13D and 13G form types (including SC 13D, SC 13G, and amended versions). Extracts accession numbers from filing URLs.

### 3. Filing Processing
For each matching filing:
- Downloads the filing from SEC EDGAR (with rate limiting)
- Extracts CUSIP using pattern matching
- Extracts accession number from filing URL
- Writes result to CSV

### 4. CUSIP Extraction
The CUSIP extraction algorithm:
- First looks for explicit "CUSIP" markers in the text
- Searches a window around these markers for valid CUSIP patterns
- Falls back to document-wide search if needed
- Validates candidates (length, character composition, excludes false positives)
- Returns the most likely CUSIP identifier

## CIK Filtering

You can filter filings to process only specific CIKs by providing a text file with the `--cik-filter` option:

1. Create a text file with one CIK per line:
   ```
   1234567
   9876543
   0001234567
   ```

2. Use the filter when running the tool:
   ```bash
   cik-cusip extract --cik-filter my_ciks.txt --all
   ```

This is useful when you only need to track filings for specific companies and want to reduce processing time.

## Incremental Updates

The `--skip-existing` flag allows you to perform incremental updates without re-processing filings that are already in your output CSV:

```bash
# Initial extraction
cik-cusip extract --start-year 2020 --end-year 2023

# Later, update with 2024 data without re-processing 2020-2023
cik-cusip extract --skip-existing --start-year 2020 --end-year 2024
```

How it works:
- The tool reads the existing output CSV and identifies all accession numbers already processed
- New filings are filtered to exclude those already in the CSV
- After processing, existing and new results are merged into the output file
- This significantly reduces processing time when updating your dataset

Use cases:
- Periodic updates (e.g., quarterly data updates)
- Resuming interrupted extractions
- Adding new quarters to an existing dataset
- Re-running with expanded date ranges

## Progress Display

The tool displays a real-time progress bar while processing filings with the following information:
- **Progress**: Current filing number and percentage complete
- **ETA**: Estimated time remaining to complete processing
- **Success/Failed counts**: Number of successful CUSIP extractions vs. failures
- **Latest filing**: Shows the most recently processed company and result

Example output:
```
[150/1000] 15.0% | ETA:   12m 34s | Success: 142 | Failed: 8 | Latest: ACME CORPORATION - ✓ CUSIP: 68389X105
```

## Rate Limiting

The tool uses a token bucket algorithm to respect SEC rate limits:
- Default: 10 requests per second
- Configurable via `--rate` argument
- Includes exponential backoff for failed requests
- Retries on 429, 500, 502, 503, 504 status codes

## Testing

Run the test suite:

```bash
pytest
```

With coverage:

```bash
pytest --cov=cik_cusip --cov-report=html
```

## License

MIT License - See LICENSE file for details.
