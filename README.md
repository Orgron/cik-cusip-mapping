# CIK-CUSIP Mapping

A simple, focused tool for extracting CUSIP identifiers from SEC 13D and 13G filings.

## What it does

This tool:
1. Downloads the SEC EDGAR master index
2. Filters for 13D and 13G forms
3. Downloads each filing
4. Parses CUSIP identifiers from the filing text
5. Writes results to a CSV file

All while respecting SEC rate limits and authorization requirements.

## Installation

### Requirements

- Python 3.9 or higher
- `requests` library

### Install dependencies

```bash
pip install requests
```

Or if you want to install as a package:

```bash
pip install -e .
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

### Command Line

Basic usage:

```bash
python main.py --sec-name "Jane Doe" --sec-email "jane@example.com"
```

With options:

```bash
python main.py \
  --sec-name "Jane Doe" \
  --sec-email "jane@example.com" \
  --index data/master.idx \
  --output data/cusips.csv \
  --skip-index \
  --rate 5
```

Using environment variables:

```bash
export SEC_NAME="Jane Doe"
export SEC_EMAIL="jane@example.com"
python main.py
```

### As a Python Library

```python
from main import process_filings

# Process filings and extract CUSIPs
process_filings(
    index_path='data/master.idx',
    output_csv='data/cusips.csv',
    forms=('13D', '13G'),
    sec_name='Jane Doe',
    sec_email='jane@example.com',
    requests_per_second=10.0,
    skip_index_download=True,  # Skip if index already exists
)
```

Or use individual functions:

```python
from main import create_session, download_index, parse_index, extract_cusip
import os

# Create SEC-compliant session
session = create_session('Jane Doe', 'jane@example.com')

# Download index
index_path = download_index('data/master.idx', session, skip_if_exists=True)

# Parse index for 13D/13G forms
entries = parse_index(index_path, forms=('13D', '13G'))

# Process individual filing
for entry in entries[:10]:  # First 10 as example
    response = session.get(entry['url'])
    cusip = extract_cusip(response.text)
    print(f"{entry['cik']}: {cusip}")
```

## Output

The tool generates a CSV file with the following columns:

- `cik`: Central Index Key (SEC identifier)
- `company_name`: Company name from filing
- `form`: Form type (13D, SC 13D, 13G, SC 13G, etc.)
- `date`: Filing date
- `cusip`: Extracted CUSIP identifier (8-10 characters)

Example output:

```csv
cik,company_name,form,date,cusip
0001234567,EXAMPLE CORP,SC 13D,2024-01-15,12345678
0007654321,ANOTHER COMPANY,SC 13G,2024-01-20,87654321
```

## Command Line Options

```
--index PATH        Path to index file (default: data/master.idx)
--output PATH       Path to output CSV (default: data/cusips.csv)
--skip-index        Skip index download if file exists
--sec-name NAME     Your name for SEC User-Agent header
--sec-email EMAIL   Your email for SEC headers
--rate FLOAT        Requests per second (default: 10.0)
```

## How It Works

### 1. Index Download
Downloads the current quarter's SEC EDGAR master index file, which contains metadata for all filings.

### 2. Index Parsing
Parses the index file and filters for 13D and 13G form types (including SC 13D, SC 13G, and amended versions).

### 3. Filing Processing
For each matching filing:
- Downloads the filing from SEC EDGAR (with rate limiting)
- Extracts CUSIP using pattern matching
- Writes result to CSV

### 4. CUSIP Extraction
The CUSIP extraction algorithm:
- First looks for explicit "CUSIP" markers in the text
- Searches a window around these markers for valid CUSIP patterns
- Falls back to document-wide search if needed
- Validates candidates (length, character composition, excludes false positives)
- Returns the most likely CUSIP identifier

## Rate Limiting

The tool uses a token bucket algorithm to respect SEC rate limits:
- Default: 10 requests per second
- Configurable via `--rate` argument
- Includes exponential backoff for failed requests
- Retries on 429, 500, 502, 503, 504 status codes

## License

MIT License - See LICENSE file for details.
