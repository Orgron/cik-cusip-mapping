# CIK-CUSIP Mapping

A simple, focused tool for extracting CUSIP identifiers from SEC 13D and 13G filings.

## What it does

This tool:
1. Downloads SEC EDGAR master indices (current quarter or historical data from 1993-present)
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

Basic usage (downloads current quarter only):

```bash
python main.py --sec-name "Jane Doe" --sec-email "jane@example.com"
```

Download all historical indices (1993 to present):

```bash
python main.py --sec-name "Jane Doe" --sec-email "jane@example.com" --all
```

Download specific year range:

```bash
python main.py \
  --sec-name "Jane Doe" \
  --sec-email "jane@example.com" \
  --start-year 2020 \
  --end-year 2024
```

Download specific quarter range:

```bash
python main.py \
  --sec-name "Jane Doe" \
  --sec-email "jane@example.com" \
  --start-year 2023 \
  --start-quarter 3 \
  --end-year 2024 \
  --end-quarter 2
```

With additional options:

```bash
python main.py \
  --sec-name "Jane Doe" \
  --sec-email "jane@example.com" \
  --index-dir data/indices \
  --output data/cusips.csv \
  --skip-index \
  --rate 5 \
  --all
```

Filter for specific CIKs only:

```bash
# Create a file with CIKs (one per line)
echo "1234567" > my_ciks.txt
echo "9876543" >> my_ciks.txt

python main.py \
  --sec-name "Jane Doe" \
  --sec-email "jane@example.com" \
  --cik-filter my_ciks.txt \
  --all
```

Using environment variables:

```bash
export SEC_NAME="Jane Doe"
export SEC_EMAIL="jane@example.com"
python main.py --all
```

### As a Python Library

```python
from main import process_filings

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
```

Or use individual functions:

```python
from main import create_session, download_indices, parse_index, extract_cusip

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
--index-dir PATH      Directory for index files (default: data/indices)
--output PATH         Path to output CSV (default: data/cusips.csv)
--skip-index          Skip index download if files exist
--sec-name NAME       Your name for SEC User-Agent header
--sec-email EMAIL     Your email for SEC headers
--rate FLOAT          Requests per second (default: 10.0)
--cik-filter PATH     Path to text file with CIKs to filter (one per line)

Year/Quarter Range Options:
--all                 Download all available indices (1993 to present)
--start-year YEAR     Starting year (default: current year if not --all)
--start-quarter 1-4   Starting quarter (default: 1)
--end-year YEAR       Ending year (default: current year)
--end-quarter 1-4     Ending quarter (default: current quarter)
```

## How It Works

### 1. Index Download
Downloads SEC EDGAR master index files for the specified time range (current quarter by default, or all historical data from 1993-present with `--all`), which contains metadata for all filings.

### 2. Index Parsing
Parses all downloaded index files and filters for 13D and 13G form types (including SC 13D, SC 13G, and amended versions).

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
   python main.py --sec-name "Your Name" --sec-email "your@email.com" \
     --cik-filter my_ciks.txt --all
   ```

This is useful when you only need to track filings for specific companies and want to reduce processing time.

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

## License

MIT License - See LICENSE file for details.
