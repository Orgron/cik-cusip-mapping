# Agent Guide for CIK-CUSIP-Mapping

This document provides guidance for AI agents (like Claude) working with the CIK-CUSIP-Mapping repository. It helps agents understand the project structure, constraints, and best practices when assisting users.

## Project Overview

This is a professional Python CLI tool that extracts CUSIP identifiers from SEC 13D and 13G filings. The tool:
- Downloads SEC EDGAR master indices for specified time periods
- Filters for 13D/13G forms
- Downloads each filing and extracts CUSIP identifiers
- Extracts accession numbers from filing URLs
- Writes results to CSV with CIK, company name, form type, date, CUSIP, and accession number
- Provides a CLI command to download individual filings by CIK and accession number
- Respects SEC rate limits and authorization requirements

## Project Structure

```
cik-cusip-mapping/
├── src/
│   └── cik_cusip/               # Main package
│       ├── __init__.py          # Package exports
│       ├── cli.py               # CLI commands (extract, download)
│       ├── rate_limiter.py      # Token bucket rate limiter
│       ├── session.py           # SEC-compliant session creation
│       ├── index.py             # Index download/parsing + accession extraction
│       ├── cusip.py             # CUSIP extraction and validation
│       ├── processor.py         # Main processing orchestration
│       └── utils.py             # Utility functions (CIK filter)
├── tests/                       # Comprehensive test suite
│   ├── test_cli.py
│   ├── test_rate_limiter.py
│   ├── test_session.py
│   ├── test_index.py
│   ├── test_cusip.py
│   ├── test_processor.py
│   └── test_utils.py
├── pyproject.toml               # Package config with CLI entry points
├── README.md                    # User-facing documentation
└── AGENTS.md                    # This file
```

## Key Files

### src/cik_cusip/cli.py
- Main CLI interface using Click framework
- Two commands:
  - `cik-cusip extract`: Extract CUSIPs from filings (main functionality)
  - `cik-cusip download`: Download filing by accession number
- Handles command-line arguments and environment variables
- Supports `--skip-existing` flag to avoid re-processing filings already in the output CSV

### src/cik_cusip/rate_limiter.py
- `RateLimiter` class: Thread-safe token bucket rate limiter
- Default: 10 requests/second
- Critical for SEC compliance

### src/cik_cusip/session.py
- `create_session()`: Creates SEC-compliant HTTP session
- Sets proper User-Agent and From headers
- Configures retry strategy with exponential backoff

### src/cik_cusip/index.py
- `download_index()`: Downloads single index file
- `download_indices()`: Downloads multiple indices for year/quarter range
- `parse_index()`: Parses index files and filters for target forms
- `extract_accession_number()`: Extracts accession number from URL

### src/cik_cusip/cusip.py
- `extract_cusip()`: Two-phase CUSIP extraction (window-based + fallback)
- `is_valid_cusip()`: Multi-layered validation with strict/lenient modes

### src/cik_cusip/processor.py
- `process_filings()`: Main orchestration function with progress display
- `download_filing_txt()`: Downloads filing by accession number
- Handles CSV output with accession numbers
- `_load_existing_results()`: Loads existing CSV to identify already-processed filings
- Supports skipping already-processed filings via `skip_existing` parameter

### src/cik_cusip/utils.py
- `load_cik_filter()`: Loads CIK filter from text file

### tests/
- Comprehensive pytest test suite
- Tests all modules with mocking for external dependencies
- Aim to maintain high test coverage

### pyproject.toml
- Package configuration with setuptools
- CLI entry point: `cik-cusip = "cik_cusip.cli:cli"`
- Dependencies: requests, click
- Dev dependencies: pytest, pytest-cov

## Critical Constraints

### SEC Fair Access Requirements

**MUST ALWAYS RESPECT**: The tool interacts with SEC EDGAR, which has strict requirements:

1. **User-Agent Header**: Required format: `{Application Name}/{Version} {Name} {Email}`
   - Currently set as: `CIK-CUSIP-Mapping/2.0 {sec_name} {sec_email}`
   - Users MUST provide their name and email (via args or SEC_NAME/SEC_EMAIL env vars)

2. **Rate Limiting**: Default 10 requests/second
   - Implemented via token bucket algorithm in `RateLimiter` class
   - DO NOT suggest removing or bypassing rate limiting
   - Configurable via `--rate` argument if user has legitimate need

3. **Retry Strategy**: Exponential backoff on errors (429, 500, 502, 503, 504)
   - Configured in `create_session()` using urllib3.Retry
   - DO NOT suggest aggressive retry settings

### When Helping Users Modify This Code

- **NEVER** suggest removing rate limiting or SEC headers
- **NEVER** suggest bypassing authorization requirements
- **ALWAYS** maintain respectful SEC etiquette (delays, proper headers)
- **WARN** users if they request changes that could violate SEC policies

## Code Architecture

### Data Flow

1. **Index Download** (`index.download_indices`)
   - Fetches master.idx files from SEC EDGAR
   - Format: `https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{quarter}/master.idx`
   - Supports year/quarter ranges (1993-present)

2. **Index Parsing** (`index.parse_index`)
   - Parses fixed-width pipe-delimited format
   - Skips first 11 header lines
   - Fields: CIK|Company Name|Form Type|Date Filed|Filename
   - Normalizes form types (removes "SC" prefix, "/A" suffix)
   - Extracts accession numbers from URLs

3. **Filing Download & Extraction** (`processor.process_filings`)
   - Rate-limited downloads of individual filings
   - Calls `extract_cusip()` on each filing
   - Extracts accession number via `extract_accession_number()`
   - Writes to CSV with 6 columns including accession_number

4. **CUSIP Extraction** (`cusip.extract_cusip`)
   - Two-phase approach:
     a. Window-based search around "CUSIP" markers (preferred)
     b. Document-wide search with scoring (fallback)
   - Validates candidates via `is_valid_cusip()`
   - Returns most likely CUSIP or None

### Design Patterns

- **CLI Framework**: Click for robust command-line interface
- **Modular Architecture**: Clean separation of concerns across modules
- **Session Management**: Single `requests.Session` per execution with connection pooling
- **Rate Limiting**: Token bucket pattern (thread-safe with Lock)
- **Error Handling**: Retry with exponential backoff, graceful degradation
- **Validation**: Multi-layered CUSIP validation (length, composition, false positive filtering)
- **CIK Filtering**: Optional filtering to process only specific CIKs
- **Progress Display**: Real-time progress bar with ETA, success/failure counts
- **Accession Numbers**: Extracted from URLs and stored in CSV output

## Installation and Usage

### Installation
```bash
pip install -e .              # Standard installation
pip install -e ".[dev]"       # With dev dependencies
```

### CLI Usage
```bash
# Extract CUSIPs
cik-cusip extract --sec-name "Name" --sec-email "email@example.com"
cik-cusip extract --all  # All historical data

# Download filing by CIK and accession number
cik-cusip download 813828 0001104659-06-026838
```

### Environment Variables
- `SEC_NAME`: Your name for User-Agent
- `SEC_EMAIL`: Your email for headers

## Common User Requests & How to Handle

### "Make it faster"

**DO:**
- Suggest increasing `--rate` if user has authorization
- Suggest `--skip-index` to reuse existing indices
- Suggest `--skip-existing` to avoid re-processing filings already in the output CSV
- Suggest reducing date range to process fewer filings

**DON'T:**
- Remove rate limiting
- Suggest parallel requests without rate limiting coordination
- Bypass SEC authorization

### "Add support for other form types"

**GUIDE USER TO:**
- Modify `forms` tuple in `process_filings()` call
- Example: `forms=("13D", "13G", "13F")` for 13F forms
- Ensure CUSIP extraction logic works for new form types (may need adjustment)

### "Extract additional data fields"

**APPROACH:**
- Study the form structure (provide SEC EDGAR examples)
- Add new extraction functions similar to `extract_cusip()` in cusip.py
- Update CSV output schema in processor.py
- Add validation similar to `is_valid_cusip()`
- Update tests to cover new extraction logic

### "Fix CUSIP extraction accuracy"

**DEBUGGING APPROACH:**
1. Ask user for specific failing examples (CIK, form, date)
2. Examine the filing text structure
3. Adjust regex patterns in `cusip.extract_cusip()`
4. Modify `cusip.is_valid_cusip()` validation rules
5. Test on known cases
6. Update test suite with new cases

### "Only process specific companies/CIKs"

**GUIDE USER TO:**
- Create a text file with one CIK per line
- Use `--cik-filter` argument or `cik_filter_file` parameter
- Example: `cik-cusip extract --cik-filter my_ciks.txt --all`
- This significantly reduces processing time when tracking specific companies

### "I want to download a specific filing"

**GUIDE USER TO:**
- Use the `cik-cusip download` command with both CIK and accession number
- Both CIK and accession numbers are included in the CSV output
- Example: `cik-cusip download 813828 0001104659-06-026838 -o filing.txt`

### "I want to incrementally update my data"

**GUIDE USER TO:**
- Use the `--skip-existing` flag to avoid re-processing filings already in the output CSV
- The tool checks accession numbers to identify already-processed filings
- Existing results are preserved and merged with new results
- Example: `cik-cusip extract --skip-existing --all`
- This is especially useful when running periodic updates or resuming interrupted extractions

## Testing

The project has comprehensive pytest coverage:
- Tests for all modules: cli, rate_limiter, session, index, cusip, processor, utils
- Includes mocking of HTTP requests and file I/O
- Tests edge cases, error handling, rate limiting
- Run with: `pytest`
- Coverage: `pytest --cov=cik_cusip --cov-report=html`

**When making changes**: Always update or add tests to maintain coverage.

## Common Pitfalls

1. **Index Format**: Indices have 11-line headers - don't forget to skip them
2. **Form Normalization**: "SC 13D" and "13D" are different in raw index, same conceptually
3. **CUSIP Validation**: Many false positives (ZIP codes, file numbers) - validation is crucial
4. **Rate Limiting**: Must be enforced per-request, not per-batch
5. **Year/Quarter Logic**: Edge cases in range handling (start/end year quarters)
6. **Accession Number Format**: Must match pattern NNNNNNNNNN-NN-NNNNNN
7. **CLI Entry Points**: Defined in pyproject.toml [project.scripts]

## Environment & Dependencies

- Python 3.9+
- Dependencies: `requests`, `click`
- Dev dependencies: `pytest`, `pytest-cov`
- No database required (CSV output)
- No authentication tokens (just User-Agent headers)

## Useful Commands for Development

```bash
# Install for development
pip install -e ".[dev]"

# Run CLI
cik-cusip extract --help
cik-cusip download --help

# Run with environment variables
export SEC_NAME="Test User"
export SEC_EMAIL="test@example.com"
cik-cusip extract

# Run tests
pytest
pytest --cov=cik_cusip
pytest -v tests/test_cli.py

# Check imports
python -c "from cik_cusip import extract_cusip, process_filings; print('OK')"
```

## When Reading SEC Filings

If users ask you to help debug extraction issues:
1. SEC filings are messy - mix of HTML, SGML, plain text
2. CUSIP location varies by filer
3. The tool uses a "marker + window" approach to find CUSIPs near the word "CUSIP"
4. Scoring heuristics prefer: letters over all-digits, 9-character length
5. False positives are common - `is_valid_cusip()` filters many but not all

## Agent Best Practices

1. **Read Before Suggesting**: Always read relevant code sections before making suggestions
2. **Respect Constraints**: SEC policies are non-negotiable
3. **Test Coverage**: Maintain high test coverage standard
4. **User Education**: Explain SEC requirements when relevant
5. **Examples**: Provide working code examples when suggesting changes
6. **Documentation**: Update README.md and AGENTS.md if user-facing changes are made
7. **Module Organization**: Keep modules focused on single responsibilities

## Quick Reference: Key Functions

### CLI Commands
```bash
cik-cusip extract [OPTIONS]            # Extract CUSIPs from filings
cik-cusip download CIK ACCESSION       # Download filing by CIK and accession number
```

### Main Functions
```python
# processor.py
process_filings(index_dir, output_csv, forms, sec_name, sec_email, skip_existing, ...)
download_filing_txt(cik, accession_number, output_path, sec_name, sec_email)
_load_existing_results(output_csv) -> tuple[set, list]

# index.py
download_indices(output_dir, session, start_year, start_quarter, ...)
parse_index(index_path, forms) -> list[dict]
extract_accession_number(url) -> str

# cusip.py
extract_cusip(text) -> Optional[str]
is_valid_cusip(candidate, strict=True) -> bool

# session.py
create_session(sec_name, sec_email) -> requests.Session

# utils.py
load_cik_filter(cik_filter_file) -> set
```

## CSV Output Schema

The output CSV has 6 columns:
1. `cik`: Central Index Key
2. `company_name`: Company name from filing
3. `form`: Form type (13D, SC 13D, 13G, SC 13G, etc.)
4. `date`: Filing date
5. `cusip`: Extracted CUSIP identifier
6. `accession_number`: SEC accession number (NEW in v2.0)

## Resources

- [SEC Fair Access Policy](https://www.sec.gov/os/webmaster-fair-access)
- [SEC EDGAR](https://www.sec.gov/edgar)
- [CUSIP on Wikipedia](https://en.wikipedia.org/wiki/CUSIP)
- [Form 13D/13G Overview](https://www.sec.gov/answers/sched13)
- [Click Documentation](https://click.palletsprojects.com/)
- [Python Packaging Guide](https://packaging.python.org/)

---

**Remember**: This tool interacts with a government system (SEC EDGAR). Always prioritize compliance, respect, and ethical use when assisting users.
