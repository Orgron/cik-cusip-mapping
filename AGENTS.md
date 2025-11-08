# Agent Guide for CIK-CUSIP-Mapping

This document provides guidance for AI agents (like Claude) working with the CIK-CUSIP-Mapping repository. It helps agents understand the project structure, constraints, and best practices when assisting users.

## Project Overview

This is a Python tool that extracts CUSIP identifiers from SEC 13D and 13G filings. The tool:
- Downloads SEC EDGAR master indices for specified time periods
- Filters for 13D/13G forms
- Downloads each filing and extracts CUSIP identifiers
- Writes results to CSV
- Respects SEC rate limits and authorization requirements

## Key Files

- `main.py`: Core functionality (565 lines)
  - `RateLimiter` class: Token bucket rate limiting
  - `create_session()`: Creates SEC-compliant HTTP session
  - `download_indices()`: Downloads multiple index files
  - `parse_index()`: Parses index files for target forms
  - `extract_cusip()`: Extracts CUSIP from filing text using pattern matching
  - `process_filings()`: Main orchestration function
- `test_main.py`: Comprehensive pytest test suite with 100% coverage
- `README.md`: User-facing documentation
- `.gitignore`: Excludes data/, pytest, and coverage files

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

1. **Index Download** (`download_indices`)
   - Fetches master.idx files from SEC EDGAR
   - Format: `https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{quarter}/master.idx`
   - Supports year/quarter ranges (1993-present)

2. **Index Parsing** (`parse_index`)
   - Parses fixed-width pipe-delimited format
   - Skips first 11 header lines
   - Fields: CIK|Company Name|Form Type|Date Filed|Filename
   - Normalizes form types (removes "SC" prefix, "/A" suffix)

3. **Filing Download & Extraction** (`process_filings`)
   - Rate-limited downloads of individual filings
   - Calls `extract_cusip()` on each filing

4. **CUSIP Extraction** (`extract_cusip`)
   - Two-phase approach:
     a. Window-based search around "CUSIP" markers (preferred)
     b. Document-wide search with scoring (fallback)
   - Validates candidates via `is_valid_cusip()`
   - Returns most likely CUSIP or None

### Design Patterns

- **Session Management**: Single `requests.Session` per execution with connection pooling
- **Rate Limiting**: Token bucket pattern (thread-safe with Lock)
- **Error Handling**: Retry with exponential backoff, graceful degradation
- **Validation**: Multi-layered CUSIP validation (length, composition, false positive filtering)

## Common User Requests & How to Handle

### "Make it faster"

**DO:**
- Suggest increasing `--rate` if user has authorization
- Suggest `--skip-index` to reuse existing indices
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
- Add new extraction functions similar to `extract_cusip()`
- Update CSV output schema
- Add validation similar to `is_valid_cusip()`
- Update tests to cover new extraction logic

### "Fix CUSIP extraction accuracy"

**DEBUGGING APPROACH:**
1. Ask user for specific failing examples (CIK, form, date)
2. Examine the filing text structure
3. Adjust regex patterns in `extract_cusip()`
4. Modify `is_valid_cusip()` validation rules
5. Test on known cases
6. Update test suite with new cases

## Testing

The project has comprehensive pytest coverage:
- `test_main.py`: Tests all major functions
- Includes mocking of HTTP requests
- Tests edge cases, error handling, rate limiting
- Run with: `pytest test_main.py -v`
- Coverage: `pytest --cov=main --cov-report=html`

**When making changes**: Always update or add tests to maintain coverage.

## Common Pitfalls

1. **Index Format**: Indices have 11-line headers - don't forget to skip them
2. **Form Normalization**: "SC 13D" and "13D" are different in raw index, same conceptually
3. **CUSIP Validation**: Many false positives (ZIP codes, file numbers) - validation is crucial
4. **Rate Limiting**: Must be enforced per-request, not per-batch
5. **Year/Quarter Logic**: Edge cases in range handling (start/end year quarters)

## Environment & Dependencies

- Python 3.9+
- Dependencies: `requests` (with urllib3 for retry logic)
- No database required (CSV output)
- No authentication tokens (just User-Agent headers)

## Useful Commands for Development

```bash
# Run with current quarter only
python main.py --sec-name "Test User" --sec-email "test@example.com"

# Download all historical data
python main.py --sec-name "Test User" --sec-email "test@example.com" --all

# Download specific range
python main.py --sec-name "Test User" --sec-email "test@example.com" \
  --start-year 2020 --end-year 2024

# Run tests
pytest test_main.py -v

# Check coverage
pytest --cov=main --cov-report=term-missing
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
3. **Test Coverage**: Maintain the 100% test coverage standard
4. **User Education**: Explain SEC requirements when relevant
5. **Examples**: Provide working code examples when suggesting changes
6. **Documentation**: Update README.md if user-facing changes are made

## Quick Reference: Main Function Parameters

```python
process_filings(
    index_dir: str,          # Where to save/load indices
    output_csv: str,         # Output file path
    forms: tuple,            # ("13D", "13G") - which forms to process
    sec_name: str,           # User's name (REQUIRED)
    sec_email: str,          # User's email (REQUIRED)
    requests_per_second: float,  # Rate limit (default: 10)
    skip_index_download: bool,   # Reuse existing indices
    start_year: int,         # Start year (default: 1993)
    start_quarter: int,      # Start quarter 1-4
    end_year: int,           # End year (default: current)
    end_quarter: int,        # End quarter (default: current)
)
```

## Resources

- [SEC Fair Access Policy](https://www.sec.gov/os/webmaster-fair-access)
- [SEC EDGAR](https://www.sec.gov/edgar)
- [CUSIP on Wikipedia](https://en.wikipedia.org/wiki/CUSIP)
- [Form 13D/13G Overview](https://www.sec.gov/answers/sched13)

---

**Remember**: This tool interacts with a government system (SEC EDGAR). Always prioritize compliance, respect, and ethical use when assisting users.
