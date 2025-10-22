# Repository guidance

## Repo purpose
This repo's main goal is parsing 13D and 13G forms submitted to SEC by firms, to create a mapping from firms' CIKs (Central Index Key is used on the SEC's computer systems to identify corporations and individual people who have filed disclosure with the SEC) to CUSIPs (Committee on Uniform Security Identification Procedures, a nine-character numeric or alphanumeric code that uniquely identifies a North American financial security for the purposes of facilitating clearing and settlement of trades). 

# The structure of CUSIP
The first six characters are known as the base (or CUSIP-6), and uniquely identify the issuer. Issuer codes are assigned alphabetically from a series that includes deliberately built-in gaps for future expansion. The 7th and 8th digit identify the exact issue. The 9th digit is a checksum (some clearing bodies ignore or truncate the last digit). The last three characters of the issuer code can be letters, in order to provide more room for expansion.

## Scope
These instructions apply to the entire repository.

## Code style
- Follow standard Python 3.12 type-hinted style. Prefer explicit imports and keep functions pure where practical.
- Maintain existing docstrings and logging patterns; use descriptive function names.
- Keep line length within 100 characters to match the existing codebase.

## Testing
- Run `pytest` after making changes.

## Tooling
- Use `pip install -e .` for editable installs during development.

## Documentation
- Update `README.md` if you change usage patterns, configuration, or supported commands.
