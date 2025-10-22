# Repository guidance

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
