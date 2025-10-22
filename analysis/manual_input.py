from __future__ import annotations

from pathlib import Path
import html
import importlib.util
import re
import sys
import types
from typing import Dict


def _load_parsing_module():
    """Load the parsing helpers without importing the package initializer."""

    project_root = Path(__file__).resolve().parents[1]
    parsing_path = project_root / "src" / "cik_cusip_mapping" / "parsing.py"
    package_name = "cik_cusip_mapping"
    if package_name not in sys.modules:
        package = types.ModuleType(package_name)
        package.__path__ = [str(parsing_path.parent)]
        sys.modules[package_name] = package
    spec = importlib.util.spec_from_file_location(
        f"{package_name}.parsing", parsing_path
    )
    if spec is None or spec.loader is None:  # pragma: no cover - defensive guard
        raise RuntimeError("Unable to load parsing helpers")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


parsing_module = _load_parsing_module()
TAG_PATTERN = parsing_module.TAG_PATTERN
extract_cik = parsing_module._extract_cik

ROOT = Path('form_examples')
OUTPUT = Path('analysis/manual_input.csv')
CUSIP_WORD = re.compile(r"CUSIP", re.IGNORECASE)
TOKEN_PATTERN = re.compile(r"[0-9A-Z][0-9A-Z \-]{3,40}")


def normalize(token: str) -> str:
    return ''.join(ch for ch in token if ch.isalnum())


def collect_tokens(text: str) -> tuple[list[str], list[str]]:
    cleaned = TAG_PATTERN.sub(' ', html.unescape(text)).replace('\xa0', ' ')
    upper = cleaned.upper()
    tokens: set[str] = set()
    contexts: list[str] = []
    for match in CUSIP_WORD.finditer(upper):
        start = max(0, match.start() - 160)
        end = min(len(upper), match.end() + 160)
        snippet = upper[start:end]
        contexts.append(' '.join(snippet.split()))
        if ' NONE' in snippet or 'NOT APPLICABLE' in snippet:
            tokens.add('NONE')
        for tok in TOKEN_PATTERN.findall(snippet):
            normalized = normalize(tok)
            if any(ch.isdigit() for ch in normalized) and len(normalized) >= 5:
                tokens.add(normalized)
    if not contexts:
        contexts.append(' '.join(upper.split()[:80]))
        for tok in TOKEN_PATTERN.findall(upper):
            normalized = normalize(tok)
            if any(ch.isdigit() for ch in normalized) and len(normalized) >= 5:
                tokens.add(normalized)
    tokens = list(tokens)
    tokens.sort(key=lambda t: (
        t.startswith(('19', '20')) and t.isdigit(),
        -sum(ch.isdigit() for ch in t),
        len(t),
        t,
    ))
    return tokens, contexts


def load_existing() -> Dict[str, str]:
    if not OUTPUT.exists():
        return {}
    mapping: Dict[str, str] = {}
    for line in OUTPUT.read_text(encoding='utf-8').splitlines()[1:]:
        if not line.strip():
            continue
        filename, _cik, cusip = line.split(',', 2)
        mapping[filename] = cusip
    return mapping


def append_record(filename: str, cik: str, cusip: str) -> None:
    header = 'filename,cik,cusip\n'
    if not OUTPUT.exists():
        OUTPUT.write_text(header, encoding='utf-8')
    with OUTPUT.open('a', encoding='utf-8') as handle:
        handle.write(f"{filename},{cik},{cusip}\n")


def main() -> None:
    existing = load_existing()
    for path in sorted(ROOT.iterdir()):
        if path.name in existing:
            continue
        text = path.read_text(encoding='utf-8', errors='ignore')
        cik = extract_cik(text.splitlines()) or ''
        tokens, contexts = collect_tokens(text)
        print(f"\n{path.name}")
        print(f"CIK: {cik}")
        for idx, context in enumerate(contexts[:2], 1):
            print(f"Context {idx}: {context[:200]}")
        for idx, token in enumerate(tokens):
            print(f"  [{idx}] {token}")
        choice = input('Select token index or enter value (empty for none): ').strip()
        if choice == '':
            selected = ''
        elif choice.isdigit() and int(choice) < len(tokens):
            selected = tokens[int(choice)]
        else:
            selected = choice
        append_record(path.name, cik, selected)

    print(f"\nProgress saved to {OUTPUT}")


if __name__ == '__main__':
    main()
