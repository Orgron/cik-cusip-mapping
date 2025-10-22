"""Parse CUSIP identifiers from streamed filings."""

from __future__ import annotations

import csv
import html
import re
from collections import Counter, deque
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Deque, Iterable, Iterator, Protocol, Sequence

from .progress import resolve_tqdm


def _collect_tokens_near_cusip(segment: str) -> list[str]:
    """Return candidate tokens located near explicit ``CUSIP`` markers."""

    tokens: list[str] = []
    cleaned_lines = [
        TAG_PATTERN.sub(" ", html.unescape(line)) for line in segment.splitlines()
    ]
    for index, line in enumerate(cleaned_lines):
        upper_line = line.upper()
        if "CUSIP" not in upper_line:
            continue
        if "NONE" in upper_line or "NOT APPLICABLE" in upper_line:
            tokens.append("NONE")
            continue
        current_tokens = _extract_matches(line)
        if current_tokens:
            tokens.extend(current_tokens)
            continue
        block: list[str] = []
        for prev_index in range(index - 1, max(-1, index - 20), -1):
            candidate = cleaned_lines[prev_index]
            upper_candidate = candidate.upper()
            if any(keyword in upper_candidate for keyword in SKIP_CONTEXT_KEYWORDS):
                continue
            if "NONE" in upper_candidate:
                block.insert(0, "NONE")
                continue
            prev_tokens = _extract_matches(candidate)
            if prev_tokens:
                block = prev_tokens + block
            elif block and candidate.strip():
                break
        if block:
            tokens.extend(block)
            continue
        following: list[str] = []
        for next_index in range(index + 1, min(len(cleaned_lines), index + 20)):
            candidate = cleaned_lines[next_index]
            upper_candidate = candidate.upper()
            if any(keyword in upper_candidate for keyword in SKIP_CONTEXT_KEYWORDS):
                continue
            if "NONE" in upper_candidate:
                following.append("NONE")
                continue
            next_tokens = _extract_matches(candidate)
            if next_tokens:
                following.extend(next_tokens)
            elif following and candidate.strip():
                break
        if following:
            tokens.extend(following)
    if tokens:
        numeric_tokens = [token for token in tokens if token != "NONE"]
        if numeric_tokens:
            return numeric_tokens
        return ["NONE"]
    if not tokens:
        tokens.extend(_extract_matches(" ".join(cleaned_lines)))
    return tokens

CUSIP_PATTERN = re.compile(r"[0-9A-Z](?:[- ]?[0-9A-Z]){7,11}")
TAG_PATTERN = re.compile(r"<[^>]+>")
ARCHIVES_URL = "https://www.sec.gov/Archives/"
SKIP_CONTEXT_KEYWORDS = ("TITLE", "FILENAME", "CIK", "DOC", "HTM")


@dataclass
class ParsedFiling:
    """Structured result returned by parsing a single filing."""

    identifier: str | None
    cik: str | None
    cusip: str | None
    form: str | None = None
    filing_date: str | None = None
    accession_number: str | None = None
    company_name: str | None = None
    parse_method: str | None = None


class FilingLike(Protocol):
    """Protocol that describes the minimal attributes required from a filing."""

    identifier: str
    content: str


def _extract_cik(lines: Sequence[str]) -> str | None:
    """Return the CIK value embedded in the filing header, if present."""

    record = False
    for line in lines:
        if "SUBJECT COMPANY" in line:
            record = True
        if "CENTRAL INDEX KEY" in line and record:
            return line.split("\t\t\t")[-1].strip()
    return None


def _normalize_cusip(value: str) -> str | None:
    """Normalize a candidate CUSIP string to an uppercase token."""

    cleaned = "".join(ch for ch in value.upper() if ch.isalnum())
    digit_count = sum(ch.isdigit() for ch in cleaned)
    if 8 <= len(cleaned) <= 10 and digit_count >= 5:
        return cleaned
    return None


def _extract_matches(text: str) -> list[str]:
    """Find normalized CUSIP candidates within the provided text block."""

    cleaned = TAG_PATTERN.sub(" ", html.unescape(text))
    upper = cleaned.upper()
    matches: list[str] = []
    for match in CUSIP_PATTERN.finditer(upper):
        normalized = _normalize_cusip(match.group())
        if normalized:
            start, end = match.span()
            context = upper[max(0, start - 16) : end + 16]
            if "IRS NUMBER" in context or "I.R.S" in context:
                continue
            if ".DOC" in context or ".HTM" in context:
                continue
            if "P.O. BOX" in context or "PO BOX" in context or "P O BOX" in context:
                continue
            matches.append(normalized)
    return matches


def _extract_cusip(
    lines: Sequence[str], *, debug: bool = False, exclude: set[str] | None = None
) -> tuple[str | None, str | None]:
    """Locate the best CUSIP match in the filing and describe the method used."""

    record = False
    matches: list[tuple[str, str]] = []
    candidate_segments: list[str] = []
    segment_matches: list[tuple[list[str], str]] = []
    document_segments: list[str] = []
    exclude_tokens: set[str] = set(exclude) if exclude else set()
    for index, raw_line in enumerate(lines):
        if "<DOCUMENT>" in raw_line:
            record = True
        if not record:
            continue
        document_segments.append(raw_line)
        upper_line = raw_line.upper()
        if "CIK" in upper_line:
            exclude_tokens.update(_extract_matches(raw_line))
        if "CUSIP" in upper_line:
            window = lines[max(0, index - 15) : index + 10]
            candidate_segments.append("\n".join(window))
    for segment in candidate_segments:
        tokens = _collect_tokens_near_cusip(segment)
        if tokens:
            segment_matches.append((tokens, segment.upper()))
        for normalized in tokens:
            matches.append((normalized, "window"))
            if debug:
                print("INFO: candidate match", normalized, "from", segment.strip())
    if candidate_segments and not segment_matches:
        return "NONE", "window"
    if segment_matches:
        singleton_candidates: list[str] = []
        saw_none_only = False
        first_seen: dict[str, int] = {}
        for index, (tokens, upper_segment) in enumerate(segment_matches):
            ordered: list[str] = []
            seen: set[str] = set()
            for token in tokens:
                if token in exclude_tokens:
                    continue
                if token not in first_seen:
                    first_seen[token] = index
                if token not in seen:
                    ordered.append(token)
                    seen.add(token)
            if not ordered:
                continue
            if ordered == ["NONE"]:
                saw_none_only = True
                continue
            if len(ordered) > 1:
                bases = {token[:6] for token in ordered if len(token) >= 6}
                cusip_mentions = upper_segment.count("CUSIP")
                if len(bases) == 1 or cusip_mentions >= len(ordered):
                    return ";".join(ordered), "window"
                singleton_candidates.extend(ordered)
                continue
            singleton_candidates.extend(ordered)
        if singleton_candidates:
            counts = Counter(singleton_candidates)
            unique_candidates = list(counts)

            def score(token: str) -> tuple[int, int, int, int, int]:
                has_letter = int(any(ch.isalpha() for ch in token))
                digit_count = sum(ch.isdigit() for ch in token)
                first_index = first_seen.get(token, len(segment_matches))
                length_score = -abs(len(token) - 9)
                return (
                    has_letter,
                    digit_count,
                    -first_index,
                    counts[token],
                    length_score,
                )

            winner = max(unique_candidates, key=score)
            return winner, "window"
        if saw_none_only:
            return "NONE", "window"

    if not matches and document_segments:
        combined = "\n".join(document_segments)
        for normalized in _extract_matches(combined):
            matches.append((normalized, "fallback"))
            if debug:
                print("INFO: fallback match", normalized)
    if not matches:
        return None, None
    filtered_matches = [
        match for match, _ in matches if not exclude_tokens or match not in exclude_tokens
    ]
    if not filtered_matches:
        filtered_matches = [match for match, _ in matches]
    winner = Counter(filtered_matches).most_common(1)[0][0]
    winner_methods = [method for match, method in matches if match == winner]
    parse_method = winner_methods[0] if winner_methods else None
    return winner, parse_method


def parse_text(
    text: str, *, debug: bool = False
) -> tuple[str | None, str | None, str | None]:
    """Parse raw filing text and return the CIK, CUSIP, and parse method."""

    lines = text.splitlines()
    cik = _extract_cik(lines)
    exclude: set[str] | None = None
    if cik:
        stripped = cik.lstrip("0")
        exclude = {cik}
        if stripped:
            exclude.add(stripped)
    cusip, parse_method = _extract_cusip(lines, debug=debug, exclude=exclude)
    if debug:
        print(cusip)
    return cik, cusip, parse_method


def parse_file(path: Path | str, *, debug: bool = False) -> ParsedFiling:
    """Parse a filing stored on disk into a :class:`ParsedFiling` record."""

    path = Path(path)
    text = path.read_text(encoding="utf-8", errors="ignore")
    cik, cusip, parse_method = parse_text(text, debug=debug)
    return ParsedFiling(str(path), cik, cusip, parse_method=parse_method)


def parse_filings(
    filings: Iterable[FilingLike],
    *,
    debug: bool = False,
) -> Iterator[ParsedFiling]:
    """Iterate over in-memory filings and yield parsed results."""

    for filing in filings:
        cik, cusip, parse_method = parse_text(filing.content, debug=debug)
        yield ParsedFiling(
            filing.identifier,
            cik,
            cusip,
            form=getattr(filing, "form", None),
            filing_date=getattr(filing, "date", None),
            accession_number=getattr(filing, "accession_number", None),
            company_name=getattr(filing, "company_name", None),
            parse_method=parse_method,
        )


def parse_filings_concurrently(
    filings: Iterable[FilingLike],
    *,
    debug: bool = False,
    max_queue: int = 32,
    workers: int = 2,
) -> Iterator[ParsedFiling]:
    """Parse filings using a thread pool to overlap I/O and computation."""

    with ThreadPoolExecutor(max_workers=workers) as executor:
        pending: Deque[
            tuple[FilingLike, Future[tuple[str | None, str | None, str | None]]]
        ] = (
            deque()
        )
        for filing in filings:
            future = executor.submit(parse_text, filing.content, debug=debug)
            pending.append((filing, future))
            if len(pending) >= max_queue:
                oldest_filing, oldest_future = pending.popleft()
                cik, cusip, parse_method = oldest_future.result()
                yield ParsedFiling(
                    oldest_filing.identifier,
                    cik,
                    cusip,
                    form=getattr(oldest_filing, "form", None),
                    filing_date=getattr(oldest_filing, "date", None),
                    accession_number=getattr(oldest_filing, "accession_number", None),
                    company_name=getattr(oldest_filing, "company_name", None),
                    parse_method=parse_method,
                )
        while pending:
            filing, future = pending.popleft()
            cik, cusip, parse_method = future.result()
            yield ParsedFiling(
                filing.identifier,
                cik,
                cusip,
                form=getattr(filing, "form", None),
                filing_date=getattr(filing, "date", None),
                accession_number=getattr(filing, "accession_number", None),
                company_name=getattr(filing, "company_name", None),
                parse_method=parse_method,
            )


def stream_events_to_csv(
    filings: Iterable[FilingLike],
    events_csv_path: Path | str,
    *,
    debug: bool = False,
    concurrent: bool = False,
    max_queue: int = 32,
    workers: int = 2,
    show_progress: bool = True,
    total_hint: int | None = None,
    use_notebook: bool | None = None,
) -> int:
    """Stream parsed filings to an events CSV with derived CUSIP details."""

    events_path = Path(events_csv_path)
    events_path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    if concurrent:
        iterator = parse_filings_concurrently(
            filings,
            debug=debug,
            max_queue=max_queue,
            workers=workers,
        )
    else:
        iterator = parse_filings(filings, debug=debug)

    with events_path.open("w", newline="", encoding="utf-8") as events_handle:
        events_writer = csv.writer(events_handle)
        events_writer.writerow(
            [
                "cik",
                "form",
                "filing_date",
                "accession_number",
                "company_name",
                "cusip9",
                "cusip8",
                "cusip6",
                "parse_method",
            ]
        )
        progress = (
            resolve_tqdm(use_notebook)(
                total=total_hint,
                desc="Parsing filings",
                unit="filing",
                dynamic_ncols=True,
                mininterval=0.1,
                leave=True,
            )
            if show_progress
            else None
        )
        try:
            for parsed in iterator:
                cusip9 = parsed.cusip or ""
                events_writer.writerow(
                    [
                        parsed.cik or "",
                        parsed.form or "",
                        parsed.filing_date or "",
                        parsed.accession_number or "",
                        parsed.company_name or "",
                        cusip9,
                        cusip9[:8] if cusip9 else "",
                        cusip9[:6] if cusip9 else "",
                        parsed.parse_method or "",
                    ]
                )
                count += 1
                if progress is not None:
                    progress.update(1)
        finally:
            if progress is not None:
                progress.close()
    return count


def reconstruct_filing_url(cik: str | int, accession_number: str) -> str:
    """Recreate the EDGAR index URL for a filing from its identifiers."""

    cik_value = str(cik).lstrip("0") or "0"
    accession_fragment = accession_number.replace("-", "")
    return (
        f"{ARCHIVES_URL}edgar/data/{cik_value}/{accession_fragment}/"
        f"{accession_number}-index.html"
    )


def _iter_directory_files(
    path: Path, *, glob_pattern: str = "**/*"
) -> Iterator[Path]:
    """Yield files within ``path`` grouped by subdirectory for reproducible order."""

    for candidate in sorted(path.glob(glob_pattern)):
        if candidate.is_file():
            yield candidate


def parse_directory(
    directory: Path | str,
    *,
    output_csv: Path | None = None,
    debug: bool = False,
    concurrent: bool = False,
    max_queue: int = 32,
    workers: int = 2,
    glob_pattern: str = "**/*",
    show_progress: bool = True,
) -> int:
    """Parse every filing stored in ``directory`` and write results to CSV."""

    directory = Path(directory)
    if not directory.exists():
        raise FileNotFoundError(f"Directory not found: {directory}")
    output_csv = output_csv or Path(f"{directory}.csv")
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    file_paths = list(_iter_directory_files(directory, glob_pattern=glob_pattern))
    with output_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        if concurrent:
            filings = (
                SimpleNamespace(
                    identifier=str(file_path),
                    content=file_path.read_text(
                        encoding="utf-8", errors="ignore"
                    ),
                )
                for file_path in file_paths
            )
            iterator = parse_filings_concurrently(
                filings,
                debug=debug,
                max_queue=max_queue,
                workers=workers,
            )
        else:
            iterator = (
                parse_file(file_path, debug=debug)
                for file_path in file_paths
            )
        progress = (
            resolve_tqdm(None)(
                iterator,
                total=len(file_paths),
                desc="Parsing directory",
                unit="filing",
                dynamic_ncols=True,
                mininterval=0.1,
            )
            if show_progress
            else iterator
        )
        for parsed in progress:
            writer.writerow([parsed.identifier, parsed.cik, parsed.cusip])
            count += 1
        if show_progress and hasattr(progress, "close"):
            progress.close()
    return count
