import argparse
import csv
import re
from collections import Counter
from functools import partial
from glob import glob
from multiprocessing import Pool
from pathlib import Path
from typing import Iterable, Iterator, List

try:
    from tqdm import tqdm
except ImportError:  # pragma: no cover - fallback when tqdm isn't installed
    tqdm = None  # type: ignore


PATTERN = re.compile(
    r"[\( >]*[0-9A-Z]{1}[0-9]{3}[0-9A-Za-z]{2}[- ]*[0-9]{0,2}[- ]*[0-9]{0,1}[\) \n<]*"
)
WORD_PATTERN = re.compile(r"\w+")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("files", help="Folder containing filing text files to parse.")
    parser.add_argument("--debug", action="store_true", help="Parse a single file and print the result.")
    parser.add_argument(
        "--no-progress",
        action="store_true",
        help="Disable progress bars for non-interactive environments.",
    )
    return parser


def parse_file(file: str, debug: bool = False) -> List[str | None]:
    with open(file, "r") as f:
        lines = f.readlines()

    record = 0
    cik = None
    for line in lines:
        if "SUBJECT COMPANY" in line:
            record = 1
        if "CENTRAL INDEX KEY" in line and record == 1:
            cik = line.split("\t\t\t")[-1].strip()
            break

    cusips = []
    record = 0
    for line in lines:
        if "<DOCUMENT>" in line:  # lines are after the document preamble
            record = 1
        if record == 1:
            if "IRS" not in line and "I.R.S" not in line:
                fd = PATTERN.findall(line)
                if fd:
                    cusip = fd[0].strip().strip("<>")
                    if debug:
                        print(
                            "INFO: added --- ",
                            line,
                            " --- extracted [",
                            cusip,
                            "]",
                        )
                    cusips.append(cusip)
    if len(cusips) == 0:
        cusip = None
    else:
        cusip = Counter(cusips).most_common()[0][0]
        cusip = "".join(WORD_PATTERN.findall(cusip))
    if debug:
        print(cusip)

    return [file, cik, cusip]


def _progress(
    iterable: Iterable[List[str | None]],
    *,
    enabled: bool,
    total: int,
    desc: str,
) -> Iterator[List[str | None]]:
    if enabled and tqdm is not None:
        return iter(tqdm(iterable, total=total, desc=desc, unit="file"))
    return iter(iterable)


def main(argv: Iterable[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)

    path = Path(args.files)

    if args.debug:
        if path.exists():
            result = parse_file(str(path), debug=True)
            print(result)
        else:
            raise ValueError("provide a single file to debug ...")
        return

    files = sorted(glob(str(path / "*" / "*")))
    total = len(files)

    with Pool(30) as pool:
        with open(str(path) + ".csv", "w") as output:
            writer = csv.writer(output)
            iterator = pool.imap(partial(parse_file, debug=False), files, chunksize=100)
            for result in _progress(
                iterator,
                enabled=not args.no_progress,
                total=total,
                desc=f"Parsing {path.name or path}",
            ):
                writer.writerow(result)


if __name__ == "__main__":
    main()
