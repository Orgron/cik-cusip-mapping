#!/bin/python
import argparse
import csv
import logging
from pathlib import Path

import requests

from sec_utils import RateLimiter, build_request_headers


logger = logging.getLogger(__name__)


def download_master_index(requests_per_second: float, name: str | None, email: str | None) -> None:
    headers = build_request_headers(name, email)
    limiter = RateLimiter(requests_per_second)

    with open("master.idx", "wb") as f:
        for year in range(1994, 2023):
            for q in range(1, 5):
                print(year, q)
                limiter.wait()
                response = requests.get(
                    f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{q}/master.idx",
                    headers=headers,
                    timeout=60,
                )
                response.raise_for_status()
                f.write(response.content)


def write_full_index() -> None:
    master_path = Path("master.idx")
    with open("full_index.csv", "w", newline="", errors="ignore") as csvfile:
        wr = csv.writer(csvfile)
        wr.writerow(["cik", "comnam", "form", "date", "url"])
        with open(master_path, "r", encoding="latin1") as f:
            for r in f:
                if ".txt" in r:
                    wr.writerow(r.strip().split("|"))

    existed = master_path.exists()
    master_path.unlink(missing_ok=True)
    if existed:
        logger.info("Removed master.idx after creating full_index.csv.")
    else:
        logger.info("No master.idx file found to remove after creating full_index.csv.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--requests-per-second",
        type=float,
        default=10.0,
        help="Maximum number of requests per second when downloading master index files.",
    )
    parser.add_argument("--sec-name", help="Contact name to include in SEC requests.")
    parser.add_argument("--sec-email", help="Contact email to include in SEC requests.")
    args = parser.parse_args()

    download_master_index(args.requests_per_second, args.sec_name, args.sec_email)
    write_full_index()
