#!/usr/bin/python
import argparse
import csv
import os
from pathlib import Path

import requests

from sec_utils import RateLimiter, build_request_headers


def download_filings(
    filing: str,
    folder: str,
    requests_per_second: float,
    name: str | None,
    email: str | None,
) -> None:
    headers = build_request_headers(name, email)
    limiter = RateLimiter(requests_per_second)

    to_dl = []
    with open("full_index.csv", "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if filing in row["form"]:
                to_dl.append(row)

    len_ = len(to_dl)
    print(len_)
    print("start to download")

    for n, row in enumerate(to_dl):
        print(f"{n} out of {len_}")
        cik = row["cik"].strip()
        date = row["date"].strip()
        year = row["date"].split("-")[0].strip()
        month = row["date"].split("-")[1].strip()
        url = row["url"].strip()
        accession = url.split(".")[0].split("-")[-1]
        Path(f"./{folder}/{year}_{month}").mkdir(parents=True, exist_ok=True)
        file_path = f"./{folder}/{year}_{month}/{cik}_{date}_{accession}.txt"
        if os.path.exists(file_path):
            continue
        try:
            limiter.wait()
            response = requests.get(
                f"https://www.sec.gov/Archives/{url}",
                headers=headers,
                timeout=60,
            )
            response.raise_for_status()
            txt = response.text
            with open(file_path, "w", errors="ignore") as f:
                f.write(txt)
        except Exception:
            print(f"{cik}, {date} failed to download")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("filing", type=str)
    parser.add_argument("folder", type=str)
    parser.add_argument(
        "--requests-per-second",
        type=float,
        default=10.0,
        help="Maximum number of requests per second when downloading filings.",
    )
    parser.add_argument("--sec-name", help="Contact name to include in SEC requests.")
    parser.add_argument("--sec-email", help="Contact email to include in SEC requests.")

    args = parser.parse_args()
    download_filings(
        args.filing,
        args.folder,
        args.requests_per_second,
        args.sec_name,
        args.sec_email,
    )
