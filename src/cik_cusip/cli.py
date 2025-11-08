"""Command-line interface for CIK-CUSIP mapping tool."""

import os
from datetime import datetime

import click

from .processor import download_filing_txt, process_filings


@click.group()
@click.version_option(version="2.0.0", prog_name="cik-cusip")
def cli():
    """
    CIK-CUSIP Mapping - Extract CUSIPs from SEC filings.

    A tool for downloading SEC EDGAR indices and extracting CUSIP identifiers
    from 13D and 13G forms.
    """
    pass


@cli.command()
@click.option(
    "--index-dir",
    default="data/indices",
    help="Directory for index files (default: data/indices)",
)
@click.option(
    "--output",
    default="data/cusips.csv",
    help="Path to output CSV (default: data/cusips.csv)",
)
@click.option(
    "--skip-index",
    is_flag=True,
    help="Skip index download if files exist",
)
@click.option(
    "--sec-name",
    envvar="SEC_NAME",
    help="Your name for SEC User-Agent (or set SEC_NAME env var)",
)
@click.option(
    "--sec-email",
    envvar="SEC_EMAIL",
    help="Your email for SEC headers (or set SEC_EMAIL env var)",
)
@click.option(
    "--rate",
    type=float,
    default=10.0,
    help="Requests per second (default: 10.0)",
)
@click.option(
    "--cik-filter",
    type=click.Path(exists=True),
    help="Path to text file with CIKs to filter (one per line)",
)
@click.option(
    "--all",
    "download_all",
    is_flag=True,
    help="Download all available indices (1993 to present)",
)
@click.option(
    "--start-year",
    type=int,
    help="Starting year (default: current year if not --all)",
)
@click.option(
    "--start-quarter",
    type=click.IntRange(1, 4),
    default=1,
    help="Starting quarter 1-4 (default: 1)",
)
@click.option(
    "--end-year",
    type=int,
    help="Ending year (default: current year)",
)
@click.option(
    "--end-quarter",
    type=click.IntRange(1, 4),
    help="Ending quarter 1-4 (default: current quarter)",
)
def extract(
    index_dir,
    output,
    skip_index,
    sec_name,
    sec_email,
    rate,
    cik_filter,
    download_all,
    start_year,
    start_quarter,
    end_year,
    end_quarter,
):
    """
    Extract CUSIPs from SEC 13D/13G filings.

    Downloads SEC EDGAR indices, extracts filings, and parses CUSIP identifiers.
    Results are written to a CSV file with columns: cik, company_name, form,
    date, cusip, and accession_number.

    Examples:

      # Extract from current quarter only
      cik-cusip extract --sec-name "Jane Doe" --sec-email "jane@example.com"

      # Download all historical data (1993-present)
      cik-cusip extract --all --sec-name "Jane Doe" --sec-email "jane@example.com"

      # Download specific year range
      cik-cusip extract --start-year 2020 --end-year 2024

      # Filter for specific CIKs
      cik-cusip extract --cik-filter my_ciks.txt --all
    """
    # Validate SEC credentials
    if not sec_name or not sec_email:
        click.echo(
            "Error: SEC credentials required. Provide --sec-name and --sec-email, "
            "or set SEC_NAME and SEC_EMAIL environment variables.",
            err=True,
        )
        raise click.Abort()

    # Handle --all flag
    if download_all:
        start_year = 1993
        start_quarter = 1
        end_year = None  # Will default to current year
        end_quarter = None  # Will default to current quarter
    else:
        # If no year specified, default to current year only
        if start_year is None and end_year is None:
            current_year = datetime.now().year
            current_quarter = (datetime.now().month - 1) // 3 + 1
            start_year = current_year
            start_quarter = current_quarter
            end_year = current_year
            end_quarter = current_quarter
            click.echo(
                f"No year range specified, defaulting to current quarter: {current_year} Q{current_quarter}"
            )
            click.echo(
                "Use --all to download all historical indices, or specify --start-year/--end-year"
            )

    # Call the main processing function
    process_filings(
        index_dir=index_dir,
        output_csv=output,
        forms=("13D", "13G"),
        sec_name=sec_name,
        sec_email=sec_email,
        requests_per_second=rate,
        skip_index_download=skip_index,
        start_year=start_year,
        start_quarter=start_quarter,
        end_year=end_year,
        end_quarter=end_quarter,
        cik_filter_file=cik_filter,
    )


@cli.command()
@click.argument("cik")
@click.argument("accession_number")
@click.option(
    "--output",
    "-o",
    type=click.Path(),
    help="Output file path (default: {accession_number}.txt)",
)
@click.option(
    "--sec-name",
    envvar="SEC_NAME",
    help="Your name for SEC User-Agent (or set SEC_NAME env var)",
)
@click.option(
    "--sec-email",
    envvar="SEC_EMAIL",
    help="Your email for SEC headers (or set SEC_EMAIL env var)",
)
def download(cik, accession_number, output, sec_name, sec_email):
    """
    Download a SEC filing in text format by CIK and accession number.

    CIK: Company's Central Index Key (e.g., 813828)
    ACCESSION_NUMBER: SEC accession number format NNNNNNNNNN-NN-NNNNNN (e.g., 0001104659-06-026838)

    Examples:

      # Download a specific filing
      cik-cusip download 813828 0001104659-06-026838

      # Download with custom output path
      cik-cusip download 813828 0001104659-06-026838 -o myfile.txt
    """
    # Validate SEC credentials
    if not sec_name or not sec_email:
        click.echo(
            "Error: SEC credentials required. Provide --sec-name and --sec-email, "
            "or set SEC_NAME and SEC_EMAIL environment variables.",
            err=True,
        )
        raise click.Abort()

    # Default output path if not specified
    if not output:
        output = f"{accession_number}.txt"

    # Download the filing
    try:
        download_filing_txt(
            accession_number=accession_number,
            cik=cik,
            output_path=output,
            sec_name=sec_name,
            sec_email=sec_email,
        )
        click.echo(f"✓ Successfully downloaded filing to {output}")
    except Exception as e:
        click.echo(f"Error downloading filing: {e}", err=True)
        raise click.Abort()


if __name__ == "__main__":
    cli()
