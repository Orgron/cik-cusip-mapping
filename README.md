# cik-cusip mapping

## I decided not to maintain this repo as there are good forks that continue what I do here

*** If you just want the mapping, download cik-cusip-maps.csv ***

This repository produces the link between cik and cusip using EDGAR 13D and 13G fillings, that is more robust than Compustat (due to backward filling of new cusip to old records). It is a competitor to WRDS SEC platform while this one is free.

This project now targets **Python 3.12**. The scripts rely only on the standard library plus very common third-party dependencies (e.g. pandas). Install any missing packages with `pip install <package>`.

### Quick start: run the full pipeline with one command

```
python run_pipeline.py
```

The command above will:

1. Download the full EDGAR master index.
2. Download every 13D and 13G filing referenced in the index.
3. Parse the CUSIPs from the downloaded filings.
4. Post-process the parsed data into the final `cik-cusip-maps.csv` file.

You can customise the run, for example to use a different output directory or run only for form 13G:

```
python run_pipeline.py --forms 13G --output-root data
```

Run `python run_pipeline.py --help` for the full list of options, including skipping individual steps if you have already generated intermediate outputs.  The pipeline enforces the SEC's rate guidance by defaulting to **10 requests per second**; adjust it and supply identifying contact details when needed:

```
python run_pipeline.py --requests-per-second 5 --sec-name "Jane Doe" --sec-email jane@example.com
```

Any values you provide are forwarded to the underlying download scripts so that all EDGAR requests share the same rate limit and headers.

### Running the automated tests

Install the test dependencies (primarily `pytest`) and execute:

```
pip install -r requirements-dev.txt  # optional helper; install pytest manually if you prefer
pytest
```

The tests exercise the orchestration logic in `run_pipeline.py` to ensure it remains compatible with Python 3.12 and correctly sequences each stage of the workflow.

### Running individual steps (legacy workflow)

dl_idx.py will download the EDGAR index file containing addresses for each filing, i.e. full_index.csv is generated

```
python dl_idx.py --requests-per-second 5 --sec-email jane@example.com
```

dl.py will download a certain type of filing, check form_type.txt for available filing types. for example,
```python
python dl.py 13G 13G --sec-email jane@example.com # this will download all 13G (second 13G) filing into 13G (first 13G) folder
```
```python
python parse_cusip.py 13G # this will process all files in 13G directory, creating a file called 13G.csv with filing name, cik, cusip number.
```
Finally, you can clean the resulting csv files and get the mapping
```python
python post_proc.py 13G.csv 13D.csv
# This will process both 13G.csv and 13D.csv and generate the mapping file
```

If you do not care obtaining the original data, just download cik-cusip-maps.csv, it has the mapping without timestamp information, but should be good if you use it for merging databases. Please deal with duplications yourself.

The reason why I do not provide timestamp is because there will be truncations due to timing of the filings. For example, when filings are filed in 2005 and 2007 for a link, I can only see the link in 2005 and 2007, but the link should be valid in 2006 too. One way to fix this is to interpolate the link to 2006. However, when filing ends in 2006, we do not know when should the link is valid to and how long after we should extrapolate, i.e. One could extrapolate to 2020 but we do not know the true date the link ends. This is arbitrary choice of the user, therefore I remove the timestamp for you to deal with yourself. For database merging purpose, this should be fine because two databases you are merging should have timestamp and it's rare for duplicated links to exist at some given time.

*** Finally, if you find this repo useful, please give it a STAR ***
