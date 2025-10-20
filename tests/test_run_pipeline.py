import os
import sys
from pathlib import Path
from typing import List

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import run_pipeline


class SubprocessRecorder:
    def __init__(self) -> None:
        self.calls: List[List[str]] = []

    def __call__(self, command: List[str], check: bool) -> None:
        self.calls.append(command)
        if len(command) >= 2 and command[1] == "post_proc.py":
            Path("cik-cusip-maps.csv").write_text("CIK,CUSIP\n")


@pytest.fixture(autouse=True)
def clean_generated_mapping():
    mapping_path = Path("cik-cusip-maps.csv")
    if mapping_path.exists():
        mapping_path.unlink()
    yield
    if mapping_path.exists():
        mapping_path.unlink()


def test_skip_index_requires_existing_full_index(tmp_path):
    with pytest.raises(FileNotFoundError):
        run_pipeline.main(["--skip-index"])


def test_skip_parse_requires_existing_csv(monkeypatch, tmp_path):
    recorder = SubprocessRecorder()
    monkeypatch.setattr(run_pipeline.subprocess, "run", recorder)

    with pytest.raises(FileNotFoundError):
        run_pipeline.main(["--skip-parse", "--output-root", str(tmp_path)])


def test_pipeline_invokes_all_steps(monkeypatch, tmp_path):
    recorder = SubprocessRecorder()
    monkeypatch.setattr(run_pipeline.subprocess, "run", recorder)

    folder_path = tmp_path / "13G"
    folder_arg = os.path.relpath(folder_path, start=Path.cwd())
    csv_path = f"{folder_arg}.csv"

    output_file = tmp_path / "final.csv"
    run_pipeline.main(
        [
            "--forms",
            "13G",
            "--output-root",
            str(tmp_path),
            "--output-file",
            str(output_file),
        ]
    )

    expected_calls = [
        [run_pipeline.sys.executable, "dl_idx.py"],
        [run_pipeline.sys.executable, "dl.py", "13G", folder_arg],
        [run_pipeline.sys.executable, "parse_cusip.py", folder_arg],
        [run_pipeline.sys.executable, "post_proc.py", csv_path],
    ]
    assert recorder.calls == expected_calls
    assert output_file.exists()
    assert not Path("cik-cusip-maps.csv").exists()
