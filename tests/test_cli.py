from __future__ import annotations

import sys
from pathlib import Path

from cik_cusip_mapping import cli


class DummyFrame:
    def __init__(self, size: int):
        self._size = size

    def __len__(self) -> int:  # pragma: no cover - trivial
        return self._size


def test_run_pipeline_cli_emits_summary(monkeypatch, tmp_path, capsys):
    output_root = tmp_path / "outputs"

    def fake_run_pipeline(**kwargs):
        assert kwargs["output_root"] == Path(output_root)
        assert kwargs["emit_dynamics"] is True
        assert kwargs["events_output_root"] == Path(".")
        dynamics_path = kwargs["dynamics_output_file"]
        assert dynamics_path == Path("cik-cusip-dynamics.csv")
        return DummyFrame(3), DummyFrame(2)

    monkeypatch.setattr(cli.pipeline, "run_pipeline", fake_run_pipeline)

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "cik-cusip-mapping",
            "--output-root",
            str(output_root),
            "--output-file",
            "maps.csv",
        ],
    )

    cli.run_pipeline_cli()

    captured = capsys.readouterr()
    assert "Generated 3 CIK/CUSIP mappings" in captured.out
    assert "Aggregated 2 dynamics rows" in captured.out
