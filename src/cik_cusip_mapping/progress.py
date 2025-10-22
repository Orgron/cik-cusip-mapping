"""Helpers for configuring tqdm progress bars."""

from __future__ import annotations

from collections.abc import Callable
import importlib.util
from typing import Any

from tqdm.auto import tqdm as auto_tqdm


ProgressFactory = Callable[..., Any]


def _detect_notebook_environment() -> bool:
    """Return ``True`` when running inside a Jupyter notebook shell."""

    ipython_spec = importlib.util.find_spec("IPython")
    if ipython_spec is None:
        return False
    from IPython import get_ipython  # type: ignore import-not-found

    shell = get_ipython()
    if shell is None:
        return False
    shell_name = shell.__class__.__name__
    return "ZMQInteractiveShell" in shell_name


def resolve_tqdm(use_notebook: bool | None = None) -> ProgressFactory:
    """Return the appropriate tqdm factory for the current environment."""

    notebook_requested = use_notebook
    if notebook_requested is None:
        notebook_requested = _detect_notebook_environment()
    if notebook_requested:
        notebook_spec = importlib.util.find_spec("tqdm.notebook")
        if notebook_spec is not None:
            from tqdm.notebook import tqdm as notebook_tqdm  # type: ignore import-not-found

            return notebook_tqdm
    return auto_tqdm
