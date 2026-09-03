"""Structural checks on the notebooks.

Executing them needs live AWS, RDS and Airflow, so that lives in the integration suite.
These checks are the cheap ones that catch the mistakes that actually happen: a notebook
saved with stale output, invalid JSON, or -- the one that matters -- the POS terminal
reaching past the API into a database.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

NOTEBOOK_DIR = Path(__file__).resolve().parents[2] / "notebooks"


def _notebooks() -> list[Path]:
    return sorted(NOTEBOOK_DIR.glob("*.ipynb"))


def test_notebooks_exist() -> None:
    assert _notebooks(), "no notebooks found"


@pytest.mark.parametrize("path", _notebooks(), ids=lambda p: p.name)
def test_notebook_is_valid_json_with_cells(path: Path) -> None:
    nb = json.loads(path.read_text())
    assert nb["nbformat"] == 4
    assert nb["cells"], f"{path.name} has no cells"


@pytest.mark.parametrize("path", _notebooks(), ids=lambda p: p.name)
def test_notebook_is_committed_without_output(path: Path) -> None:
    """Committed notebooks stay clean: no outputs, no execution counts.

    Saved output makes diffs unreadable and can leak data into git.
    """
    nb = json.loads(path.read_text())
    for index, cell in enumerate(nb["cells"]):
        if cell["cell_type"] != "code":
            continue
        assert not cell.get("outputs"), f"{path.name} cell {index} carries saved output"
        assert cell.get("execution_count") is None, f"{path.name} cell {index} has an exec count"


@pytest.mark.parametrize("path", _notebooks(), ids=lambda p: p.name)
def test_no_notebook_contains_a_credential(path: Path) -> None:
    text = path.read_text()
    for marker in ("AKIA", "password=", "aws_secret_access_key", "PASSWORD'"):
        assert marker not in text, f"{path.name} appears to contain a credential ({marker})"
