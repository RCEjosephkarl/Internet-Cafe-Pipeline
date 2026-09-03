"""DAG import and structure tests (spec §9: "DAGs import cleanly").

These run without a scheduler or a metadata database. They catch the mistakes that actually
break a deployment: a DAG that fails to parse, a DAG whose id does not match its filename,
a scheduled DAG that would backfill two months of runs on first unpause, and — the one the
spec cares about most — business logic that has crept into a DAG file (§8.8).
"""

from __future__ import annotations

import ast
import importlib.util
import sys
from pathlib import Path

import pytest

DAGS_DIR = Path(__file__).resolve().parents[2] / "dags"

EXPECTED_DAGS = {
    "bootstrap_raw_landing",
    "rds_to_s3_incremental",
    "dynamodb_to_s3_incremental",
    "curate_silver_gold",
    "load_redshift",
    "reconcile_data",
}


def _dag_files() -> list[Path]:
    return sorted(p for p in DAGS_DIR.glob("*.py") if not p.name.startswith("_"))


@pytest.fixture(scope="module", autouse=True)
def _dags_on_path() -> None:
    """DAG files import `_common` as a sibling, the way Airflow loads them."""
    if str(DAGS_DIR) not in sys.path:
        sys.path.insert(0, str(DAGS_DIR))


def _load(path: Path):
    spec = importlib.util.spec_from_file_location(path.stem, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_all_six_dags_are_present() -> None:
    assert {p.stem for p in _dag_files()} == EXPECTED_DAGS


@pytest.mark.parametrize("path", _dag_files(), ids=lambda p: p.stem)
def test_dag_file_imports_cleanly(path: Path) -> None:
    _load(path)


@pytest.mark.parametrize("path", _dag_files(), ids=lambda p: p.stem)
def test_dag_id_matches_the_filename(path: Path) -> None:
    """An id that drifts from its filename makes a DAG very hard to find."""
    source = path.read_text()
    assert f'dag_id="{path.stem}"' in source


@pytest.mark.parametrize("path", _dag_files(), ids=lambda p: p.stem)
def test_no_dag_backfills_on_first_unpause(path: Path) -> None:
    """catchup=False everywhere. The start dates are historical; catchup would fire
    dozens of runs the moment someone unpauses a DAG."""
    assert "catchup=False" in path.read_text()


def test_the_bootstrap_dag_is_manual_only() -> None:
    """§6.1: manual trigger only. It is the one DAG that reads the EC2 landing directory,
    and it is not meant to run twice by accident."""
    source = (DAGS_DIR / "bootstrap_raw_landing.py").read_text()
    assert "schedule=None" in source


@pytest.mark.parametrize(
    "path", [p for p in _dag_files() if p.stem != "bootstrap_raw_landing"], ids=lambda p: p.stem
)
def test_ongoing_dags_never_read_the_landing_directory(path: Path) -> None:
    """§3: after the bootstrap, S3 is the source of record."""
    source = path.read_text()
    for forbidden in ("raw_landing", "AIMTERNET_RAW_LANDING", "legacy_batches"):
        assert forbidden not in source, (
            f"{path.name} references {forbidden}; only the bootstrap may read the EC2 "
            f"landing directory"
        )


@pytest.mark.parametrize("path", _dag_files(), ids=lambda p: p.stem)
def test_dag_files_contain_no_business_logic(path: Path) -> None:
    """§8.8: "DAG files stay thin. They import and call src/aimternet/."

    Checked structurally rather than by eye: a DAG file may define the DAG and its tasks,
    but it must not define classes, and no function in it may be long enough to be hiding
    a transformation.
    """
    tree = ast.parse(path.read_text())

    classes = [n.name for n in ast.walk(tree) if isinstance(n, ast.ClassDef)]
    assert classes == [], f"{path.name} defines classes: {classes}"

    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef):
            continue
        # The @dag-decorated function is the DAG definition itself: it declares tasks and
        # their dependencies, which is structure rather than logic. Its tasks are what must
        # stay short.
        decorators = {
            d.func.id if isinstance(d, ast.Call) and isinstance(d.func, ast.Name)
            else d.id if isinstance(d, ast.Name) else ""
            for d in node.decorator_list
        }
        if "dag" in decorators:
            continue
        body_lines = (node.end_lineno or 0) - node.lineno
        assert body_lines <= 40, (
            f"{path.name}::{node.name} is {body_lines} lines — too long for a DAG task. "
            f"Move the logic into src/aimternet/ and call it."
        )


@pytest.mark.parametrize("path", _dag_files(), ids=lambda p: p.stem)
def test_dags_import_from_the_package(path: Path) -> None:
    """The inverse of the previous test: a thin DAG must actually delegate somewhere."""
    assert "aimternet" in path.read_text()


def test_tuning_comes_from_airflow_variables() -> None:
    """The Airflow lens notebook changes behaviour by writing Variables; the DAGs read
    them. Hardcoding these would make that notebook a decoration."""
    source = (DAGS_DIR / "bootstrap_raw_landing.py").read_text()
    assert "aimternet_telemetry_days" in source
    assert "aimternet_load_threads" in source


def test_variable_lookup_survives_a_missing_metadata_database() -> None:
    """DAG *parsing* must never depend on Airflow's database being reachable."""
    from _common import int_variable, variable

    assert variable("definitely_not_a_real_variable", "fallback") == "fallback"
    assert int_variable("definitely_not_a_real_variable", 7) == 7
