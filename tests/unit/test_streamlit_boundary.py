"""The Streamlit dashboard must be an HTTP client and nothing more.

Mirrors ``test_notebook_boundary.py``'s enforcement of the same rule for
``pos_terminal.ipynb``: the dashboard holds no database credentials of its own and talks to
``/v1/metrics/*`` over HTTP only. Every ``.py`` file under ``streamlit_app/`` is grepped for a
forbidden import; none may appear.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

STREAMLIT_APP = Path(__file__).resolve().parents[2] / "streamlit_app"

FORBIDDEN_IMPORTS = (
    "psycopg2", "psycopg", "sqlalchemy", "boto3", "botocore", "redshift_connector",
    "duckdb", "pymysql", "asyncpg",
)

FORBIDDEN_MODULES = (
    "aimternet.db",
    "aimternet.pipeline",
    "aimternet.io",
    "aimternet.config.settings",
)

IMPORT_PATTERN = re.compile(r"^\s*(import|from)\s+{module}\b", re.MULTILINE)


def _all_python_files() -> list[Path]:
    return sorted(STREAMLIT_APP.rglob("*.py"))


@pytest.fixture(scope="module")
def source_by_file() -> dict[Path, str]:
    return {path: path.read_text() for path in _all_python_files()}


def test_the_streamlit_app_exists() -> None:
    assert STREAMLIT_APP.is_dir(), "streamlit_app/ is the dashboard and must ship"
    assert (STREAMLIT_APP / "Home.py").is_file()
    assert list((STREAMLIT_APP / "pages").glob("*.py")), "no pages under streamlit_app/pages/"


@pytest.mark.parametrize("module", FORBIDDEN_IMPORTS)
def test_no_database_or_aws_driver_is_imported(
    source_by_file: dict[Path, str], module: str
) -> None:
    pattern = re.compile(IMPORT_PATTERN.pattern.format(module=re.escape(module)))
    for path, source in source_by_file.items():
        assert not pattern.search(source), (
            f"{path.relative_to(STREAMLIT_APP)} imports {module}. The dashboard talks to the "
            f"metrics API over HTTP only; it holds no database credentials of its own."
        )


@pytest.mark.parametrize("module", FORBIDDEN_MODULES)
def test_no_file_reaches_into_the_project_internals(
    source_by_file: dict[Path, str], module: str
) -> None:
    pattern = re.compile(IMPORT_PATTERN.pattern.format(module=re.escape(module)))
    for path, source in source_by_file.items():
        assert not pattern.search(source), (
            f"{path.relative_to(STREAMLIT_APP)} imports {module}, which bypasses the API "
            f"boundary"
        )


def test_the_api_client_actually_uses_http() -> None:
    """The inverse check: proving no driver is imported is only half the claim."""
    client_source = (STREAMLIT_APP / "lib" / "api_client.py").read_text()
    assert re.search(r"^\s*import\s+httpx\b", client_source, re.MULTILINE)
    assert "/v1/metrics" in client_source


def test_pages_only_call_the_shared_api_client_module(source_by_file: dict[Path, str]) -> None:
    """Every page's HTTP calls should funnel through lib/api_client.py, not a fresh client."""
    for path, source in source_by_file.items():
        if path.name == "api_client.py":
            continue
        assert "httpx." not in source, (
            f"{path.relative_to(STREAMLIT_APP)} calls httpx directly instead of going through "
            f"lib/api_client.py"
        )
