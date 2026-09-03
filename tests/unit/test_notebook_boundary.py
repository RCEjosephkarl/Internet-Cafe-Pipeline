"""The POS notebook must be an HTTP client and nothing more (spec §7.3, acceptance item 12).

"Add a test that greps the notebook JSON for forbidden imports and fails the build if any
appear. The boundary should be enforced, not just requested."

This targets `pos_terminal.ipynb` only. The lens notebooks legitimately read databases —
that is what they are for — so a blanket rule across `notebooks/` would be wrong.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

NOTEBOOK = Path(__file__).resolve().parents[2] / "notebooks" / "pos_terminal.ipynb"

FORBIDDEN_IMPORTS = (
    "psycopg2", "psycopg", "sqlalchemy", "boto3", "botocore", "redshift_connector",
    "duckdb", "pymysql", "asyncpg",
)

# Reaching into the project's own data layer would bypass the API just as surely as
# importing a driver would.
FORBIDDEN_MODULES = (
    "aimternet.db",
    "aimternet.pipeline",
    "aimternet.io",
    "aimternet.config.settings",
)

SQL_STATEMENTS = re.compile(
    r"\b(SELECT\s+.+\s+FROM|INSERT\s+INTO|UPDATE\s+\w+\s+SET|DELETE\s+FROM|"
    r"CREATE\s+TABLE|DROP\s+TABLE|ALTER\s+TABLE)\b",
    re.IGNORECASE,
)

CREDENTIAL_MARKERS = (
    "AKIA", "aws_secret_access_key", "aws_access_key_id",
    "password=", "PGPASSWORD", "secret_key",
)


@pytest.fixture(scope="module")
def source() -> str:
    """Every line of code in the notebook, concatenated."""
    notebook = json.loads(NOTEBOOK.read_text())
    return "\n".join(
        "".join(cell["source"]) for cell in notebook["cells"] if cell["cell_type"] == "code"
    )


def test_the_pos_notebook_exists() -> None:
    assert NOTEBOOK.is_file(), f"{NOTEBOOK.name} is the POS terminal and must ship"


@pytest.mark.parametrize("module", FORBIDDEN_IMPORTS)
def test_no_database_or_aws_driver_is_imported(source: str, module: str) -> None:
    pattern = re.compile(rf"^\s*(import|from)\s+{re.escape(module)}\b", re.MULTILINE)
    assert not pattern.search(source), (
        f"pos_terminal.ipynb imports {module}. The POS talks to the API over HTTP only; "
        f"business rules live in the API so they exist in exactly one place."
    )


@pytest.mark.parametrize("module", FORBIDDEN_MODULES)
def test_the_notebook_does_not_reach_into_the_project_internals(
    source: str, module: str
) -> None:
    pattern = re.compile(rf"^\s*(import|from)\s+{re.escape(module)}\b", re.MULTILINE)
    assert not pattern.search(source), (
        f"pos_terminal.ipynb imports {module}, which bypasses the API boundary"
    )


def test_no_sql_appears_anywhere_in_the_notebook(source: str) -> None:
    match = SQL_STATEMENTS.search(source)
    assert match is None, f"pos_terminal.ipynb contains SQL: {match.group(0)!r}"


def test_no_credentials_are_embedded(source: str) -> None:
    notebook_text = NOTEBOOK.read_text()
    for marker in CREDENTIAL_MARKERS:
        assert marker not in notebook_text, (
            f"pos_terminal.ipynb appears to contain a credential ({marker})"
        )


def test_the_notebook_actually_uses_http(source: str) -> None:
    """The inverse check: proving it has no database driver is only half the claim."""
    assert re.search(r"^\s*import\s+requests\b", source, re.MULTILINE)
    assert "/v1/rentals/check-in" in source
    assert "/v1/concessions/purchases" in source
    assert "/v1/rentals/check-out" in source


def test_the_notebook_computes_no_prices_of_its_own(source: str) -> None:
    """§3: no business rule may be implemented twice. The till displays; it does not price."""
    for constant in ("0.10", "0.20", "1.25", "1.50", "* 0.9", "* 0.8"):
        assert constant not in source, (
            f"pos_terminal.ipynb contains {constant!r}, which looks like a pricing rule. "
            f"Pricing belongs to the API."
        )


def test_the_lens_notebooks_are_deliberately_exempt() -> None:
    """Guard the guard: db_lens is *supposed* to hold a database driver."""
    lens = NOTEBOOK.parent / "db_lens.ipynb"
    assert lens.is_file()
    assert "db_lens" in lens.name and lens.name != NOTEBOOK.name
