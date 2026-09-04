"""`aimternet bootstrap` and dags/bootstrap_raw_landing.py must stay the same pipeline.

The CLI subcommand mirrors the DAG stage for stage, and both are meant to be the same run by
two routes. Nothing checked that. Two divergences had already appeared:

* the CLI applied `--telemetry-days` to `workstation_events` as well as to telemetry, while
  the DAG passes it only for telemetry -- so `bootstrap --telemetry-days 3` silently loaded
  three days of events, contradicting the flag's own help text;
* the CLI returned 0 whatever happened to the DynamoDB per-file failures, while the DAG raises
  on them -- so `make bootstrap` reported success over a partial load.

These tests compare the two by what they *call*, not by their text: a rename that keeps the
behaviour identical should not fail, and a stage silently dropped from one side should.

Related: `make bootstrap` invoked `cli.py bootstrap` from the very first commit, and the
subcommand did not exist until eleven phases later. Nothing noticed, so the Makefile targets
are checked here too.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
CLI = ROOT / "src" / "aimternet" / "pipeline" / "cli.py"
DAG = ROOT / "dags" / "bootstrap_raw_landing.py"
MAKEFILE = ROOT / "Makefile"

#: The package functions the bootstrap runs, in stage order (spec §6.1 A-D). Each side reaches
#: them by its own route -- the DAG imports them inside tasks, the CLI through its `_cmd_*`
#: wrappers -- so the shared assertion is that both end up calling this same set.
BOOTSTRAP_CALLS = {
    "build_manifest",
    "upload_bronze",
    "verify_bronze",
    "load_all",       # RDS
    "ensure_tables",  # DynamoDB
    "load_dataset",   # DynamoDB
}


def _tree(path: Path) -> ast.Module:
    return ast.parse(path.read_text(encoding="utf-8"))


def _function(tree: ast.Module, name: str) -> ast.FunctionDef:
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return node
    raise AssertionError(f"{name} not found")


def _called_names(node: ast.AST) -> set[str]:
    """Every bare function name called anywhere under `node`."""
    names = set()
    for child in ast.walk(node):
        if isinstance(child, ast.Call):
            func = child.func
            if isinstance(func, ast.Name):
                names.add(func.id)
            elif isinstance(func, ast.Attribute):
                names.add(func.attr)
    return names


def _cli_bootstrap_calls() -> set[str]:
    """What `_cmd_bootstrap` reaches, following the `_cmd_*` wrappers it delegates to."""
    tree = _tree(CLI)
    entry = _function(tree, "_cmd_bootstrap")
    reached = _called_names(entry)
    for name in sorted(reached):
        if name.startswith("_cmd_"):
            reached |= _called_names(_function(tree, name))
    return reached


def _dag_calls() -> set[str]:
    return _called_names(_tree(DAG))


# ------------------------------------------------------------------ stage parity


def test_the_cli_runs_every_stage_the_dag_runs() -> None:
    missing = BOOTSTRAP_CALLS - _cli_bootstrap_calls()
    assert not missing, f"`aimternet bootstrap` never reaches: {sorted(missing)}"


def test_the_dag_runs_every_stage_the_cli_runs() -> None:
    missing = BOOTSTRAP_CALLS - _dag_calls()
    assert not missing, f"bootstrap_raw_landing.py never reaches: {sorted(missing)}"


def test_the_two_agree_on_which_stages_exist() -> None:
    """Neither side may grow a stage the other does not have."""
    shared = _cli_bootstrap_calls() & _dag_calls() & BOOTSTRAP_CALLS
    assert shared == BOOTSTRAP_CALLS


# ------------------------------------------ the day limit is telemetry-only, on both sides


def test_the_day_limit_is_never_applied_to_workstation_events() -> None:
    """`--telemetry-days` limits telemetry. The CLI used to apply it to events as well.

    Asserted on both files: whichever way each expresses it, neither may pass a day limit
    alongside `workstation_events`.
    """
    for path in (CLI, DAG):
        source = path.read_text(encoding="utf-8")
        for match in re.finditer(r"datasets=\[([^\]]*)\][^)]*?days=([^,)\n]+)", source):
            datasets, days = match.group(1), match.group(2).strip()
            if "workstation_events" not in datasets:
                continue
            # A day limit reaching workstation_events at all is the bug -- including via a
            # combined list, which is exactly how it was written: one `days` handed to
            # ["workstation_events", "telemetry"] together. Load the two separately, or scope
            # the limit per dataset the way the DAG does.
            assert days == "None", (
                f"{path.name} passes days={days} to a call loading workstation_events "
                f"(datasets=[{datasets}]); the day limit is a telemetry limit"
            )


def test_the_dag_scopes_the_day_limit_to_telemetry() -> None:
    source = DAG.read_text(encoding="utf-8")
    assert 'days=days if dataset == "telemetry" else None' in source


# ------------------------------------------------------- failures must not exit clean


def test_the_cli_reports_dynamodb_failures_in_its_exit_status() -> None:
    """The DAG raises on report.failures; the CLI must not return 0 over them."""
    body = ast.unparse(_function(_tree(CLI), "_cmd_load_dynamodb"))
    assert "failures" in body, "_cmd_load_dynamodb ignores report.failures entirely"
    assert not re.search(r"\n    return 0\s*$", body), (
        "_cmd_load_dynamodb returns 0 unconditionally, so a partial load exits clean"
    )


def test_the_dag_raises_on_dynamodb_failures() -> None:
    assert "if report.failures:" in DAG.read_text(encoding="utf-8")


# ------------------------------------------------- every Makefile target is a real subcommand


def _cli_subcommands() -> set[str]:
    source = CLI.read_text(encoding="utf-8")
    return set(re.findall(r'sub\.add_parser\(\s*"([a-z0-9-]+)"', source))


@pytest.mark.parametrize(
    "invoked",
    sorted(set(re.findall(r"cli\s+([a-z0-9-]+)", MAKEFILE.read_text(encoding="utf-8")))),
)
def test_every_subcommand_the_makefile_invokes_exists(invoked: str) -> None:
    assert invoked in _cli_subcommands(), (
        f"the Makefile runs `cli {invoked}`, which argparse would reject. "
        "`make bootstrap` was this exact error for eleven phases."
    )
