"""The landing tree is read-only (spec §2, acceptance item 1).

"Nothing writes back into the raw landing directory. Ever. Enforce it — open source files
read-only and assert it in a test."
"""

from __future__ import annotations

import ast
import hashlib
from pathlib import Path

import pytest

SRC = Path(__file__).resolve().parents[2] / "src" / "aimternet"
DAGS = Path(__file__).resolve().parents[2] / "dags"

WRITE_MODES = {"w", "a", "x", "w+", "a+", "r+", "wb", "ab", "xb", "wb+", "ab+", "rb+"}


def _python_files() -> list[Path]:
    return sorted(SRC.rglob("*.py")) + sorted(DAGS.rglob("*.py"))


def test_no_source_file_opens_the_landing_tree_for_writing() -> None:
    """Static check: every open() in the package uses a read mode.

    Deliberately conservative -- it flags any write-mode open on a path the reader modules
    produce. Writers target the work, quarantine and Bronze locations, never the landing tree.
    """
    from aimternet.io import readers

    reader_source = Path(readers.__file__).read_text()
    tree = ast.parse(reader_source)
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            if node.func.attr != "open":
                continue
            mode = "r"
            if node.args:
                first = node.args[0]
                if isinstance(first, ast.Constant) and isinstance(first.value, str):
                    mode = first.value
            for keyword in node.keywords:
                if keyword.arg == "mode" and isinstance(keyword.value, ast.Constant):
                    mode = str(keyword.value.value)
            assert mode not in WRITE_MODES, (
                f"aimternet.io.readers opens a source file with mode {mode!r}; "
                f"the landing tree is read-only"
            )


def test_readers_never_import_shutil_or_os_remove() -> None:
    from aimternet.io import readers

    source = Path(readers.__file__).read_text()
    for banned in ("shutil", "os.remove", "os.unlink", "Path.unlink", "rmtree"):
        assert banned not in source, f"aimternet.io.readers references {banned}"


@pytest.mark.slow
def test_a_full_validation_run_does_not_modify_the_landing_tree(raw_landing: Path) -> None:
    """Behavioural check: hash a sample before and after, and compare.

    A static check can be fooled; this one cannot. Kept to a sample so it stays quick --
    hashing 4.4 GB on every test run would not be.
    """
    from aimternet.pipeline.validation.engine import Validator

    sample = sorted(raw_landing.rglob("*.csv"))[:40]
    sample += sorted((raw_landing / "legacy_batches").rglob("workstation_events.json"))[:5]
    sample += sorted((raw_landing / "telemetry").glob("*/00.json"))[:5]
    assert sample, "expected files to sample"

    def fingerprint() -> dict[str, tuple[int, float, str]]:
        return {
            str(p): (
                p.stat().st_size,
                p.stat().st_mtime,
                hashlib.sha256(p.read_bytes()).hexdigest(),
            )
            for p in sample
        }

    before = fingerprint()
    Validator(raw_landing).run(telemetry_days=1)
    after = fingerprint()

    changed = [path for path in before if before[path] != after[path]]
    assert changed == [], f"validation modified the landing tree: {changed}"
