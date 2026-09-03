"""Guards on what may enter git history (spec §0.8, §8).

These are cheap and they fail loudly, which is the point: a committed .env or a
committed .pyc is the kind of mistake nobody notices until it is in a public repo.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

SECRET_PATTERNS = ("*_pw.txt", "*.pem", "*.key", ".env")
FORBIDDEN_SUFFIXES = (".pyc", ".pyo", ".parquet")


def _tracked_files(repo_root: Path) -> list[str]:
    out = subprocess.run(
        ["git", "ls-files"], cwd=repo_root, capture_output=True, text=True, check=True
    )
    return out.stdout.splitlines()


def test_no_compiled_or_generated_artifacts_tracked(repo_root: Path) -> None:
    offenders = [f for f in _tracked_files(repo_root) if f.endswith(FORBIDDEN_SUFFIXES)]
    assert offenders == [], f"generated artifacts are tracked in git: {offenders}"


def test_no_secret_files_tracked(repo_root: Path) -> None:
    tracked = _tracked_files(repo_root)
    offenders = [
        f
        for f in tracked
        if Path(f).name == ".env"
        or f.endswith(("_pw.txt", ".pem", ".key"))
        or Path(f).name == "credentials"
    ]
    assert offenders == [], f"secret-bearing files are tracked in git: {offenders}"


def test_gitignore_covers_the_dangerous_patterns(repo_root: Path) -> None:
    ignored = (repo_root / ".gitignore").read_text()
    for pattern in (*SECRET_PATTERNS, "__pycache__/", ".terraform/", ".ipynb_checkpoints/"):
        assert pattern in ignored, f".gitignore is missing {pattern!r}"


def test_env_example_exists_and_holds_no_real_secrets(repo_root: Path) -> None:
    example = repo_root / ".env.example"
    assert example.is_file(), ".env.example must ship with the repo (§8)"
    text = example.read_text()
    assert "AIMTERNET_S3_BUCKET" in text
    # A real 40-char AWS secret key or a real host would mean someone pasted their .env here.
    assert "rds.amazonaws.com" not in text.replace("your-instance.rds.amazonaws.com", "")
    assert "AKIA" not in text
