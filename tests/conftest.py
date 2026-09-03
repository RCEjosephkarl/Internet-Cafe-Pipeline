"""Shared pytest fixtures.

Nothing here may touch AWS, RDS or Redshift: the default `make test` run must work
on a laptop with no credentials. Tests that need live resources carry a marker.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="session")
def repo_root() -> Path:
    return REPO_ROOT


@pytest.fixture(scope="session")
def raw_landing() -> Path:
    """The read-only source tree. Falls back to the repo copy when the env var is unset."""
    configured = os.environ.get("AIMTERNET_RAW_LANDING")
    if configured and Path(configured).is_dir():
        return Path(configured)
    return REPO_ROOT / "data" / "raw-landing"
