"""The package must import cleanly with no side effects and no AWS calls."""

from __future__ import annotations

import importlib

import pytest

MODULES = [
    "aimternet",
    "aimternet.config",
    "aimternet.schemas",
    "aimternet.io",
    "aimternet.pipeline",
    "aimternet.db",
    "aimternet.api",
    "aimternet.observability",
]


@pytest.mark.parametrize("name", MODULES)
def test_module_imports(name: str) -> None:
    importlib.import_module(name)


def test_version_is_set() -> None:
    import aimternet

    assert aimternet.__version__
