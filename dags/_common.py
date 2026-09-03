"""Shared helpers for the AIMternet DAGs.

DAG files stay thin (spec §8.8): they import and call ``src/aimternet/``, and hold no
business logic. What lives here is the small amount of glue every DAG needs — reading its
tuning from Airflow Variables so ``notebooks/airflow_lens.ipynb`` can change behaviour
without a code change or a redeploy.
"""

from __future__ import annotations

import os
import sys
from datetime import timedelta
from typing import Any

# The package is installed editable, but a DAG parsed by a scheduler that was started from a
# different environment should still find it.
_SRC = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

DEFAULT_ARGS: dict[str, Any] = {
    "owner": "aimternet",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "depends_on_past": False,
}

TAGS = ["aimternet"]


def variable(key: str, default: str) -> str:
    """Read an Airflow Variable, falling back to a default.

    Variables are how the pipeline is tuned at run time: telemetry days, thread counts,
    batch sizes, the D2 policy. The Airflow lens notebook writes them; the DAGs read them.
    A missing Variable must never break DAG *parsing*, so this never raises.
    """
    try:
        from airflow.sdk import Variable

        return str(Variable.get(key, default_var=default))
    except Exception:
        return default


def int_variable(key: str, default: int) -> int:
    try:
        return int(variable(key, str(default)))
    except ValueError:
        return default
