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

# --------------------------------------------------------------------------------------
# Assets: what each stage produces, so the next stage runs when it lands rather than at a
# clock slot it hopes is late enough. The four DAGs used to be staggered on cron (:00, :15,
# :30, :45), which put a POS sale up to 1h45m away from the dashboard and, worse, let
# `load_redshift` fire on a Gold build that `curate` had not finished writing. Airflow 3
# calls these Assets; `airflow.datasets` no longer exists on 3.3.
# --------------------------------------------------------------------------------------

try:
    from airflow.sdk import Asset

    # One asset per producer, not one shared "operational" asset: a list schedule in
    # Airflow is AND, so `schedule=[SILVER_RDS, SILVER_DYNAMODB]` waits for both exports the
    # way the old :00/:15/:30 stagger was trying to. A single shared asset would be OR, and
    # would rebuild Gold twice per cycle off half-updated Silver.
    SILVER_RDS = Asset("s3://aimternet/silver/rds_operational")
    SILVER_DYNAMODB = Asset("s3://aimternet/silver/dynamodb_operational")
    GOLD = Asset("s3://aimternet/gold")
except Exception:  # pragma: no cover - keeps a bare import of this module working
    SILVER_RDS = SILVER_DYNAMODB = GOLD = None  # type: ignore[assignment]


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
