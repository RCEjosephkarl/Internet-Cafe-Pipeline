"""A small client for the Airflow 3.x REST API, used by ``notebooks/airflow_lens.ipynb``.

Airflow 3 replaced the old Basic-auth experimental API: you exchange credentials at
``POST /auth/token`` for a JWT and send it as a bearer token against ``/api/v2``. The token
is cached and refreshed automatically, so a notebook cell never has to think about it.

The lens observes *and* controls: pause, trigger, clear, and edit the Variables that the DAGs
read their tuning from. It deliberately cannot edit DAG source -- that belongs in git, and a
notebook that rewrites pipeline code is a way to lose work, not a control panel.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import requests

from aimternet.config.settings import settings

log = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 30


class AirflowApiError(RuntimeError):
    """The Airflow API refused a request. The message carries the status and body."""


@dataclass
class AirflowLens:
    """Observe and control the local Airflow instance."""

    base_url: str = ""
    username: str = ""
    password: str = ""
    timeout: int = DEFAULT_TIMEOUT
    _token: str | None = None

    def __post_init__(self) -> None:
        cfg = settings()
        self.base_url = (self.base_url or cfg.airflow_api_url).rstrip("/")
        self.username = self.username or cfg.airflow_username
        if not self.password:
            self.password = (
                cfg.airflow_password.get_secret_value() if cfg.airflow_password else ""
            )

    # ---------------------------------------------------------------- plumbing

    def _login(self) -> str:
        response = requests.post(
            f"{self.base_url}/auth/token",
            json={"username": self.username, "password": self.password},
            timeout=self.timeout,
        )
        if response.status_code != 201 and not response.ok:
            raise AirflowApiError(
                f"could not obtain an Airflow token ({response.status_code}). "
                f"Check AIMTERNET_AIRFLOW_USERNAME/PASSWORD. Body: {response.text[:200]}"
            )
        token = response.json().get("access_token")
        if not token:
            raise AirflowApiError(
                f"auth response contained no access_token: {response.text[:200]}"
            )
        return str(token)

    def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        if self._token is None:
            self._token = self._login()
        url = f"{self.base_url}/api/v2/{path.lstrip('/')}"
        headers = {"Authorization": f"Bearer {self._token}", "Accept": "application/json"}
        response = requests.request(method, url, headers=headers, timeout=self.timeout, **kwargs)

        if response.status_code == 401:  # token expired mid-session; retry once
            self._token = self._login()
            headers["Authorization"] = f"Bearer {self._token}"
            response = requests.request(
                method, url, headers=headers, timeout=self.timeout, **kwargs
            )

        if not response.ok:
            raise AirflowApiError(
                f"{method} {url} -> {response.status_code}: {response.text[:400]}"
            )
        if response.status_code == 204 or not response.content:
            return None
        return response.json()

    # ---------------------------------------------------------------- observe

    def health(self) -> dict[str, Any]:
        """Scheduler, triggerer and DAG-processor health."""
        response = requests.get(f"{self.base_url}/api/v2/monitor/health", timeout=self.timeout)
        return dict(response.json())

    def version(self) -> dict[str, Any]:
        response = requests.get(f"{self.base_url}/api/v2/version", timeout=self.timeout)
        return dict(response.json())

    def list_dags(self, *, only_ours: bool = True, limit: int = 200) -> list[dict[str, Any]]:
        """Every DAG.

        ``only_ours`` hides Airflow's bundled example DAGs. They ship inside site-packages
        across several provider bundles, so filtering on the file location is more reliable
        than listing bundle names -- and it keeps working when a new provider is installed.
        """
        payload = self._request("GET", f"dags?limit={limit}")
        dags = list(payload.get("dags", []))
        if only_ours:
            dags = [d for d in dags if "site-packages" not in (d.get("fileloc") or "")]
        return dags

    def get_dag(self, dag_id: str) -> dict[str, Any]:
        return dict(self._request("GET", f"dags/{dag_id}"))

    def dag_source(self, dag_id: str) -> str:
        """The DAG's source, for reading. Editing happens in git, not here."""
        dag = self.get_dag(dag_id)
        version = self._request("GET", f"dags/{dag_id}/dagVersions?limit=1")
        versions = version.get("dag_versions", [])
        if not versions:
            return f"# no parsed version available for {dag_id} ({dag.get('fileloc', '')})"
        token = versions[0].get("version_number")
        payload = self._request("GET", f"dagSources/{dag_id}?version_number={token}")
        return str(payload.get("content", ""))

    def list_runs(self, dag_id: str, limit: int = 10) -> list[dict[str, Any]]:
        payload = self._request(
            "GET", f"dags/{dag_id}/dagRuns?limit={limit}&order_by=-logical_date"
        )
        return list(payload.get("dag_runs", []))

    def list_task_instances(self, dag_id: str, run_id: str) -> list[dict[str, Any]]:
        payload = self._request("GET", f"dags/{dag_id}/dagRuns/{run_id}/taskInstances")
        return list(payload.get("task_instances", []))

    def task_log(self, dag_id: str, run_id: str, task_id: str, try_number: int = 1) -> str:
        payload = self._request(
            "GET",
            f"dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/logs/{try_number}",
            headers=None,
        )
        if isinstance(payload, dict):
            content = payload.get("content", "")
            if isinstance(content, list):
                return "\n".join(
                    str(chunk.get("event", chunk)) if isinstance(chunk, dict) else str(chunk)
                    for chunk in content
                )
            return str(content)
        return str(payload)

    # ---------------------------------------------------------------- control

    def pause(self, dag_id: str) -> dict[str, Any]:
        return dict(self._request("PATCH", f"dags/{dag_id}", json={"is_paused": True}))

    def unpause(self, dag_id: str) -> dict[str, Any]:
        return dict(self._request("PATCH", f"dags/{dag_id}", json={"is_paused": False}))

    def trigger(
        self, dag_id: str, conf: dict[str, Any] | None = None, note: str | None = None
    ) -> dict[str, Any]:
        """Kick off a run. ``conf`` reaches the DAG as ``dag_run.conf``."""
        body: dict[str, Any] = {"logical_date": None, "conf": conf or {}}
        if note:
            body["note"] = note
        return dict(self._request("POST", f"dags/{dag_id}/dagRuns", json=body))

    def clear_task(self, dag_id: str, run_id: str, task_ids: list[str]) -> Any:
        """Clear tasks so the scheduler reruns them -- the usual fix for a transient failure."""
        return self._request(
            "PATCH",
            f"dags/{dag_id}/dagRuns/{run_id}/taskInstances",
            json={"dry_run": False, "task_ids": task_ids, "include_downstream": True},
        )

    # ---------------------------------------------------------------- tuning knobs

    def get_variable(self, key: str) -> Any:
        return self._request("GET", f"variables/{key}")

    def list_variables(self) -> list[dict[str, Any]]:
        payload = self._request("GET", "variables?limit=100")
        return list(payload.get("variables", []))

    def set_variable(self, key: str, value: str, description: str = "") -> dict[str, Any]:
        """Create or update a Variable.

        This is how the lens changes pipeline behaviour without touching code: the DAGs read
        their telemetry-day count, thread count and batch size from Variables at parse time.
        """
        body = {"key": key, "value": value, "description": description}
        try:
            return dict(self._request("PATCH", f"variables/{key}", json=body))
        except AirflowApiError:
            return dict(self._request("POST", "variables", json=body))

    def list_pools(self) -> list[dict[str, Any]]:
        payload = self._request("GET", "pools?limit=100")
        return list(payload.get("pools", []))

    def set_pool_slots(self, name: str, slots: int) -> dict[str, Any]:
        """Throttle or widen concurrency for whatever runs in this pool."""
        return dict(self._request("PATCH", f"pools/{name}", json={"pool": name, "slots": slots}))
