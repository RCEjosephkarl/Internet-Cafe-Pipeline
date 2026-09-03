"""Typed configuration (spec §8).

Nothing in this repo hardcodes a bucket, hostname, table, account id or path. Everything
comes from the environment, with two rules:

* **Paths degrade gracefully.** They fall back to repo-relative locations, so the test suite
  and local development work on a machine where ``/opt/aimternet`` does not exist.
* **Credentials fail loudly.** They have no defaults. Asking for a connection without the
  environment to back it raises :class:`MissingConfiguration` naming the exact variable,
  rather than surfacing later as an opaque connection error.
"""

from __future__ import annotations

import functools
from pathlib import Path
from typing import Literal

from pydantic import Field, SecretStr, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict

REPO_ROOT = Path(__file__).resolve().parents[3]
OrphanPolicy = Literal["synthesize_stub", "quarantine"]
MetricsSource = Literal["rds", "redshift"]


class MissingConfiguration(RuntimeError):
    """A required setting is absent. The message names the variable to set."""


def _require(value: str | SecretStr | None, env_var: str, purpose: str) -> str:
    if value is None or (isinstance(value, SecretStr) and not value.get_secret_value()):
        raise MissingConfiguration(
            f"{env_var} is not set, and it is required to {purpose}. "
            f"Copy .env.example to .env and fill it in."
        )
    return value.get_secret_value() if isinstance(value, SecretStr) else value


class Settings(BaseSettings):
    """Everything the POC reads from the environment."""

    model_config = SettingsConfigDict(
        env_prefix="AIMTERNET_",
        env_file=REPO_ROOT / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # ---------------------------------------------------------------- paths (§2)
    raw_landing: Path = REPO_ROOT / "data" / "raw-landing"
    dag_input: Path | None = None
    work_dir: Path = REPO_ROOT / "work"
    quarantine_dir: Path = REPO_ROOT / "quarantine"

    # ---------------------------------------------------------------- AWS / S3
    aws_region: str = Field(default="us-east-1", validation_alias="AWS_REGION")
    s3_bucket: str | None = None
    s3_bronze_prefix: str = "bronze"
    s3_silver_prefix: str = "silver"
    s3_gold_prefix: str = "gold"
    s3_quarantine_prefix: str = "quarantine"
    s3_manifest_prefix: str = "manifests"

    # ---------------------------------------------------------------- RDS PostgreSQL
    pg_host: str | None = None
    pg_port: int = 5432
    pg_database: str = "postgres"
    pg_user: str | None = None
    pg_password: SecretStr | None = None
    pg_schema: str = "aimternet_oltp"
    pg_connect_timeout: int = 15
    pg_ro_user: str = "aimternet_ro"
    pg_ro_password: SecretStr | None = None

    # ---------------------------------------------------------------- Redshift
    redshift_host: str | None = None
    redshift_port: int = 5439
    redshift_database: str = "dev"
    redshift_user: str | None = None
    redshift_password: SecretStr | None = None
    redshift_schema: str = "aimternet_olap"
    redshift_copy_iam_role: str = ""

    # ---------------------------------------------------------------- DynamoDB
    ddb_events_table: str = "aimternet_workstation_events"
    ddb_telemetry_table: str = "aimternet_workstation_telemetry"
    # F2: source expires_at values are already in the past. Enabling TTL would purge
    # ~61 of 62 days within ~48h of loading. See CLAUDE.md.
    ddb_ttl_enabled: bool = False
    ddb_ttl_shift_days: int = 0

    # ---------------------------------------------------------------- POC policy (§0.5)
    orphan_member_policy: OrphanPolicy = "synthesize_stub"
    telemetry_days: int = 62
    load_threads: int = 8
    batch_size: int = 1000

    # ---------------------------------------------------------------- API
    api_base_url: str = "http://127.0.0.1:8000"
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    metrics_live_source: MetricsSource = "rds"

    # ---------------------------------------------------------------- Airflow REST
    airflow_api_url: str = "http://127.0.0.1:8080"
    airflow_username: str = "admin"
    airflow_password: SecretStr | None = None

    # ---------------------------------------------------------------- derived

    @computed_field  # type: ignore[prop-decorator]
    @property
    def effective_dag_input(self) -> Path:
        """What a DAG reads. Falls back to the landing path when unset (§2)."""
        return self.dag_input if self.dag_input is not None else self.raw_landing

    def require_bucket(self) -> str:
        return _require(self.s3_bucket, "AIMTERNET_S3_BUCKET", "read or write S3")

    def postgres_dsn(self, *, read_only: bool = False) -> str:
        """libpq DSN for RDS. ``read_only`` selects the restricted role used by db_lens."""
        host = _require(self.pg_host, "AIMTERNET_PG_HOST", "connect to PostgreSQL")
        if read_only:
            user = self.pg_ro_user
            password = _require(
                self.pg_ro_password, "AIMTERNET_PG_RO_PASSWORD", "connect as the read-only role"
            )
        else:
            user = _require(self.pg_user, "AIMTERNET_PG_USER", "connect to PostgreSQL")
            password = _require(
                self.pg_password, "AIMTERNET_PG_PASSWORD", "connect to PostgreSQL"
            )
        # A bounded connect_timeout matters inside loaders: without it a stalled connect
        # blocks the thread that is draining a thread pool, and the failure is invisible.
        return (
            f"host={host} port={self.pg_port} dbname={self.pg_database} "
            f"user={user} password={password} connect_timeout={self.pg_connect_timeout}"
        )

    def redshift_credentials(self) -> dict[str, object]:
        """kwargs for ``redshift_connector.connect``."""
        return {
            "host": _require(
                self.redshift_host, "AIMTERNET_REDSHIFT_HOST", "connect to Redshift"
            ),
            "port": self.redshift_port,
            "database": self.redshift_database,
            "user": _require(
                self.redshift_user, "AIMTERNET_REDSHIFT_USER", "connect to Redshift"
            ),
            "password": _require(
                self.redshift_password, "AIMTERNET_REDSHIFT_PASSWORD", "connect to Redshift"
            ),
        }

    def s3_uri(self, prefix: str, *parts: str) -> str:
        joined = "/".join(p.strip("/") for p in parts if p)
        base = f"s3://{self.require_bucket()}/{prefix.strip(chr(47))}"
        return f"{base}/{joined}" if joined else base

    def ensure_writable_dirs(self) -> None:
        """Create the scratch and quarantine trees. Never touches the landing tree."""
        for path in (self.work_dir, self.quarantine_dir):
            path.mkdir(parents=True, exist_ok=True)


@functools.lru_cache(maxsize=1)
def settings() -> Settings:
    """The process-wide settings singleton."""
    return Settings()
