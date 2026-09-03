"""POC policy — the register of decisions the source data did not make for us (spec §0.5).

When the data is silent or self-contradictory, the choice goes here rather than being
buried in a transformation. Everything in this module is reported in the reconciliation
output, so a reader can see what was assumed and change it.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Final

from aimternet.config.settings import settings


class OrphanMemberPolicy(StrEnum):
    """How to handle D2 — 840 members referenced by transactions but defined nowhere."""

    SYNTHESIZE_STUB = "synthesize_stub"
    """Insert minimal member rows, flagged as derived, so the FK-ordered load can proceed."""

    QUARANTINE = "quarantine"
    """Reject every dependent rental, purchase and ledger row instead of inventing members."""


@dataclass(frozen=True, slots=True)
class Assumption:
    """One documented decision, with the evidence that forced it."""

    key: str
    decision: str
    rationale: str
    evidence: str = ""
    spec_reference: str = ""

    def as_dict(self) -> dict[str, str]:
        return {
            "key": self.key,
            "decision": self.decision,
            "rationale": self.rationale,
            "evidence": self.evidence,
            "spec_reference": self.spec_reference,
        }


# Marker written onto members that did not come from a members.csv row.
DERIVED_SOURCE_SYSTEM: Final = "DERIVED_FROM_TRANSACTIONS"

# The opening cohort with no source row anywhere in data/raw-landing/ (D2).
ORPHAN_MEMBER_ID_RANGE: Final = ("M-1001", "M-1840")
EXPECTED_ORPHAN_MEMBER_COUNT: Final = 840


ASSUMPTIONS: Final[tuple[Assumption, ...]] = (
    Assumption(
        key="D2_ORPHAN_MEMBERS",
        decision=(
            "840 members referenced by transactions but never defined are backfilled as stub "
            "rows marked source_system='DERIVED_FROM_TRANSACTIONS', is_backfilled=true. Tier "
            "comes from the member's earliest rental; opening balance from their earliest "
            "ledger row (resulting_balance - points_delta)."
        ),
        rationale=(
            "A naive FK-ordered load would fail outright. The alternative policy, quarantine, "
            "would discard a large share of the transactional history."
        ),
        evidence=(
            "members.csv across all 62 batches defines 360 members, all M-1841+. Rentals and "
            "the ledger reference M-1001..M-1840, which appear in no source file. Verified "
            "count: exactly 840."
        ),
        spec_reference="§1.3 D2, §6.2",
    ),
    Assumption(
        key="F1_GROSS_RENTAL_AMOUNT",
        decision=(
            "gross_rental_amount = final_hourly_rate x duration_hours (post-discount), "
            "not base_hourly_rate x duration_hours as §5 states."
        ),
        rationale=(
            "The spec contradicts both the synthesizer bytecode and the data. §5 itself says a "
            "disagreement is a data-quality finding, so the finding is reported and the "
            "observed rule is implemented."
        ),
        evidence=(
            "rentals.cpython-312.pyc line 215 computes gross from final_hourly_rate. Replaying "
            "all 28,287 historical rentals through business_rules.price_rental() reproduces "
            "every field of every row exactly; the §5 formula disagrees with ~37% of them "
            "(every Silver and Gold rental)."
        ),
        spec_reference="§5",
    ),
    Assumption(
        key="F2_TELEMETRY_TTL_DISABLED",
        decision=(
            "expires_at is written verbatim as the DynamoDB TTL attribute, but TTL enforcement "
            "is off by default (AIMTERNET_DDB_TTL_ENABLED=false). "
            "AIMTERNET_DDB_TTL_SHIFT_DAYS can rebase it into the future for a live demo."
        ),
        rationale=(
            "Honours §4's instruction to wire the source attribute rather than invent one, "
            "without the POC deleting its own dataset shortly after paying to load it."
        ),
        evidence=(
            "expires_at = timestamp + 7 days and the simulated window is 2026-07-01..08-31, "
            "already past. 2026-07-01T00:00+08:00 expires 2026-07-07. 61 of 62 days would be "
            "purged within ~48h of loading."
        ),
        spec_reference="§4, §6.3, §10.6",
    ),
    Assumption(
        key="DUCKDB_REPLACES_PYARROW",
        decision="Parquet is written and read with DuckDB. pyarrow is not installed.",
        rationale=(
            "pyarrow aborts at interpreter shutdown on this host, which would fail Airflow "
            "tasks and test runs at random. fastparquet silently downcasts Decimal to float64, "
            "which breaks the no-floats-in-money rule."
        ),
        evidence=(
            "SIGABRT 'terminate called without an active exception' in 23/30 runs with the pip "
            "wheel (16.1.0 and 25.0.1) and 11/20 with conda-forge pyarrow 15 in a clean probe "
            "env. DuckDB: 0/20, writes true decimal128(12,2)."
        ),
        spec_reference="§6.4",
    ),
    Assumption(
        key="SHARED_DATABASE_NAMESPACES",
        decision=(
            "All objects live in schema aimternet_oltp (RDS) and aimternet_olap (Redshift). "
            "Terraform manages only S3 configuration and the two DynamoDB tables."
        ),
        rationale=(
            "Both instances are shared with unrelated coursework. Namespacing keeps this build "
            "from touching it, and keeping them out of Terraform means no plan can destroy them."
        ),
        evidence=(
            "RDS already holds schemas simple_oltp and public (bus tables); Redshift holds "
            "bus_ticketing, krusty_krab_olap and catalog_history."
        ),
        spec_reference="§8",
    ),
    Assumption(
        key="REDSHIFT_LOADS_VIA_INSERT",
        decision=(
            "Redshift is loaded with batched INSERT from the Gold Parquet rather than COPY "
            "FROM S3. AIMTERNET_REDSHIFT_COPY_IAM_ROLE switches COPY back on if a role is "
            "attached later; the loader probes for it at run time rather than assuming."
        ),
        rationale=(
            "COPY needs an IAM role on the cluster and there is none. Inline access keys are "
            "prohibited by §6.5, so INSERT is the only remaining path that does not weaken "
            "the credential rules."
        ),
        evidence=(
            "A COPY probe against a deliberately absent key returns 'Cannot find default IAM "
            "role on this cluster' (code 30001). The IAM user cannot call "
            "redshift:DescribeClusters to inspect or attach one."
        ),
        spec_reference="§6.5",
    ),
    Assumption(
        key="METRICS_LIVE_SOURCE",
        decision=(
            "The metrics API reads today's and live numbers from RDS, historical aggregates "
            "from Redshift, and workstation status/telemetry from DynamoDB. Configurable via "
            "AIMTERNET_METRICS_LIVE_SOURCE."
        ),
        rationale=(
            "§7.2 says Redshift + DynamoDB, but Redshift only sees a POS transaction after the "
            "next DAG run. A check-in the dashboard cannot show is not a useful dashboard."
        ),
        evidence="",
        spec_reference="§7.2",
    ),
    Assumption(
        key="PYTHON_VERSION",
        decision="Python 3.12, not the 3.11 §2 suggests.",
        rationale=(
            "Airflow 3.3.0 was already installed and passing `airflow db check` on 3.12 in this "
            "environment. Rebuilding carried risk with no benefit."
        ),
        evidence="",
        spec_reference="§2",
    ),
)


@dataclass(frozen=True, slots=True)
class PocPolicy:
    """Resolved policy for this run."""

    orphan_member_policy: OrphanMemberPolicy
    telemetry_days: int
    load_threads: int
    batch_size: int
    ddb_ttl_enabled: bool
    ddb_ttl_shift_days: int
    assumptions: tuple[Assumption, ...] = field(default=ASSUMPTIONS)

    @classmethod
    def from_settings(cls) -> PocPolicy:
        s = settings()
        return cls(
            orphan_member_policy=OrphanMemberPolicy(s.orphan_member_policy),
            telemetry_days=s.telemetry_days,
            load_threads=s.load_threads,
            batch_size=s.batch_size,
            ddb_ttl_enabled=s.ddb_ttl_enabled,
            ddb_ttl_shift_days=s.ddb_ttl_shift_days,
        )

    def as_report(self) -> dict[str, object]:
        """The block the reconciliation report embeds verbatim."""
        return {
            "orphan_member_policy": str(self.orphan_member_policy),
            "telemetry_days": self.telemetry_days,
            "load_threads": self.load_threads,
            "batch_size": self.batch_size,
            "dynamodb_ttl_enabled": self.ddb_ttl_enabled,
            "dynamodb_ttl_shift_days": self.ddb_ttl_shift_days,
            "assumptions": [a.as_dict() for a in self.assumptions],
        }


def policy() -> PocPolicy:
    return PocPolicy.from_settings()
