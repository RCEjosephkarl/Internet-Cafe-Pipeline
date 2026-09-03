"""Prove the quarantine path works, by breaking data on purpose.

The real landing tree is clean: validation rejects nothing, so `s3://…/quarantine/` holds
only a summary saying zero. That is honest but it does not demonstrate that the reject path
functions. This script builds a *temporary copy* of a few batches, injects defects of each
kind, and runs the validator over it.

The landing tree is never touched — the copy lives under the work directory and the run
asserts the originals are byte-identical afterwards.

    python scripts/quarantine_demo.py        (or: make quarantine-demo)
"""

from __future__ import annotations

import csv
import hashlib
import json
import shutil
import sys
from pathlib import Path

from aimternet.config.settings import settings
from aimternet.pipeline.validation.engine import Validator
from aimternet.pipeline.validation.quarantine import publish

BATCHES = ("2026-07-01", "2026-07-02")


def _fingerprint(root: Path) -> dict[str, str]:
    return {
        str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted(root.rglob("*"))
        if p.is_file() and not any(part.startswith(".") for part in p.parts)
    }


def build_broken_copy(source: Path, destination: Path) -> list[str]:
    """Copy a slice of the landing tree and inject one defect of each kind."""
    if destination.exists():
        shutil.rmtree(destination)
    (destination / "catalog").mkdir(parents=True)
    (destination / "dimensions").mkdir(parents=True)
    (destination / "legacy_batches").mkdir(parents=True)
    (destination / "telemetry").mkdir(parents=True)

    for name in ("workstations", "concession_items"):
        shutil.copy2(source / "catalog" / f"{name}.csv", destination / "catalog")
    for name in ("dim_date", "dim_time"):
        shutil.copy2(source / "dimensions" / f"{name}.csv", destination / "dimensions")
    for batch in BATCHES:
        shutil.copytree(
            source / "legacy_batches" / batch, destination / "legacy_batches" / batch
        )
    shutil.copytree(
        source / "telemetry" / BATCHES[0], destination / "telemetry" / BATCHES[0]
    )

    injected: list[str] = []
    batch_dir = destination / "legacy_batches" / BATCHES[0]

    # 1. An unknown membership tier -> SCHEMA_INVALID
    path = batch_dir / "members.csv"
    rows = list(csv.reader(path.open(newline="", encoding="utf-8-sig")))
    rows[1][5] = "Platinum"
    with path.open("w", newline="", encoding="utf-8") as fh:
        csv.writer(fh, lineterminator="\r\n").writerows(rows)
    injected.append(f"members.csv row 1: current_tier -> 'Platinum' (unknown tier)")

    # 2. A duplicated primary key -> PK_DUPLICATE
    path = batch_dir / "member_points_ledger.csv"
    rows = list(csv.reader(path.open(newline="", encoding="utf-8-sig")))
    rows.append(list(rows[1]))
    with path.open("w", newline="", encoding="utf-8") as fh:
        csv.writer(fh, lineterminator="\r\n").writerows(rows)
    injected.append(f"member_points_ledger.csv: duplicated ledger_id {rows[1][0]}")

    # 3. A line total that disagrees with quantity x unit price -> ORDER_TOTAL_MISMATCH
    path = batch_dir / "concession_order_items.csv"
    rows = list(csv.reader(path.open(newline="", encoding="utf-8-sig")))
    rows[1][5] = "999999.00"
    with path.open("w", newline="", encoding="utf-8") as fh:
        csv.writer(fh, lineterminator="\r\n").writerows(rows)
    injected.append("concession_order_items.csv row 1: total_price -> 999999.00")

    # 4. A rental whose money contradicts the pricing rules -> PRICING_MISMATCH
    path = batch_dir / "rental_transactions.csv"
    rows = list(csv.reader(path.open(newline="", encoding="utf-8-sig")))
    rows[1][13] = "1.00"  # net_amount_paid
    with path.open("w", newline="", encoding="utf-8") as fh:
        csv.writer(fh, lineterminator="\r\n").writerows(rows)
    injected.append(f"rental_transactions.csv {rows[1][0]}: net_amount_paid -> 1.00")

    # 5. A workstation that does not exist -> FK_ORPHAN
    rows[2][2] = "PC-999"
    with path.open("w", newline="", encoding="utf-8") as fh:
        csv.writer(fh, lineterminator="\r\n").writerows(rows)
    injected.append(f"rental_transactions.csv {rows[2][0]}: workstation_id -> PC-999")

    return injected


def main() -> int:
    cfg = settings()
    source = cfg.raw_landing
    destination = Path(cfg.work_dir) / "quarantine-demo" / "raw-landing"

    print(f"source landing tree : {source}")
    before = _fingerprint(source)

    injected = build_broken_copy(source, destination)
    print(f"broken copy         : {destination}")
    print("\ninjected defects:")
    for description in injected:
        print(f"  - {description}")

    print("\nrunning validation over the broken copy…\n")
    result = Validator(destination, run_id="quarantine-demo").run()
    print(result.summary_table())

    published = publish(result)
    print(f"\nquarantine dir  : {published['local_dir']}")
    print(f"records rejected: {published['records_rejected']}")
    for uri in published["s3_uris"]:  # type: ignore[union-attr]
        print(f"published       : {uri}")

    quarantine_dir = Path(str(published["local_dir"]))
    for path in sorted(quarantine_dir.glob("*.jsonl")):
        lines = path.read_text().strip().splitlines()
        print(f"\n--- {path.name}: {len(lines)} rejected record(s) ---")
        for line in lines[:2]:
            record = json.loads(line)
            print(f"  rule   : {record['rule']}")
            print(f"  detail : {record['detail'][:110]}")
            print(f"  key    : {record['record_key']}")

    after = _fingerprint(source)
    unchanged = before == after
    print(f"\nlanding tree unchanged: {unchanged}")
    if not unchanged:
        print("  CHANGED:", sorted(set(before) ^ set(after))[:5])
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
