"""Stage B — Bronze: byte-identical copies of the source files in S3 (spec §6.1).

Bronze is immutable. Objects are written once and never mutated; corrections happen in Silver.
The upload is concurrent and resumable, because 1,488 telemetry files at ~1.4 MB each will
not survive a flaky connection on the first try every time.

The checksum from Stage A rides along as object metadata, so the copy can be verified against
its source later without re-reading the landing tree.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

from aimternet.config.settings import settings
from aimternet.io.s3 import S3Client, UploadOutcome
from aimternet.pipeline.manifest import ManifestEntry

log = logging.getLogger(__name__)


@dataclass
class BronzeResult:
    outcome: UploadOutcome
    versioning: tuple[bool, str]
    manifest_uri: str | None = None

    def summary(self) -> str:
        o = self.outcome
        lines = [
            f"  uploaded : {o.uploaded:,}",
            f"  skipped  : {o.skipped:,} (already present with a matching size)",
            f"  failed   : {len(o.failed):,}",
            f"  bytes    : {o.bytes_uploaded / 1e9:.2f} GB",
            f"  bucket   : versioning -> {self.versioning[1]}",
        ]
        if self.manifest_uri:
            lines.append(f"  manifest : {self.manifest_uri}")
        for key, error in o.failed[:10]:
            lines.append(f"    FAILED {key}: {error[:120]}")
        return "\n".join(lines)


def upload_bronze(
    entries: list[ManifestEntry],
    *,
    threads: int | None = None,
    skip_existing: bool = True,
) -> BronzeResult:
    """Copy every manifest entry into the Bronze layout, unchanged."""
    cfg = settings()
    client = S3Client()
    versioning = client.ensure_versioning()

    checksums = {entry.bronze_key: entry.checksum for entry in entries}
    items = [(Path(entry.source_file), entry.bronze_key) for entry in entries]

    def metadata_for(_path: Path, key: str) -> dict[str, str]:
        return {"source-sha256": checksums[key]}

    outcome = client.upload_many(
        items,
        threads=threads or cfg.load_threads,
        skip_existing=skip_existing,
        prefix_for_listing=f"{cfg.s3_bronze_prefix.strip('/')}/",
        metadata_for=metadata_for,
    )
    return BronzeResult(outcome=outcome, versioning=versioning)


def verify_bronze(entries: list[ManifestEntry], *, sample: int | None = None) -> dict[str, object]:
    """Confirm Bronze holds what the manifest says it does (acceptance item 3).

    Compares object size against the manifest and the stored ``source-sha256`` metadata
    against the manifest checksum. ``sample`` limits how many objects are head-checked when
    the full set would be slow.
    """
    cfg = settings()
    client = S3Client()
    listing = client.list_objects(f"{cfg.s3_bronze_prefix.strip('/')}/")

    missing: list[str] = []
    size_mismatch: list[str] = []
    for entry in entries:
        actual = listing.get(entry.bronze_key)
        if actual is None:
            missing.append(entry.bronze_key)
        elif actual != entry.file_size:
            size_mismatch.append(entry.bronze_key)

    checked = 0
    checksum_mismatch: list[str] = []
    for entry in entries[: sample or len(entries)]:
        if entry.bronze_key in listing:
            head = client.client.head_object(Bucket=client.bucket, Key=entry.bronze_key)
            stored = head.get("Metadata", {}).get("source-sha256")
            checked += 1
            if stored and stored != entry.checksum:
                checksum_mismatch.append(entry.bronze_key)

    return {
        "expected_objects": len(entries),
        "objects_in_bronze": len(listing),
        "missing": missing,
        "size_mismatch": size_mismatch,
        "checksum_checked": checked,
        "checksum_mismatch": checksum_mismatch,
        "verified": not missing and not size_mismatch and not checksum_mismatch,
    }
