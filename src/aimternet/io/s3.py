"""S3 access for the Bronze, Silver, Gold and quarantine layers.

The design constraint is the telemetry upload: 1,488 files, 4.1 GB, on a 4-vCPU instance.
It has to be concurrent, and it has to be resumable, because a run that dies at file 1,200
must not start again from zero.

Resumability comes from the object store itself. Before uploading, the client lists what is
already in the destination prefix with its size; a source file whose object already exists at
the same size is skipped. That is cheaper and more honest than a local checkpoint file, which
can disagree with reality.
"""

from __future__ import annotations

import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config
from botocore.exceptions import ClientError

from aimternet.config.settings import settings

log = logging.getLogger(__name__)

MULTIPART_THRESHOLD = 16 * 1024 * 1024
MULTIPART_CHUNKSIZE = 16 * 1024 * 1024


@dataclass
class UploadOutcome:
    """What an upload run did. Skipped files are as interesting as uploaded ones."""

    uploaded: int = 0
    skipped: int = 0
    failed: list[tuple[str, str]] = field(default_factory=list)
    bytes_uploaded: int = 0

    @property
    def total(self) -> int:
        return self.uploaded + self.skipped + len(self.failed)


class S3Client:
    """A thin, thread-safe wrapper over boto3 for the operations this pipeline needs."""

    def __init__(self, bucket: str | None = None) -> None:
        cfg = settings()
        self.bucket = bucket or cfg.require_bucket()
        self._config = Config(
            region_name=cfg.aws_region,
            retries={"max_attempts": 10, "mode": "adaptive"},
            max_pool_connections=max(cfg.load_threads * 2, 16),
        )
        self._local = threading.local()
        self._transfer = TransferConfig(
            multipart_threshold=MULTIPART_THRESHOLD,
            multipart_chunksize=MULTIPART_CHUNKSIZE,
            max_concurrency=4,
            use_threads=True,
        )

    @property
    def client(self) -> Any:
        """One boto3 client per thread. Clients are not documented as thread-safe."""
        existing = getattr(self._local, "client", None)
        if existing is None:
            existing = boto3.client("s3", config=self._config)
            self._local.client = existing
        return existing

    # ---------------------------------------------------------------- reads

    def list_objects(self, prefix: str) -> dict[str, int]:
        """key -> size for everything under ``prefix``. One call per 1,000 objects."""
        found: dict[str, int] = {}
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                found[obj["Key"]] = int(obj["Size"])
        return found

    def object_exists(self, key: str) -> bool:
        try:
            self.client.head_object(Bucket=self.bucket, Key=key)
        except ClientError as exc:
            if exc.response["Error"]["Code"] in {"404", "NoSuchKey", "NotFound"}:
                return False
            raise
        return True

    def read_json(self, key: str) -> Any:
        import json

        body = self.client.get_object(Bucket=self.bucket, Key=key)["Body"].read()
        return json.loads(body)

    # ---------------------------------------------------------------- writes

    def upload_file(self, path: Path, key: str, metadata: dict[str, str] | None = None) -> str:
        extra: dict[str, Any] = {}
        if metadata:
            extra["Metadata"] = metadata
        self.client.upload_file(
            str(path), self.bucket, key, ExtraArgs=extra or None, Config=self._transfer
        )
        return f"s3://{self.bucket}/{key}"

    def put_bytes(self, key: str, payload: bytes, content_type: str = "application/json") -> str:
        self.client.put_object(
            Bucket=self.bucket, Key=key, Body=payload, ContentType=content_type
        )
        return f"s3://{self.bucket}/{key}"

    def upload_many(
        self,
        items: list[tuple[Path, str]],
        *,
        threads: int | None = None,
        skip_existing: bool = True,
        prefix_for_listing: str | None = None,
        metadata_for: Any = None,
        progress_every: int = 200,
    ) -> UploadOutcome:
        """Upload ``(path, key)`` pairs concurrently, skipping what is already there.

        Skipping is what makes the run resumable: an interrupted 1,488-file upload picks up
        where it stopped instead of re-sending gigabytes.
        """
        cfg = settings()
        workers = threads or cfg.load_threads
        outcome = UploadOutcome()

        existing: dict[str, int] = {}
        if skip_existing and items:
            listing_prefix = prefix_for_listing or _common_prefix([key for _, key in items])
            existing = self.list_objects(listing_prefix)
            log.info("found %d existing object(s) under %s", len(existing), listing_prefix)

        pending = []
        for path, key in items:
            size = path.stat().st_size
            if skip_existing and existing.get(key) == size:
                outcome.skipped += 1
                continue
            pending.append((path, key, size))

        if not pending:
            log.info("nothing to upload: all %d object(s) already present", outcome.skipped)
            return outcome

        log.info("uploading %d object(s) with %d threads", len(pending), workers)
        done = 0
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(
                    self.upload_file,
                    path,
                    key,
                    metadata_for(path, key) if metadata_for else None,
                ): (path, key, size)
                for path, key, size in pending
            }
            for future in as_completed(futures):
                path, key, size = futures[future]
                try:
                    future.result()
                except Exception as exc:
                    outcome.failed.append((key, str(exc)))
                    log.error("upload failed for %s: %s", key, exc)
                else:
                    outcome.uploaded += 1
                    outcome.bytes_uploaded += size
                done += 1
                if progress_every and done % progress_every == 0:
                    log.info("  %d/%d uploaded", done, len(pending))
        return outcome

    def ensure_versioning(self) -> tuple[bool, str]:
        """Turn on bucket versioning (spec §6.1 Stage B). Reports rather than assumes."""
        try:
            current = self.client.get_bucket_versioning(Bucket=self.bucket)
            if current.get("Status") == "Enabled":
                return True, "versioning already enabled"
            self.client.put_bucket_versioning(
                Bucket=self.bucket, VersioningConfiguration={"Status": "Enabled"}
            )
        except ClientError as exc:
            return False, f"could not enable versioning: {exc.response['Error']['Code']}"
        return True, "versioning enabled"


def _common_prefix(keys: list[str]) -> str:
    if not keys:
        return ""
    first, last = min(keys), max(keys)
    for index, (a, b) in enumerate(zip(first, last, strict=False)):
        if a != b:
            return first[:index]
    return first
