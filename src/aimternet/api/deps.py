"""Shared FastAPI dependencies."""

from __future__ import annotations

from typing import Annotated

from fastapi import Header


def idempotency_key(
    value: Annotated[
        str | None,
        Header(
            alias="Idempotency-Key",
            description="Send the same key when retrying a write so it happens at most once.",
        ),
    ] = None,
) -> str | None:
    return value
