"""Peso/number formatting helpers, mirroring the old dashboard's ``app.js`` ``peso()``/``num()``.

Pure string formatting — no imports beyond the standard library, deliberately, so this module
can never become a place a database import sneaks in.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any


def peso(value: Any) -> str:
    """PHP currency, e.g. ``peso("1234.5")`` -> ``"₱1,234.50"``."""
    try:
        amount = Decimal(str(value))
    except (InvalidOperation, TypeError):
        return "—"
    return f"₱{amount:,.2f}"


def num(value: Any) -> str:
    """A whole-number count with thousands separators, or an em dash if it isn't one."""
    try:
        return f"{int(value):,}"
    except (TypeError, ValueError):
        return "—"


def pct(value: Any, *, decimals: int = 1) -> str:
    try:
        return f"{float(value):.{decimals}f}%"
    except (TypeError, ValueError):
        return "—"
