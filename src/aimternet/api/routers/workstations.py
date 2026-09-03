"""Workstation queries (spec §7.1)."""

from __future__ import annotations

from fastapi import APIRouter, Query

from aimternet.api.schemas import AvailableWorkstations, WorkstationOut
from aimternet.db.session import fetch_all

router = APIRouter(prefix="/v1/workstations", tags=["workstations"])


@router.get("/available", response_model=AvailableWorkstations)
def available_workstations(
    zone: str | None = Query(default=None, description="Filter to one zone"),
) -> AvailableWorkstations:
    """Workstations a member can be checked in to right now.

    Availability is the workstation's own status, which check-in and check-out maintain in
    the same transaction as the rental. It is not derived by scanning rentals at read time.
    """
    sql = """
        SELECT workstation_id, zone_classification, base_hourly_rate, status
        FROM workstations
        WHERE status = 'AVAILABLE'
    """
    params: tuple[object, ...] = ()
    if zone:
        sql += " AND zone_classification = %s"
        params = (zone,)
    sql += " ORDER BY zone_classification, workstation_id"

    rows = fetch_all(sql, params or None)
    return AvailableWorkstations(
        count=len(rows), workstations=[WorkstationOut(**row) for row in rows]
    )


@router.get("", response_model=list[WorkstationOut])
def all_workstations() -> list[WorkstationOut]:
    """Every workstation with its current status — the floor view for the POS."""
    rows = fetch_all(
        """SELECT workstation_id, zone_classification, base_hourly_rate, status
           FROM workstations ORDER BY workstation_id"""
    )
    return [WorkstationOut(**row) for row in rows]
