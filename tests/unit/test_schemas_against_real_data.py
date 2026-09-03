"""Every schema must validate real rows from the real files.

This is the Phase 2 acceptance criterion. A schema that only validates a handcrafted
fixture proves nothing about 4.2 GB of data it has never seen, so these tests read the
actual landing tree and refuse to be satisfied by a sample of zero.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from aimternet.io.readers import discover, read_csv_rows, read_json_array
from aimternet.schemas.source import DATASET_MODELS

SAMPLE_ROWS = 200


@pytest.fixture(scope="module")
def sources(raw_landing: Path) -> dict[str, list]:
    grouped: dict[str, list] = {}
    for source in discover(raw_landing):
        grouped.setdefault(source.dataset, []).append(source)
    return grouped


@pytest.mark.parametrize("dataset", sorted(DATASET_MODELS))
def test_schema_validates_real_rows(dataset: str, sources: dict[str, list]) -> None:
    model = DATASET_MODELS[dataset]
    files = sources[dataset]
    assert files, f"no source files discovered for {dataset}"

    # First and last file: catches drift that only shows up late in the window.
    checked = 0
    for source in {files[0], files[-1]}:
        if source.source_type == "csv":
            rows = list(read_csv_rows(source.path))[:SAMPLE_ROWS]
        else:
            rows = read_json_array(source.path)[:SAMPLE_ROWS]
        for row in rows:
            try:
                model.model_validate(row)
            except ValidationError as exc:
                pytest.fail(f"{dataset}: {source.path.name} row rejected: {exc}")
            checked += 1
    assert checked > 0, f"{dataset}: validated zero rows, which proves nothing"


def test_every_dataset_has_a_model(sources: dict[str, list]) -> None:
    assert set(sources) == set(DATASET_MODELS)


def test_timestamps_normalise_to_utc_and_keep_their_offset(sources: dict[str, list]) -> None:
    """§4: store UTC internally, keep the original offset as lineage."""
    from aimternet.schemas.source import RentalTransaction

    row = next(iter(read_csv_rows(sources["rental_transactions"][0].path)))
    rental = RentalTransaction.model_validate(row)
    assert rental.source_tz_offset == "+08:00"
    assert rental.session_start_utc.utcoffset().total_seconds() == 0  # type: ignore[union-attr]
    # +08:00 means UTC is eight hours earlier, not a relabelled wall clock.
    assert rental.session_start_utc.hour == (rental.session_start.hour - 8) % 24


def test_alert_events_are_modelled_permissively(sources: dict[str, list]) -> None:
    """Alert events parse, and the schema tolerates a missing session or member.

    Spec §4 states that HARDWARE_ALERT and PERIPHERAL_ALERT have session_id/member_id unset.
    That is not true of this dataset: all 1,644 alert events across the 62 batches carry both.
    The schema still declares them optional -- the stated contract permits null, tolerating it
    costs nothing, and a loader that crashes on a null the contract allows is a worse outcome
    than one that accepts a value the contract did not promise. Reported as finding F4.
    """
    from aimternet.schemas.source import WorkstationEvent

    seen_alert = False
    for source in sources["workstation_events"][:5]:
        for record in read_json_array(source.path):
            event = WorkstationEvent.model_validate(record)
            seen_alert = seen_alert or event.event_type.endswith("_ALERT")
    assert seen_alert, "expected at least one alert event in the first five batches"

    # The permissive part of the contract, exercised directly.
    bare = WorkstationEvent.model_validate(
        {
            "event_id": "EVT-TEST-00000",
            "workstation_id": "PC-001",
            "event_timestamp": "2026-07-01T00:30:00+08:00",
            "event_type": "HARDWARE_ALERT",
            "client_os_version": "Win11-Pro-AIM-Build104",
        }
    )
    assert bare.session_id is None
    assert bare.member_id is None
