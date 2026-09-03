"""Operational API tests against the real database (spec §9: "all endpoints tested
including the concurrency and double-check-in cases").

Functional tests run through FastAPI's TestClient, so they need no running server. The
concurrency test needs genuine parallel requests, so it talks to a live server and skips
when one is not up.

Every test cleans up after itself: an open rental left behind would block the workstation
and the member for every later test.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from decimal import Decimal

import pytest

pytestmark = pytest.mark.rds

API_URL = os.environ.get("AIMTERNET_API_BASE_URL", "http://127.0.0.1:8000")

# Members that exist in the loaded data. M-1841 is the first source-defined member.
MEMBER = "M-1841"
OTHER_MEMBER = "M-1842"


@pytest.fixture(scope="module")
def client() -> Iterator:
    from fastapi.testclient import TestClient

    from aimternet.api.main import app

    with TestClient(app, raise_server_exceptions=False) as test_client:
        yield test_client


@pytest.fixture
def free_workstation(client) -> Iterator[str]:
    """Borrow an available workstation and guarantee it is released afterwards."""
    response = client.get("/v1/workstations/available")
    assert response.status_code == 200
    workstations = response.json()["workstations"]
    assert workstations, "no available workstation to test with"
    workstation_id = workstations[0]["workstation_id"]
    yield workstation_id
    client.post("/v1/rentals/check-out", json={"workstation_id": workstation_id})


def _check_in(client, member_id: str, workstation_id: str, hours: str = "2.00", **kwargs):
    return client.post(
        "/v1/rentals/check-in",
        json={
            "member_id": member_id,
            "workstation_id": workstation_id,
            "duration_hours": hours,
        },
        **kwargs,
    )


# --------------------------------------------------------------------------- reads


def test_healthz_reports_a_real_database_round_trip(client) -> None:
    body = client.get("/healthz").json()
    assert body["status"] == "ok"
    assert body["database"] == "ok"


def test_available_workstations_are_only_the_available_ones(client) -> None:
    body = client.get("/v1/workstations/available").json()
    assert body["count"] == len(body["workstations"])
    assert all(w["status"] == "AVAILABLE" for w in body["workstations"])


def test_member_lookup_exposes_redeemable_units(client) -> None:
    """The POS should never have to know that points are spent in hundreds."""
    from aimternet.config.business_rules import rules

    body = client.get(f"/v1/members/{MEMBER}").json()
    assert body["member_id"] == MEMBER
    expected = body["current_points_balance"] // rules().redemption_unit_points
    assert body["redeemable_units"] == expected


def test_unknown_member_returns_a_problem_document(client) -> None:
    response = client.get("/v1/members/M-9999")
    assert response.status_code == 404
    body = response.json()
    assert body["title"] == "MemberNotFound"
    assert body["status"] == 404
    assert body["correlation_id"]


# --------------------------------------------------------------------------- rentals


def test_check_in_prices_from_the_shared_business_rules(client, free_workstation: str) -> None:
    from aimternet.config.business_rules import rules

    response = _check_in(client, MEMBER, free_workstation, "3.00")
    assert response.status_code == 201
    rental = response.json()

    member = client.get(f"/v1/members/{MEMBER}").json()
    expected = rules().price_rental(
        workstation_id=free_workstation,
        duration_hours=Decimal("3.00"),
        tier_name=member["current_tier"],
    )
    assert rental["base_hourly_rate"] == str(expected.base_hourly_rate)
    assert rental["final_hourly_rate"] == str(expected.final_hourly_rate)
    assert rental["is_open"] is True


def test_a_second_check_in_on_the_same_workstation_is_refused(
    client, free_workstation: str
) -> None:
    assert _check_in(client, MEMBER, free_workstation).status_code == 201
    response = _check_in(client, OTHER_MEMBER, free_workstation)
    assert response.status_code == 409
    assert response.json()["title"] == "WorkstationUnavailable"


def test_a_member_cannot_hold_two_open_rentals(client, free_workstation: str) -> None:
    assert _check_in(client, MEMBER, free_workstation).status_code == 201
    others = client.get("/v1/workstations/available").json()["workstations"]
    second = others[0]["workstation_id"]
    response = _check_in(client, MEMBER, second)
    assert response.status_code == 409
    assert response.json()["title"] == "MemberAlreadyCheckedIn"


def test_check_out_closes_the_rental_and_frees_the_workstation(
    client, free_workstation: str
) -> None:
    rental = _check_in(client, MEMBER, free_workstation, "2.00").json()

    response = client.post("/v1/rentals/check-out", json={"rental_id": rental["rental_id"]})
    assert response.status_code == 200
    closed = response.json()
    assert closed["is_open"] is False
    assert closed["net_amount_paid"] is not None

    status = {
        w["workstation_id"]: w["status"] for w in client.get("/v1/workstations").json()
    }
    assert status[free_workstation] == "AVAILABLE"


def test_checking_out_a_closed_rental_is_a_conflict_not_a_crash(
    client, free_workstation: str
) -> None:
    rental = _check_in(client, MEMBER, free_workstation).json()
    client.post("/v1/rentals/check-out", json={"rental_id": rental["rental_id"]})

    response = client.post("/v1/rentals/check-out", json={"rental_id": rental["rental_id"]})
    assert response.status_code == 409, "must be 409, never 500"
    assert response.json()["title"] == "RentalAlreadyClosed"


def test_redeeming_more_points_than_held_is_rejected(client, free_workstation: str) -> None:
    rental = _check_in(client, MEMBER, free_workstation).json()
    response = client.post(
        "/v1/rentals/check-out",
        json={"rental_id": rental["rental_id"], "points_to_redeem": 1_000_000},
    )
    assert response.status_code == 422
    assert response.json()["title"] == "InsufficientPoints"
    client.post("/v1/rentals/check-out", json={"rental_id": rental["rental_id"]})


def test_points_must_be_redeemed_in_whole_units(client, free_workstation: str) -> None:
    """§5: redemption happens in units of 100. 150 is not a valid amount to spend."""
    rental = _check_in(client, MEMBER, free_workstation).json()
    response = client.post(
        "/v1/rentals/check-out",
        json={"rental_id": rental["rental_id"], "points_to_redeem": 150},
    )
    assert response.status_code in {422}
    client.post("/v1/rentals/check-out", json={"rental_id": rental["rental_id"]})


# --------------------------------------------------------------------------- concessions


def test_purchase_prices_from_the_catalog_not_the_client(client) -> None:
    catalog = {item["item_sku"]: item for item in client.get("/v1/concessions/catalog").json()}
    sku = "SKU-BEV-01"

    response = client.post(
        "/v1/concessions/purchases",
        json={"member_id": MEMBER, "items": [{"item_sku": sku, "quantity": 2}]},
    )
    assert response.status_code == 201
    purchase = response.json()
    line = purchase["items"][0]
    assert line["unit_price"] == str(catalog[sku]["unit_retail_price"])
    assert Decimal(line["total_price"]) == Decimal(line["unit_price"]) * 2
    assert Decimal(purchase["total_amount"]) == Decimal(line["total_price"])


def test_purchase_decrements_inventory_in_the_same_transaction(client) -> None:
    sku = "SKU-BEV-03"
    before = {i["item_sku"]: i for i in client.get("/v1/concessions/catalog").json()}[sku]
    client.post(
        "/v1/concessions/purchases",
        json={"member_id": MEMBER, "items": [{"item_sku": sku, "quantity": 3}]},
    )
    after = {i["item_sku"]: i for i in client.get("/v1/concessions/catalog").json()}[sku]
    assert after["stock_quantity"] == before["stock_quantity"] - 3


def test_unknown_sku_is_rejected(client) -> None:
    response = client.post(
        "/v1/concessions/purchases",
        json={"member_id": MEMBER, "items": [{"item_sku": "SKU-XXX-99", "quantity": 1}]},
    )
    assert response.status_code == 404
    assert response.json()["title"] == "ItemNotFound"


def test_ordering_more_than_the_stock_is_rejected(client) -> None:
    catalog = {i["item_sku"]: i for i in client.get("/v1/concessions/catalog").json()}
    sku, item = min(catalog.items(), key=lambda kv: kv[1]["stock_quantity"])
    response = client.post(
        "/v1/concessions/purchases",
        json={
            "member_id": MEMBER,
            "items": [{"item_sku": sku, "quantity": item["stock_quantity"] + 1}],
        },
    )
    assert response.status_code in {409, 422}
    if response.status_code == 409:
        assert response.json()["title"] == "InsufficientStock"


# --------------------------------------------------------------------------- idempotency


def test_a_replayed_write_creates_nothing_new(client, free_workstation: str) -> None:
    key = f"test-{uuid.uuid4().hex[:12]}"
    first = _check_in(client, MEMBER, free_workstation, headers={"Idempotency-Key": key})
    assert first.status_code == 201

    second = _check_in(client, MEMBER, free_workstation, headers={"Idempotency-Key": key})
    assert second.status_code == 201
    assert second.json()["rental_id"] == first.json()["rental_id"]

    open_rentals = client.get("/v1/rentals/active").json()
    matching = [r for r in open_rentals if r["workstation_id"] == free_workstation]
    assert len(matching) == 1, "a retry must not create a second rental"


def test_reusing_a_key_for_a_different_request_is_a_conflict(
    client, free_workstation: str
) -> None:
    key = f"test-{uuid.uuid4().hex[:12]}"
    assert _check_in(
        client, MEMBER, free_workstation, headers={"Idempotency-Key": key}
    ).status_code == 201

    others = client.get("/v1/workstations/available").json()["workstations"]
    response = _check_in(
        client, OTHER_MEMBER, others[0]["workstation_id"], headers={"Idempotency-Key": key}
    )
    assert response.status_code == 409
    assert response.json()["title"] == "IdempotencyConflict"


# --------------------------------------------------------------------------- concurrency


@pytest.mark.slow
def test_simultaneous_check_ins_leave_exactly_one_winner() -> None:
    """The double-check-in case, run for real (spec §9).

    Eight requests hit one workstation at once. Read-then-write would let several through;
    the partial unique index `one_open_rental_per_workstation` is what makes exactly one
    succeed, and this is the test that demonstrates it rather than asserting it.

    Needs a live server, because TestClient serialises requests through one portal.
    """
    import concurrent.futures as cf

    requests = pytest.importorskip("requests")
    try:
        health = requests.get(f"{API_URL}/healthz", timeout=3)
        assert health.status_code == 200
    except Exception:
        pytest.skip(f"no live API at {API_URL}; start it with `make api`")

    available = requests.get(f"{API_URL}/v1/workstations/available", timeout=10).json()
    workstation_id = available["workstations"][0]["workstation_id"]
    members = [f"M-{1841 + i}" for i in range(8)]

    def attempt(member_id: str) -> int:
        return requests.post(
            f"{API_URL}/v1/rentals/check-in",
            json={
                "member_id": member_id,
                "workstation_id": workstation_id,
                "duration_hours": "1.00",
            },
            timeout=30,
        ).status_code

    try:
        with cf.ThreadPoolExecutor(max_workers=8) as pool:
            codes = list(pool.map(attempt, members))
        assert codes.count(201) == 1, f"expected exactly one winner, got {codes}"
        assert all(code == 409 for code in codes if code != 201)
    finally:
        requests.post(
            f"{API_URL}/v1/rentals/check-out",
            json={"workstation_id": workstation_id},
            timeout=30,
        )
