"""Structured error responses (spec §7.1).

Every failure returns the same shape -- an RFC 7807-style problem document -- so a client can
branch on ``type`` instead of parsing prose. The correlation id is echoed back so a user
reporting "it said conflict" can be traced to the exact request in the logs.
"""

from __future__ import annotations

from typing import Any

from fastapi import Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse


class ApiProblem(Exception):
    """A failure the API understands and can explain."""

    status_code: int = status.HTTP_400_BAD_REQUEST
    problem_type: str = "about:blank"

    def __init__(self, detail: str, **context: Any) -> None:
        super().__init__(detail)
        self.detail = detail
        self.context = context

    #: RFC 7807 members a caller can always rely on. Domain context never overwrites them --
    #: a WorkstationUnavailable carrying status="OCCUPIED" would otherwise replace the HTTP
    #: status in the body and make the document unparseable by a client that trusts it.
    RESERVED = frozenset({"type", "title", "status", "detail", "instance", "correlation_id"})

    def to_response(self, request: Request) -> JSONResponse:
        body: dict[str, Any] = {
            "type": self.problem_type,
            "title": self.__class__.__name__,
            "status": self.status_code,
            "detail": self.detail,
            "instance": str(request.url.path),
            "correlation_id": getattr(request.state, "correlation_id", None),
        }
        extra = {k: v for k, v in self.context.items() if k not in self.RESERVED}
        collisions = sorted(set(self.context) & self.RESERVED)
        if collisions:
            # Keep the information rather than dropping it, but out of the reserved namespace.
            extra["context"] = {k: self.context[k] for k in collisions}
        body.update(jsonable_encoder(extra))
        return JSONResponse(status_code=self.status_code, content=body)


class MemberNotFound(ApiProblem):
    status_code = status.HTTP_404_NOT_FOUND
    problem_type = "/problems/member-not-found"


class MemberInactive(ApiProblem):
    status_code = status.HTTP_409_CONFLICT
    problem_type = "/problems/member-inactive"


class WorkstationNotFound(ApiProblem):
    status_code = status.HTTP_404_NOT_FOUND
    problem_type = "/problems/workstation-not-found"


class WorkstationUnavailable(ApiProblem):
    status_code = status.HTTP_409_CONFLICT
    problem_type = "/problems/workstation-unavailable"


class MemberAlreadyCheckedIn(ApiProblem):
    status_code = status.HTTP_409_CONFLICT
    problem_type = "/problems/member-already-checked-in"


class RentalNotFound(ApiProblem):
    status_code = status.HTTP_404_NOT_FOUND
    problem_type = "/problems/rental-not-found"


class RentalAlreadyClosed(ApiProblem):
    status_code = status.HTTP_409_CONFLICT
    problem_type = "/problems/rental-already-closed"


class ItemNotFound(ApiProblem):
    status_code = status.HTTP_404_NOT_FOUND
    problem_type = "/problems/item-not-found"


class InsufficientStock(ApiProblem):
    status_code = status.HTTP_409_CONFLICT
    problem_type = "/problems/insufficient-stock"


class InsufficientPoints(ApiProblem):
    status_code = status.HTTP_422_UNPROCESSABLE_CONTENT
    problem_type = "/problems/insufficient-points"


class PricingRejected(ApiProblem):
    status_code = status.HTTP_422_UNPROCESSABLE_CONTENT
    problem_type = "/problems/pricing-rejected"


class IdempotencyConflict(ApiProblem):
    status_code = status.HTTP_409_CONFLICT
    problem_type = "/problems/idempotency-conflict"
