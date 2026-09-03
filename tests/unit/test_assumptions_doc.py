"""`docs/assumptions.md` is generated; this is what stops it going stale.

The policy register lives in `src/aimternet/config/poc_policy.py` and is rendered from
there. Committing the rendering makes it readable on GitHub without running anything; this
test makes the committed copy an assertion rather than a promise.
"""

from __future__ import annotations

from pathlib import Path

from aimternet.pipeline.cli import assumptions_markdown

DOC = Path(__file__).resolve().parents[2] / "docs" / "assumptions.md"


def test_the_committed_doc_matches_the_policy_register() -> None:
    assert DOC.exists(), "docs/assumptions.md is missing — run `make docs`"
    assert DOC.read_text() == assumptions_markdown(), (
        "docs/assumptions.md is out of date with poc_policy.py — run `make docs`"
    )


def test_every_assumption_carries_its_evidence() -> None:
    """A decision without evidence is a preference. §0.5 asks for the former."""
    from aimternet.config.poc_policy import policy

    for assumption in policy().assumptions:
        assert assumption.decision.strip(), assumption.key
        assert assumption.rationale.strip(), assumption.key
