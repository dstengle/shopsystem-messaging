"""Cross-package agreement: catalog._canonical_scenario_hash ==
`scenarios hash` CLI output for the same body.

This test lives in shop-msg-bc because shop-msg-bc is the only consumer
of both contracts: catalog defines ScenarioPayload (which embeds a
hash); the scenarios package owns the canonicalization rule that
produces it. ADR-001 puts the cross-package agreement check in whichever
shop consumes both — the catalog and scenarios suites each separately
pin their own implementations against literal expected hashes.

What this test adds beyond those per-package pins: a direct comparison.
If both sides drift in lockstep — e.g., a copy-paste "fix" that touches
both canonicalization implementations and updates both sides' literal
expectations — the per-package tests would still pass. This one would
not, because it never names an expected hash; it just asserts the two
implementations agree on a set of bodies.
"""
from __future__ import annotations

import subprocess

import pytest

from catalog.schemas import _canonical_scenario_hash


# Bodies covering: happy-path canonicalization (S4), a multi-step body
# with embedded quote characters (S6), and the canonicalization rule's
# @scenario_hash-line-stripping invariant (so a payload that round-trips
# through embed-the-hash-as-a-tag stays byte-stable).
S4_BODY = """Scenario: Boiling water in Fahrenheit
    Given a temperature of 100 degrees Celsius
    When I convert it to Fahrenheit
    Then I get 212 degrees Fahrenheit"""

S6_BODY = """Scenario: Reply to lead with a clarify message
    Given an empty BC at a temporary path
    When I run shop-msg respond clarify with work-id "lead-001" and question "What about equality?"
    Then the BC's outbox contains a file named "lead-001-clarify.yaml"
    And the file parses as a valid Clarify with work_id "lead-001" and question "What about equality?\""""

S6_WITH_EMBEDDED_HASH_TAG = "@scenario_hash:b9ed9c63b8ccb208\n" + S6_BODY


def _scenarios_cli_hash(body: str) -> str:
    """Run `scenarios hash` with body on stdin, return the printed hash."""
    result = subprocess.run(
        ["scenarios", "hash"],
        input=body,
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.strip()


@pytest.mark.parametrize(
    "body",
    [
        pytest.param(S4_BODY, id="S4_boiling_water"),
        pytest.param(S6_BODY, id="S6_clarify_response_with_quotes"),
        pytest.param(S6_WITH_EMBEDDED_HASH_TAG, id="S6_with_embedded_scenario_hash_tag"),
    ],
)
def test_catalog_and_scenarios_cli_agree(body: str) -> None:
    catalog_hash = _canonical_scenario_hash(body)
    cli_hash = _scenarios_cli_hash(body)
    assert catalog_hash == cli_hash, (
        f"catalog computed {catalog_hash!r}, scenarios CLI computed "
        f"{cli_hash!r} for the same body — canonicalization rules have "
        f"diverged across the packages"
    )
