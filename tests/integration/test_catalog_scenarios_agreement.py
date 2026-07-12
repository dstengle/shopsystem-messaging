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

from catalog.schemas import ScenarioPayload, _canonical_scenario_hash


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

# --- Tag/Feature-wrapped bodies (ADR-019 D1/D2, scenario 117; ADR-060) ---
#
# The canonicalization rule is SCENARIO-BLOCK-ONLY: the Scenario keyword
# line through its steps, with NO surrounding @-tag lines and NO `Feature:`
# header line. The prior samples (S4/S6) are PURE scenario blocks, so
# block-only == whole-text for them — which masked a latent defect: the
# catalog canonicalized WHOLE-TEXT (retaining a standalone @bc:/@origin:
# line and any Feature: line), agreeing with the old whole-text CLI only by
# coincidence. The bodies below carry a standalone @bc: line and/or a
# Feature: line, so block-only and whole-text DIVERGE for them, exercising
# the case the pure-block samples could not. All three forms below hash to
# the SAME block-only value as S4_BODY:
_S4_BLOCK_ONLY_HASH = "3f123ba774758ff2"

# S4 with a standalone @bc: tag line (the shape ScenarioPayload requires,
# and the shape real feature files carry).
S4_BC_TAG_WRAPPED = "@bc:shopsystem-messaging\n" + S4_BODY

# S4 wrapped in @bc:/@origin: tags, a Feature: header, and a @scenario_hash:
# tag — mirroring an on-disk feature file (e.g. send_assign_scenarios.feature).
S4_FEATURE_WRAPPED = (
    "@bc:shopsystem-messaging @origin:brief-001\n"
    "Feature: Temperature conversion\n"
    "\n"
    "  @scenario_hash:" + _S4_BLOCK_ONLY_HASH + "\n"
    + "\n".join("  " + line for line in S4_BODY.splitlines())
)


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
        pytest.param(S4_BC_TAG_WRAPPED, id="S4_with_standalone_bc_tag_line"),
        pytest.param(S4_FEATURE_WRAPPED, id="S4_feature_and_tag_wrapped"),
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


@pytest.mark.parametrize(
    "wrapped_body",
    [
        pytest.param(S4_BC_TAG_WRAPPED, id="S4_with_standalone_bc_tag_line"),
        pytest.param(S4_FEATURE_WRAPPED, id="S4_feature_and_tag_wrapped"),
    ],
)
def test_catalog_canonicalizes_wrapped_body_block_only(wrapped_body: str) -> None:
    """A body carrying a standalone @bc: line and/or a Feature: header must
    canonicalize to the SCENARIO-BLOCK-ONLY hash (ADR-019 D1/D2, scenario
    117) — the surrounding tag/Feature lines do NOT participate. Under the
    old whole-text catalog this FAILS because those lines perturbed the
    hash away from the block-only value."""
    assert _canonical_scenario_hash(wrapped_body) == _S4_BLOCK_ONLY_HASH


def test_scenario_payload_validates_with_block_only_pin_for_wrapped_body() -> None:
    """A ScenarioPayload whose gherkin carries the standalone @bc: tag line
    the schema requires must validate when its hash is the on-disk
    SCENARIO-BLOCK-ONLY pin. Under the old whole-text catalog the
    ScenarioPayload.hash validator recomputed a whole-text hash (which
    retained the @bc: line) and RAISED, refusing the block-only pin — the
    exact on-wire divergence ADR-060 brings into conformance."""
    payload = ScenarioPayload(
        hash=_S4_BLOCK_ONLY_HASH,
        tags=["@bc:shopsystem-messaging"],
        gherkin=S4_BC_TAG_WRAPPED,
    )
    assert payload.hash == _S4_BLOCK_ONLY_HASH
