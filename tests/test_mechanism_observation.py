"""Schema-level tests for MechanismObservation.

Per design slice A and prototype 1 finding 4 (input safety belongs in
the schema): every construction site (CLI, hand-rolled tests, future
automation) must hit the same validation. The CLI is not the gate.

After lead-231 item C the catalog dropped the required beads-shape
`bd_ref` field. A neutral, OPTIONAL `provenance_ref` replaces it: BCs
that want to point at a tracker-side record can, but the schema does
not assume any particular tracker. The tests below pin both the
minimal-required-fields path (no provenance_ref needed) and the
provenance_ref input-safety constraints when it IS supplied.
"""
import pytest
from pydantic import ValidationError

from catalog.schemas import MechanismObservation


def test_mechanism_observation_minimal_fields_accepted() -> None:
    # Minimal valid construction: only the required fields. No
    # provenance_ref, no observed_during, no evidence, no
    # proposed_action. This pins the lead-231 invariant from the BC
    # side: a BC that does not participate in any tracker can still
    # construct a valid mechanism_observation.
    obs = MechanismObservation(
        message_type="mechanism_observation",
        subject="bc-implementer template lacks under-asking discriminator",
        body=(
            "While doing lead-022 the template language did not give me a "
            "clear discriminator for whether to clarify or proceed; I had "
            "to fall back on heuristics that another implementer might "
            "interpret differently. Load-bearing because the next BC "
            "running this template will hit the same ambiguity."
        ),
    )
    assert obs.subject.startswith("bc-implementer")
    assert obs.provenance_ref is None
    assert obs.observed_during is None
    assert obs.evidence is None
    assert obs.proposed_action is None


def test_provenance_ref_with_path_separator_is_rejected() -> None:
    # Path-safety: the previous bd_ref pattern rejected slashes; the
    # neutral provenance_ref pattern does too (no '/' in the allowed
    # set). Pin that the same rejection still happens after the rename.
    with pytest.raises(ValidationError) as excinfo:
        MechanismObservation(
            message_type="mechanism_observation",
            subject="anything",
            body="x" * 50,
            provenance_ref="ddd/../etc/passwd",
        )
    assert "provenance_ref" in str(excinfo.value)


def test_provenance_ref_empty_is_rejected_when_supplied() -> None:
    # provenance_ref is optional (None is fine; see the minimal test),
    # but an explicit empty string is not — min_length=1 rejects it.
    # Distinguishing "absent" from "present-but-empty" prevents callers
    # from accidentally degrading the wire message.
    with pytest.raises(ValidationError):
        MechanismObservation(
            message_type="mechanism_observation",
            subject="anything",
            body="x" * 50,
            provenance_ref="",
        )


def test_provenance_ref_with_leading_hyphen_is_rejected() -> None:
    # The neutral provenance_ref pattern anchors the first character to
    # an alphanumeric, same as the prior bd_ref pattern. Pin the
    # rejection so a future regex relaxation surfaces here.
    with pytest.raises(ValidationError):
        MechanismObservation(
            message_type="mechanism_observation",
            subject="anything",
            body="x" * 50,
            provenance_ref="-leading-hyphen",
        )


def test_provenance_ref_accepts_neutral_tracker_shapes() -> None:
    # The point of decoupling is that any reasonable tracker id /
    # document name should be accepted: beads-style "prefix-suffix",
    # numeric-only github issue ids, dotted document paths, mixed
    # case, underscores. Pin a few representative shapes so a future
    # regex tightening that re-couples the field to a specific tracker
    # surfaces here.
    accepted_refs = [
        "ddd-product-system-abc",  # beads-style (still works; not required)
        "1234",                    # numeric only (e.g. GitHub Issues id)
        "PR-42",                   # mixed case + hyphen
        "design.md",               # dotted doc name
        "notes_v2",                # underscore separator
    ]
    for ref in accepted_refs:
        obs = MechanismObservation(
            message_type="mechanism_observation",
            subject="anything",
            body="x" * 50,
            provenance_ref=ref,
        )
        assert obs.provenance_ref == ref


def test_subject_too_short_is_rejected() -> None:
    with pytest.raises(ValidationError):
        MechanismObservation(
            message_type="mechanism_observation",
            subject="hi",  # min length is 5
            body="x" * 50,
        )


def test_subject_too_long_is_rejected() -> None:
    with pytest.raises(ValidationError):
        MechanismObservation(
            message_type="mechanism_observation",
            subject="x" * 121,  # max length is 120
            body="x" * 50,
        )


def test_body_too_short_is_rejected() -> None:
    # Minimum 50 chars prevents stub observations that carry no
    # explanation of what was observed or why it's load-bearing.
    with pytest.raises(ValidationError):
        MechanismObservation(
            message_type="mechanism_observation",
            subject="anything",
            body="x" * 49,
        )


def test_optional_fields_round_trip() -> None:
    obs = MechanismObservation(
        message_type="mechanism_observation",
        subject="anything",
        observed_during="lead-022",
        body="x" * 50,
        evidence=[
            "shop-templates/src/shop_templates/templates/bc-implementer.md:42",
            "catalog/src/catalog/schemas.py:155",
        ],
        proposed_action="Tighten the bc-implementer anti-rationalization section.",
        provenance_ref="ddd-product-system-abc",
    )
    assert obs.observed_during == "lead-022"
    assert len(obs.evidence) == 2
    assert obs.proposed_action.startswith("Tighten")
    assert obs.provenance_ref == "ddd-product-system-abc"


def test_evidence_must_be_non_empty_when_present() -> None:
    # Distinguish "no evidence" (None — field omitted) from "empty
    # evidence list" (presence-without-content; reject).
    with pytest.raises(ValidationError):
        MechanismObservation(
            message_type="mechanism_observation",
            subject="anything",
            body="x" * 50,
            evidence=[],
        )


def test_evidence_elements_must_be_non_empty() -> None:
    # The list constraint min_length=1 rejects []; the per-element
    # constraint rejects [''], [' ', 'real']. An evidence pointer
    # with no content is not load-bearing — surface it as a schema
    # rejection so the BC's wire message can't be silently degraded.
    with pytest.raises(ValidationError):
        MechanismObservation(
            message_type="mechanism_observation",
            subject="anything",
            body="x" * 50,
            evidence=[""],
        )


def test_no_required_field_names_a_beads_identifier() -> None:
    # Lead-231 invariant pinned at the schema level: of the required
    # fields, none names a beads identifier in its name or pattern.
    # This complements the BDD scenario by inspecting the model's
    # required-field set directly, so a regression that re-adds a
    # required beads-shape field surfaces here regardless of whether
    # the BDD suite is run.
    required_fields = {
        name for name, field in MechanismObservation.model_fields.items()
        if field.is_required()
    }
    for name in required_fields:
        assert not name.startswith("bd_"), (
            f"required field {name!r} re-introduces a bd_-prefixed name; "
            f"violates lead-231 item C decoupling invariant"
        )
        assert "beads" not in name.lower(), (
            f"required field {name!r} names beads explicitly; "
            f"violates lead-231 item C decoupling invariant"
        )
