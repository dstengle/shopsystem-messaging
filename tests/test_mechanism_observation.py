"""Schema-level tests for MechanismObservation.

Per design slice A and prototype 1 finding 4 (input safety belongs in
the schema): every construction site (CLI, hand-rolled tests, future
automation) must hit the same validation. The CLI is not the gate.
"""
import pytest
from pydantic import ValidationError

from catalog.schemas import MechanismObservation


def test_mechanism_observation_minimal_fields_accepted() -> None:
    obs = MechanismObservation(
        message_type="mechanism_observation",
        bd_ref="ddd-product-system-abc",
        subject="bc-implementer template lacks under-asking discriminator",
        body=(
            "While doing lead-022 the template language did not give me a "
            "clear discriminator for whether to clarify or proceed; I had "
            "to fall back on heuristics that another implementer might "
            "interpret differently. Load-bearing because the next BC "
            "running this template will hit the same ambiguity."
        ),
    )
    assert obs.bd_ref == "ddd-product-system-abc"
    assert obs.observed_during is None
    assert obs.evidence is None
    assert obs.proposed_action is None


def test_bd_ref_with_path_separator_is_rejected() -> None:
    with pytest.raises(ValidationError) as excinfo:
        MechanismObservation(
            message_type="mechanism_observation",
            bd_ref="ddd/../etc/passwd",
            subject="anything",
            body="x" * 50,
        )
    assert "bd_ref" in str(excinfo.value)


def test_bd_ref_empty_is_rejected() -> None:
    with pytest.raises(ValidationError):
        MechanismObservation(
            message_type="mechanism_observation",
            bd_ref="",
            subject="anything",
            body="x" * 50,
        )


def test_bd_ref_must_have_suffix() -> None:
    # Regex requires at least one hyphen separating prefix from suffix.
    with pytest.raises(ValidationError):
        MechanismObservation(
            message_type="mechanism_observation",
            bd_ref="noseparator",
            subject="anything",
            body="x" * 50,
        )


def test_bd_ref_with_leading_hyphen_is_rejected() -> None:
    # Real beads issue ids always begin with the repo prefix's first
    # alphanumeric character. A leading hyphen is invalid by the stated
    # shape; pin the rejection so a future regex relaxation surfaces here.
    with pytest.raises(ValidationError):
        MechanismObservation(
            message_type="mechanism_observation",
            bd_ref="-leading-hyphen",
            subject="anything",
            body="x" * 50,
        )


def test_subject_too_short_is_rejected() -> None:
    with pytest.raises(ValidationError):
        MechanismObservation(
            message_type="mechanism_observation",
            bd_ref="ddd-product-system-abc",
            subject="hi",  # min length is 5
            body="x" * 50,
        )


def test_subject_too_long_is_rejected() -> None:
    with pytest.raises(ValidationError):
        MechanismObservation(
            message_type="mechanism_observation",
            bd_ref="ddd-product-system-abc",
            subject="x" * 121,  # max length is 120
            body="x" * 50,
        )


def test_body_too_short_is_rejected() -> None:
    # Minimum 50 chars prevents stub observations that carry no
    # explanation of what was observed or why it's load-bearing.
    with pytest.raises(ValidationError):
        MechanismObservation(
            message_type="mechanism_observation",
            bd_ref="ddd-product-system-abc",
            subject="anything",
            body="x" * 49,
        )


def test_optional_fields_round_trip() -> None:
    obs = MechanismObservation(
        message_type="mechanism_observation",
        bd_ref="ddd-product-system-abc",
        subject="anything",
        observed_during="lead-022",
        body="x" * 50,
        evidence=[
            "shop-templates/src/shop_templates/templates/bc-implementer.md:42",
            "catalog/src/catalog/schemas.py:155",
        ],
        proposed_action="Tighten the bc-implementer anti-rationalization section.",
    )
    assert obs.observed_during == "lead-022"
    assert len(obs.evidence) == 2
    assert obs.proposed_action.startswith("Tighten")


def test_evidence_must_be_non_empty_when_present() -> None:
    # Distinguish "no evidence" (None — field omitted) from "empty
    # evidence list" (presence-without-content; reject).
    with pytest.raises(ValidationError):
        MechanismObservation(
            message_type="mechanism_observation",
            bd_ref="ddd-product-system-abc",
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
            bd_ref="ddd-product-system-abc",
            subject="anything",
            body="x" * 50,
            evidence=[""],
        )
