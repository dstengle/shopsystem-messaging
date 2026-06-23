"""Unit tests for the ClarifyResponse catalog schema (lead-ox8).

clarify_response is the lead's in-band answer to an outstanding BC clarify.
It re-opens the original dispatch on the SAME work_id. Crucially it carries
NO scenario state: it cannot change the contract or add/tighten scenarios,
which mechanically forces a scope-changing answer to route to re-dispatch
(assign_scenarios / request_bugfix) per ADR-009 layer (b) / ADR-027 — the
same no-scenario-state constraint the nudge carries (ADR-015 decision 7).
"""
import pytest
from pydantic import ValidationError


def test_clarify_response_carries_resolution_and_work_id():
    from catalog.schemas import ClarifyResponse

    msg = ClarifyResponse(
        message_type="clarify_response",
        work_id="lead-700",
        resolution="use BROKER_HOST",
    )
    assert msg.message_type == "clarify_response"
    assert msg.work_id == "lead-700"
    assert msg.resolution == "use BROKER_HOST"


def test_clarify_response_has_no_scenario_hashes_field():
    """Constructing a ClarifyResponse with a scenario_hashes field is rejected.

    The schema has NO scenario_hashes field; supplying one must raise a
    schema validation error (it must not be silently dropped). This is the
    bounding constraint: clarify_response cannot carry scenario state.
    """
    from catalog.schemas import ClarifyResponse

    with pytest.raises(ValidationError):
        ClarifyResponse(
            message_type="clarify_response",
            work_id="lead-703",
            resolution="use BROKER_HOST",
            scenario_hashes=["abc123"],
        )


def test_clarify_response_work_id_uses_shared_pattern():
    """work_id validates under the harmonized shared _WORK_ID_PATTERN.

    A dotted child-bead id round-trips; a path-separator escape is rejected.
    """
    from catalog.schemas import ClarifyResponse

    ok = ClarifyResponse(
        message_type="clarify_response",
        work_id="lead-231.1",
        resolution="answer",
    )
    assert ok.work_id == "lead-231.1"

    with pytest.raises(ValidationError):
        ClarifyResponse(
            message_type="clarify_response",
            work_id="../escape",
            resolution="answer",
        )


def test_clarify_response_is_a_lead_message():
    """clarify_response is a lead->BC vehicle: a member of the LeadMessage union."""
    from pydantic import TypeAdapter
    from catalog.schemas import LeadMessage

    adapter = TypeAdapter(LeadMessage)
    msg = adapter.validate_python(
        {
            "message_type": "clarify_response",
            "work_id": "lead-700",
            "resolution": "use BROKER_HOST",
        }
    )
    assert msg.message_type == "clarify_response"
    assert msg.resolution == "use BROKER_HOST"
