"""Explicit cross-type work_id accept/reject AGREEMENT battery (lead-qbbk).

Companion to ``test_work_id_pattern_symmetry.py``. Where that file pins, per
vehicle, that a dotted child-bead id round-trips and an invalid id is rejected,
THIS file pins the symmetry property *directly*: for each work_id string in a
named battery, the accept/reject verdict is IDENTICAL across all eight
work_id-carrying message-type schemas. No message type may accept a work_id
that another rejects, and vice versa.

The load-bearing assertion is AGREEMENT, computed by collecting the verdict of
every vehicle for a given string and asserting the verdict set has size one. A
schema in the historical asymmetric pre-state — where ``Clarify``/``Nudge``
carried ``^[a-zA-Z0-9-]+$`` (dot REJECTED) while the other six left work_id a
bare ``str`` (dot ACCEPTED) — splits the verdict for a dotted/underscored id
like ``a.b.c`` or ``x_y`` into {accept, reject}, so this test would RED on it.
The current shared ``_WORK_ID_PATTERN`` collapses every vehicle to one verdict.

Battery (from the lead-qbbk bugfix acceptance signal):
  valid:   lead-qbbk, a.b.c, x_y, shopsystem-messaging-2gk
  invalid: '' (empty), a/b (slash), '..' (path-escape), 'a b' (space),
           .hidden (leading punctuation)
"""
import pytest
from pydantic import BaseModel, ValidationError

from catalog.schemas import (
    AssignScenarios,
    Clarify,
    Nudge,
    RequestBugfix,
    RequestCompletionJournal,
    RequestCompletionJournalResponse,
    RequestMaintenance,
    WorkDone,
)


def _construct(model: type[BaseModel], work_id: str) -> BaseModel:
    """Build a minimal-valid instance of ``model`` carrying ``work_id``.

    Each vehicle has different required companion fields; we supply the
    minimum so the ONLY field under test is work_id.
    """
    if model is AssignScenarios:
        return AssignScenarios(
            message_type="assign_scenarios", work_id=work_id, scenarios=[]
        )
    if model is RequestBugfix:
        return RequestBugfix(
            message_type="request_bugfix", work_id=work_id, description="d"
        )
    if model is RequestMaintenance:
        return RequestMaintenance(
            message_type="request_maintenance", work_id=work_id, description="d"
        )
    if model is Clarify:
        return Clarify(message_type="clarify", work_id=work_id, question="q?")
    if model is WorkDone:
        return WorkDone(
            message_type="work_done", work_id=work_id, status="complete"
        )
    if model is Nudge:
        return Nudge(
            message_type="nudge", reason="status-check", work_id=work_id
        )
    if model is RequestCompletionJournal:
        return RequestCompletionJournal(
            message_type="request_completion_journal",
            work_id=work_id,
            target_bc="shopsystem-messaging",
        )
    if model is RequestCompletionJournalResponse:
        return RequestCompletionJournalResponse(
            message_type="request_completion_journal_response",
            work_id=work_id,
        )
    raise AssertionError(f"unhandled model {model!r}")


# Every catalog message-type schema that carries a work_id.
WORK_ID_MODELS = [
    AssignScenarios,
    RequestBugfix,
    RequestMaintenance,
    Clarify,
    WorkDone,
    Nudge,
    RequestCompletionJournal,
    RequestCompletionJournalResponse,
]

# The lead-qbbk acceptance battery: work_id strings whose verdict must be the
# SAME on every vehicle. The expected verdict is recorded only to make the
# assertion message legible; the test's real claim is cross-type AGREEMENT.
VALID_WORK_IDS = ["lead-qbbk", "a.b.c", "x_y", "shopsystem-messaging-2gk"]
INVALID_WORK_IDS = ["", "a/b", "..", "a b", ".hidden"]


def _accepts(model: type[BaseModel], work_id: str) -> bool:
    """True iff ``model`` accepts ``work_id``, False iff it rejects it."""
    try:
        _construct(model, work_id)
        return True
    except ValidationError:
        return False


@pytest.mark.parametrize("work_id", VALID_WORK_IDS + INVALID_WORK_IDS)
def test_every_vehicle_agrees_on_work_id_verdict(work_id) -> None:
    # Collect the accept/reject verdict of EVERY work_id-carrying vehicle for
    # this one string. The symmetry contract is satisfied iff the set of
    # verdicts has exactly one element — i.e. every vehicle agrees.
    verdicts = {model: _accepts(model, work_id) for model in WORK_ID_MODELS}
    distinct = set(verdicts.values())
    assert len(distinct) == 1, (
        f"work_id {work_id!r} split the vehicles: "
        + ", ".join(
            f"{m.__name__}={'accept' if v else 'reject'}"
            for m, v in verdicts.items()
        )
    )


@pytest.mark.parametrize("work_id", VALID_WORK_IDS)
def test_battery_valid_work_ids_accepted_everywhere(work_id) -> None:
    # The valid half of the battery must be ACCEPTED by every vehicle.
    for model in WORK_ID_MODELS:
        assert _accepts(model, work_id), (
            f"{model.__name__} rejected valid work_id {work_id!r}"
        )


@pytest.mark.parametrize("work_id", INVALID_WORK_IDS)
def test_battery_invalid_work_ids_rejected_everywhere(work_id) -> None:
    # The invalid half of the battery must be REJECTED by every vehicle —
    # preserving the lead-008 path-safety hardening uniformly.
    for model in WORK_ID_MODELS:
        assert not _accepts(model, work_id), (
            f"{model.__name__} accepted invalid work_id {work_id!r}"
        )
