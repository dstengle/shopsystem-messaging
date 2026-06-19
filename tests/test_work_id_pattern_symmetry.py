"""work_id pattern symmetry across every catalog message-type schema (lead-4wy).

Bug being pinned: ``Clarify.work_id`` (and ``Nudge.work_id``) carried the
pattern ``^[a-zA-Z0-9-]+$``, which REJECTS the dot, while the other vehicles
(``AssignScenarios``, ``RequestBugfix``, ``RequestMaintenance``, ``WorkDone``,
``RequestCompletionJournal``, ``RequestCompletionJournalResponse``) constrained
``work_id`` only as a bare ``str`` and accepted dotted child-bead IDs like
``lead-231.1``. The asymmetry was latent: a dispatch sent with a dotted
work_id could not be answered, because ``shop-msg respond clarify
--work-id lead-231.1`` failed Clarify validation on the dot.

These tests pin the corrected contract:

  (a) a dotted child-bead work_id like ``lead-231.1`` round-trips (construct +
      validate) through EVERY work_id-carrying message-type schema, including
      Clarify; and
  (b) a clearly-invalid work_id is REJECTED uniformly across EVERY vehicle —
      same accept/reject set on every type (symmetry).

The load-bearing requirement is SYMMETRY: the same work_id is valid on every
message type, AND dotted lead child-bead IDs are admitted — applied via a
single shared Pydantic type alias rather than per-schema copies.
"""
import pytest
from pydantic import BaseModel, ValidationError

from catalog.schemas import (
    AssignScenarios,
    Clarify,
    MechanismObservation,
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
    minimum so the ONLY thing under test is the work_id constraint.
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


# Every catalog message-type schema that carries a work_id. The symmetry
# contract must hold across this whole set.
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


@pytest.mark.parametrize("model", WORK_ID_MODELS, ids=lambda m: m.__name__)
def test_dotted_child_bead_work_id_accepted_by_every_vehicle(model) -> None:
    # (a) A dotted lead child-bead id round-trips through every vehicle,
    # including Clarify (which previously rejected the dot).
    instance = _construct(model, "lead-231.1")
    assert instance.work_id == "lead-231.1"


@pytest.mark.parametrize("model", WORK_ID_MODELS, ids=lambda m: m.__name__)
def test_plain_hyphenated_work_id_accepted_by_every_vehicle(model) -> None:
    # The pre-existing valid shape (no dot) must keep round-tripping.
    instance = _construct(model, "lead-4wy")
    assert instance.work_id == "lead-4wy"


@pytest.mark.parametrize("model", WORK_ID_MODELS, ids=lambda m: m.__name__)
@pytest.mark.parametrize(
    "bad", ["../escape", "", "foo/bar", "a b", ".hidden"],
    ids=["path-escape", "empty", "slash", "space", "leading-dot"],
)
def test_invalid_work_id_rejected_uniformly_by_every_vehicle(model, bad) -> None:
    # (b) A clearly-invalid work_id is rejected by EVERY vehicle — same
    # reject set on every type. This is the symmetry half: no vehicle may
    # silently accept a path separator, whitespace, an empty string, or a
    # leading dot that the others reject.
    with pytest.raises(ValidationError):
        _construct(model, bad)
