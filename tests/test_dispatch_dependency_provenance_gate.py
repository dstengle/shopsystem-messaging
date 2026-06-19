"""Unit tests for the dispatch dependency gate's satisfaction predicate
(lead-z15d bugfix).

The dispatch dependency gate (PDR-010 / ADR-013) walks a work_id's bd
depends-on edges and refuses a strict-mode send while any GATING predecessor
is unsatisfied. Two defects are pinned here:

  (b) PROVENANCE / informational edges over-gate. A ``discovered-from``
      (or ``related`` / ``caused-by`` / ``validates`` / ``relates-to`` /
      ``supersedes`` / ``tracks``) edge is NOT a dispatch predecessor: it
      records lineage, not sequencing. Such an edge MUST NEVER gate a
      dispatch, regardless of the predecessor's state. Before this fix the
      gate-exclusion denylist only excluded ``parent-child``, so a
      ``discovered-from:lead-l7uz`` edge over-read ``lead-l7uz`` (in_progress)
      as blocking (observed 2026-06-15: lead-9qdn + lead-7if5 refused).

  Guard: a genuine ``blocks`` edge to an in-flight predecessor dispatch STILL
  gates (the queue/promote path of PDR-010 / ADR-013 decision 4 is preserved,
  not retired).

These tests exercise :mod:`shop_msg.bd_facade` directly by stubbing the single
``_run_bd`` subprocess seam, so they pin the predicate without provisioning a
real bd workspace. The stubbed JSON shapes mirror the live ``bd dep list
--json`` / ``bd show --json`` output (``dependency_type`` per edge; ``status``
per bead).
"""
from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from shop_msg import bd_facade


def _completed(stdout: str, returncode: int = 0) -> subprocess.CompletedProcess:
    return subprocess.CompletedProcess(
        args=["bd"], returncode=returncode, stdout=stdout, stderr=""
    )


class _FakeBd:
    """Stub for ``bd_facade._run_bd`` driven by a fixed graph.

    ``dep_rows`` maps a work_id to the list of predecessor rows that
    ``bd dep list <work_id> --json`` would emit (each row carries an ``id`` and
    a ``dependency_type``). ``bead_status`` maps a work_id to the ``status``
    that ``bd show <work_id> --json`` would report.
    """

    def __init__(
        self,
        dep_rows: dict[str, list[dict]],
        bead_status: dict[str, str],
    ) -> None:
        self.dep_rows = dep_rows
        self.bead_status = bead_status

    def __call__(self, args, *, cwd, check: bool = True):
        if args[:2] == ["dep", "list"]:
            work_id = args[2]
            return _completed(json.dumps(self.dep_rows.get(work_id, [])))
        if args[:1] == ["show"]:
            work_id = args[1]
            status = self.bead_status.get(work_id)
            if status is None:
                # bd show on an unknown id exits non-zero with empty stdout.
                return _completed("", returncode=1)
            return _completed(json.dumps({"id": work_id, "status": status}))
        raise AssertionError(f"unexpected bd call: {args}")


LEAD_ROOT = Path("/tmp/fake-lead-root")


def test_discovered_from_provenance_edge_never_gates_even_when_predecessor_open(
    monkeypatch,
):
    """A ``discovered-from`` provenance edge to an OPEN predecessor must not
    gate: ``list_depends_on`` excludes it, so ``first_unclosed_predecessor``
    reports no unmet dependency (observed regression: lead-9qdn/lead-7if5
    refused on ``discovered-from:lead-l7uz`` while lead-l7uz was in_progress).
    """
    fake = _FakeBd(
        dep_rows={
            "lead-9qdn": [
                {"id": "lead-l7uz", "dependency_type": "discovered-from"},
            ],
        },
        bead_status={"lead-l7uz": "in_progress"},
    )
    monkeypatch.setattr(bd_facade, "_run_bd", fake)

    assert bd_facade.list_depends_on(LEAD_ROOT, "lead-9qdn") == []
    assert bd_facade.first_unclosed_predecessor(LEAD_ROOT, "lead-9qdn") is None


@pytest.mark.parametrize(
    "provenance_type",
    ["discovered-from", "related", "relates-to", "caused-by",
     "validates", "supersedes", "tracks"],
)
def test_every_provenance_dependency_type_is_excluded_from_gating(
    monkeypatch, provenance_type
):
    """No informational/provenance edge type is a dispatch predecessor: each is
    excluded from the gating set so it can never block a downstream dispatch.
    """
    fake = _FakeBd(
        dep_rows={
            "lead-dep": [
                {"id": "lead-prov", "dependency_type": provenance_type},
            ],
        },
        bead_status={"lead-prov": "open"},
    )
    monkeypatch.setattr(bd_facade, "_run_bd", fake)

    assert bd_facade.list_depends_on(LEAD_ROOT, "lead-dep") == []
    assert bd_facade.first_unclosed_predecessor(LEAD_ROOT, "lead-dep") is None


def test_closed_provenance_predecessor_does_not_gate(monkeypatch):
    """Bug (a) cousin: a predecessor that is a bd-CLOSED non-dispatch bead,
    reached over a provenance edge, never gates (the edge is excluded outright,
    so closure is moot)."""
    fake = _FakeBd(
        dep_rows={
            "lead-csas": [
                {"id": "lead-o0b5", "dependency_type": "discovered-from"},
            ],
        },
        bead_status={"lead-o0b5": "closed"},
    )
    monkeypatch.setattr(bd_facade, "_run_bd", fake)

    assert bd_facade.first_unclosed_predecessor(LEAD_ROOT, "lead-csas") is None


def test_genuine_blocks_edge_to_inflight_predecessor_still_gates(monkeypatch):
    """GUARD (preserve queue/promote): a real ``blocks`` edge to an in-flight
    (not-closed) predecessor STILL gates — the additive provenance correction
    must not loosen genuine dispatch-to-dispatch sequencing."""
    fake = _FakeBd(
        dep_rows={
            "lead-bbb": [
                {"id": "lead-aaa", "dependency_type": "blocks"},
            ],
        },
        bead_status={"lead-aaa": "in_progress"},
    )
    monkeypatch.setattr(bd_facade, "_run_bd", fake)

    assert bd_facade.list_depends_on(LEAD_ROOT, "lead-bbb") == ["lead-aaa"]
    unmet = bd_facade.first_unclosed_predecessor(LEAD_ROOT, "lead-bbb")
    assert unmet is not None
    assert unmet[0] == "lead-aaa"


def test_genuine_blocks_edge_to_closed_predecessor_is_satisfied(monkeypatch):
    """GUARD (cure (a) preserved): a real ``blocks`` edge to a bd-CLOSED
    predecessor is satisfied — the gate reads the predecessor bd issue STATUS,
    so a closed predecessor lets the dispatch through."""
    fake = _FakeBd(
        dep_rows={
            "lead-bbb": [
                {"id": "lead-aaa", "dependency_type": "blocks"},
            ],
        },
        bead_status={"lead-aaa": "closed"},
    )
    monkeypatch.setattr(bd_facade, "_run_bd", fake)

    assert bd_facade.list_depends_on(LEAD_ROOT, "lead-bbb") == ["lead-aaa"]
    assert bd_facade.first_unclosed_predecessor(LEAD_ROOT, "lead-bbb") is None
