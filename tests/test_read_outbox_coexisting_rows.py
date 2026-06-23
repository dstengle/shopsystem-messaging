"""`shop-msg read outbox` must surface ALL coexisting outbox rows (work_id: lead-0lml).

BUG (mechanism / CLI observability): the outbox is keyed by
``(bc, work_id, direction, message_type)``, so multiple rows legitimately
coexist under ONE work_id (e.g. a ``work_done`` AND a later
``mechanism_observation``). The write guard correctly keys on that full tuple
(``respond <type>`` refuses to overwrite a same-``(work_id, message_type)`` row
without ``--force``), but ``read outbox --work-id`` only ever surfaced the most
recent row (``rows[-1]``) — so an intact earlier ``work_done`` LOOKED gone.

A router reading outbox state via ``read outbox`` could wrongly conclude a
response was overwritten and dispatch an unnecessary "restore" repair; a
``--force`` re-emit would then REPLACE a genuine intact ``work_done`` with a
reconstruction — net data degradation.

Fix (additive / observability-ergonomics, mechanism option (a)):
  * with no ``--message-type``, ``read outbox`` lists EVERY coexisting
    ``(work_id, message_type)`` row under the work_id; and
  * an optional ``--message-type`` selector narrows to exactly one row.

The ``(work_id, message_type)`` storage key and the write-guard refusal are
CORRECT and are PRESERVED unchanged.
"""
from __future__ import annotations

import argparse
import uuid
from pathlib import Path

import pytest

from shop_msg import cli, storage
from shop_msg.storage import registry_add, resolve_shop_name


def _register_bc(tmp_path: Path) -> tuple[str, str]:
    """Register a throwaway BC; return (canonical_name, abstract_address)."""
    name = f"test-bc-{uuid.uuid4().hex[:12]}"
    registry_add(name, shop_type="bc")
    address = resolve_shop_name(name)
    assert address is not None
    return name, address


def _seed_outbox_row(address: str, work_id: str, message_type: str, payload: dict) -> None:
    """Write one BC-outbox marker row directly, mirroring the local marker
    that ``respond`` deposits (direction='outbox')."""
    storage.insert_message(
        bc_root=address,
        work_id=work_id,
        direction="outbox",
        message_type=message_type,
        payload=payload,
    )


def _work_done_payload(work_id: str) -> dict:
    return {
        "message_type": "work_done",
        "work_id": work_id,
        "status": "complete",
        "summary": "implemented the assigned behavior and the suite is green",
        "scenario_hashes": [],
    }


def _mechanism_observation_payload() -> dict:
    return {
        "message_type": "mechanism_observation",
        "subject": "read outbox masks coexisting rows",
        "body": (
            "read outbox surfaced only the most-recent row, masking an intact "
            "earlier response under the same work_id."
        ),
    }


def _message_type_lines(out: str) -> list[str]:
    """The ``message_type:`` values rendered in the YAML output (one per row).

    Asserting against these avoids false positives where a type *name* happens
    to appear inside another row's free-text body.
    """
    return [
        line.split(":", 1)[1].strip()
        for line in out.splitlines()
        if line.startswith("message_type:")
    ]


def test_read_outbox_surfaces_all_coexisting_rows(tmp_path, capsys):
    """Two coexisting rows (work_done + mechanism_observation) under one
    (bc, work_id) must BOTH be observable via ``read outbox`` with no
    ``--message-type`` selector. Before the fix, only the most-recent
    (mechanism_observation) row was printed and the work_done LOOKED gone."""
    name, address = _register_bc(tmp_path)
    work_id = "lead-k4k7"

    # work_done first, mechanism_observation later (newer created_at).
    _seed_outbox_row(address, work_id, "work_done", _work_done_payload(work_id))
    _seed_outbox_row(
        address, work_id, "mechanism_observation", _mechanism_observation_payload()
    )

    args = argparse.Namespace(bc=name, work_id=work_id, message_type=None)
    rc = cli._cmd_read_outbox(args)
    out = capsys.readouterr().out

    assert rc == 0
    # BOTH message_types must be observable — the masked work_done is the bug.
    types = _message_type_lines(out)
    assert "work_done" in types, (
        f"the intact earlier work_done is masked; rendered types {types}; "
        f"stdout was:\n{out}"
    )
    assert "mechanism_observation" in types, (
        f"expected the mechanism_observation row too; rendered types {types}; "
        f"stdout was:\n{out}"
    )


def test_read_outbox_message_type_selector_picks_one_row(tmp_path, capsys):
    """``--message-type work_done`` narrows the output to exactly the
    work_done row even when a later mechanism_observation coexists."""
    name, address = _register_bc(tmp_path)
    work_id = "lead-k4k7"

    _seed_outbox_row(address, work_id, "work_done", _work_done_payload(work_id))
    _seed_outbox_row(
        address, work_id, "mechanism_observation", _mechanism_observation_payload()
    )

    args = argparse.Namespace(bc=name, work_id=work_id, message_type="work_done")
    rc = cli._cmd_read_outbox(args)
    out = capsys.readouterr().out

    assert rc == 0
    types = _message_type_lines(out)
    assert types == ["work_done"], (
        f"--message-type work_done must surface exactly the work_done row; "
        f"rendered types {types}; stdout was:\n{out}"
    )


def test_read_outbox_unknown_message_type_selector_errors(tmp_path, capsys):
    """A ``--message-type`` that matches no coexisting row exits non-zero
    with a descriptive stderr message (no silent empty success)."""
    name, address = _register_bc(tmp_path)
    work_id = "lead-k4k7"
    _seed_outbox_row(address, work_id, "work_done", _work_done_payload(work_id))

    args = argparse.Namespace(bc=name, work_id=work_id, message_type="clarify")
    rc = cli._cmd_read_outbox(args)
    err = capsys.readouterr().err

    assert rc == 1
    assert "clarify" in err
    assert work_id in err


def test_write_guard_still_refuses_same_type_overwrite(tmp_path):
    """Criterion 2 (regression-preserve): the write guard keyed on the full
    ``(bc, work_id, direction, message_type)`` tuple STILL refuses to overwrite
    an existing same-type outbox row. This bugfix is read-path-only; the
    correct write-guard refusal must be untouched."""
    _name, address = _register_bc(tmp_path)
    work_id = "lead-k4k7"

    _seed_outbox_row(address, work_id, "work_done", _work_done_payload(work_id))

    # Re-writing the SAME (work_id, message_type) outbox row must collide.
    with pytest.raises(storage.CollisionError):
        _seed_outbox_row(address, work_id, "work_done", _work_done_payload(work_id))


def test_write_guard_allows_distinct_message_type(tmp_path):
    """Criterion 3 (storage key unchanged): a DIFFERENT message_type under the
    same work_id is NOT a collision — coexistence is exactly the property the
    read path must now surface. Pins that the fix does not narrow the key."""
    _name, address = _register_bc(tmp_path)
    work_id = "lead-k4k7"

    _seed_outbox_row(address, work_id, "work_done", _work_done_payload(work_id))
    # A coexisting mechanism_observation must be permitted (no collision).
    _seed_outbox_row(
        address, work_id, "mechanism_observation", _mechanism_observation_payload()
    )

    rows = storage.read_outbox_messages(address, work_id)
    assert len(rows) == 2
