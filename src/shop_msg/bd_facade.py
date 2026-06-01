"""bd integration facade for the shop-msg CLI (PDR-010 / ADR-011 / ADR-012 / ADR-016).

This module is the single place where shop-msg drives the `bd` (beads)
work-tracker. Per ADR-016 the integration logic lives in the shop-msg CLI,
NOT in agent procedure: an agent invokes one shop-msg command and the CLI
performs both the messaging action (postgres deposit / release) and the
paired bd metadata update under one atomicity boundary.

Per ADR-011 the canonical dispatch field set is carried as bd STRUCTURED
METADATA (a JSON blob queryable via `bd show <id> --json`), NOT as a
free-form "## Dispatch state" prose block in the bead's notes.

The lead dispatch lifecycle, tracked on the bd entry's ``dispatch_state``
metadata key:

    outbox_pending  -- Step 1: bd entry written (fsynced) before any postgres
                       write. Recoverable record of intent.
    dispatched      -- Step 3: postgres outbox deposit acknowledged; the bd
                       flip is GUARDED by Step 2 success.
    bc_emitted      -- the BC has emitted a response (work_done/clarify/...).
    consumed        -- the lead has consumed the BC response.

The bd database is located by running `bd` with cwd set to the lead shop's
root; bd auto-discovers the ``.beads`` workspace by walking up from cwd.
This mirrors how the real lead shop uses bd and keeps each shop's beads
scoped to its own root (important for the test suite, where each lead is a
throwaway tmp root with its own ``.beads`` workspace).
"""
from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from typing import Any


# Canonical dispatch metadata keys (ADR-011 field mapping).
KEY_DISPATCHED_TO_BC = "dispatched_to_bc"
KEY_DISPATCH_MESSAGE_TYPE = "dispatch_message_type"
KEY_DISPATCH_STATE = "dispatch_state"
KEY_SCENARIO_HASHES_PINNED = "scenario_hashes_pinned"
KEY_DEPENDS_ON_DISPATCH = "depends_on_dispatch"
KEY_PENDING_DEPENDENCY = "pending_dependency"
KEY_BC_ORIGIN_MAIN_COMMIT = "bc_origin_main_commit_at_dispatch"
# Internal bookkeeping keys (carried so the sweeper can reconstruct a lost
# postgres deposit and judge the staleness threshold).
KEY_OUTBOX_PENDING_AT = "outbox_pending_at"
KEY_PAYLOAD_REF = "payload_ref"

# Dispatch states.
STATE_OUTBOX_PENDING = "outbox_pending"
STATE_DISPATCHED = "dispatched"
STATE_BC_EMITTED = "bc_emitted"
STATE_CONSUMED = "consumed"
# Terminal dispatch state (PDR-010 / ADR-013): the architect's close-step.
# A predecessor must be at STATE_CLOSED for a dependent's send to proceed in
# strict mode (and for the promote scan to release a queued dependent).
STATE_CLOSED = "closed"

# Default sweep staleness threshold in seconds.
DEFAULT_SWEEP_THRESHOLD_SECONDS = 60


# ---------------------------------------------------------------------------
# BC-side bead creation on inbox observation (PDR-010 / ADR-017 / lead-sn1e).
#
# Per ADR-017's 2026-05-29 revision + ADR-016 decision 2, the shop-msg CLI
# ITSELF creates a paired bead in the BC's OWN bd workspace as a side effect
# of the BC observing an inbox row (`shop-msg pending inbox --bc <name>`), and
# flips that bead's status as a side effect of the BC's `shop-msg respond`
# emissions. The agent never runs `bd create` / `bd update` by hand.
#
# Cross-shop contract is LOOSE (PDR-010 decision 4 / ADR-017 decisions 3 & 6):
# the BC bead lives in the BC's local namespace (e.g. shopsystem-messaging-<nanoid>),
# is keyed on the BC's own id (never on the lead work_id), and the ONLY link
# back to the lead is the note line "Lead work_id: <work_id>". The lead never
# learns the BC bead id and never pulls BC bd state.
#
# The cross-reference is carried two ways on the BC bead:
#   * a human-readable NOTES line "Lead work_id: <work_id>" (the contract the
#     scenarios pin verbatim), AND
#   * a metadata key ``lead_work_id=<work_id>`` so idempotency lookup is a
#     deterministic ``bd list --metadata-field lead_work_id=<work_id>`` query
#     (the notes field is not server-side queryable; metadata is).
# ---------------------------------------------------------------------------

# Metadata key carrying the lead work_id cross-reference on the BC bead.
KEY_LEAD_WORK_ID = "lead_work_id"

# Cross-reference note prefix (the verbatim line the scenarios pin).
LEAD_WORK_ID_NOTE_PREFIX = "Lead work_id: "

# ADR-017 decision 2 message_type -> BC bead type mapping. request_bugfix is a
# "bug"; assign_scenarios is a "feature"; the flat / nudge kinds are "task".
MESSAGE_TYPE_TO_BEAD_TYPE = {
    "request_bugfix": "bug",
    "assign_scenarios": "feature",
    "request_maintenance": "task",
    "nudge": "task",
}
DEFAULT_BEAD_TYPE = "task"

# ADR-017 decision 4 response-message_type -> BC bead status transition.
# work_done depends on status, so it is handled by ``status_for_response``.
RESPONSE_TO_STATUS = {
    "clarify": "blocked",
    # mechanism_observation -> unchanged + note (handled specially: None means
    # "do not flip status, append a note instead").
    "mechanism_observation": None,
}

# bd title length ceiling; titles are truncated to fit (ADR-017 decision 2
# allows a truncated form when bd's constraints apply).
BC_BEAD_TITLE_MAX = 200


def cross_reference_note(work_id: str) -> str:
    """Return the verbatim cross-reference note line for ``work_id``."""
    return f"{LEAD_WORK_ID_NOTE_PREFIX}{work_id}"


def status_for_response(message_type: str, status: str | None = None) -> str | None:
    """Return the BC bead status for a BC ``shop-msg respond`` emission.

    Returns None when the response should NOT flip the bead's status
    (mechanism_observation -> unchanged + note). work_done maps on its
    ``status`` field: complete -> closed; blocked/partial -> blocked.
    """
    if message_type == "work_done":
        return "closed" if status == "complete" else "blocked"
    return RESPONSE_TO_STATUS.get(message_type, None)


def find_bc_bead_id(bc_root: Path, work_id: str) -> str | None:
    """Return the id of the BC bead cross-referencing ``work_id``, or None.

    Idempotency primitive: a BC bead "already exists for this work_id" iff a
    bead in the BC workspace carries the ``lead_work_id=<work_id>`` metadata
    key. Uses ``bd list --metadata-field lead_work_id=<work_id> --all`` so
    closed beads are matched too (a re-observation after the work closed must
    still not create a duplicate).
    """
    proc = _run_bd(
        [
            "list",
            "--metadata-field",
            f"{KEY_LEAD_WORK_ID}={work_id}",
            "--all",
            "--json",
        ],
        cwd=bc_root,
        check=False,
    )
    if proc.returncode != 0 or not proc.stdout.strip():
        return None
    try:
        data = json.loads(proc.stdout)
    except json.JSONDecodeError:
        return None
    rows = data if isinstance(data, list) else data.get("issues", [])
    for row in rows:
        if isinstance(row, dict) and row.get("id"):
            return row["id"]
    return None


def create_bc_bead_on_observation(
    bc_root: Path,
    work_id: str,
    *,
    message_type: str,
    title: str,
) -> str | None:
    """Create a paired BC-side bead for ``work_id`` if none exists yet.

    Idempotent: returns the existing bead id (creating nothing) when a bead
    already cross-references ``work_id``; otherwise creates one in the BC's
    own bd workspace, carrying:
      * type derived from ``message_type`` (ADR-017 decision 2 mapping),
      * the note line "Lead work_id: <work_id>",
      * the metadata key ``lead_work_id=<work_id>`` for deterministic lookup,
    and returns the new bead's local id.

    Best-effort: returns None when bd is unavailable in this environment so a
    bd-less invocation still performs the messaging-layer observation.
    """
    if not bd_available(bc_root):
        return None
    existing = find_bc_bead_id(bc_root, work_id)
    if existing is not None:
        return existing
    bead_type = MESSAGE_TYPE_TO_BEAD_TYPE.get(message_type, DEFAULT_BEAD_TYPE)
    clean_title = (title or f"BC work for {work_id}").strip()
    if len(clean_title) > BC_BEAD_TITLE_MAX:
        clean_title = clean_title[:BC_BEAD_TITLE_MAX]
    proc = _run_bd(
        [
            "create",
            clean_title,
            "--type",
            bead_type,
            "--notes",
            cross_reference_note(work_id),
            "--metadata",
            json.dumps({KEY_LEAD_WORK_ID: work_id}),
            "--force",
            "--json",
        ],
        cwd=bc_root,
    )
    try:
        rec = json.loads(proc.stdout)
    except json.JSONDecodeError:
        return find_bc_bead_id(bc_root, work_id)
    if isinstance(rec, list):
        rec = rec[0] if rec else {}
    new_id = rec.get("id") if isinstance(rec, dict) else None
    _fsync_workspace(bc_root)
    return new_id or find_bc_bead_id(bc_root, work_id)


def apply_response_side_effect(
    bc_root: Path,
    work_id: str,
    *,
    message_type: str,
    status: str | None = None,
    note: str | None = None,
) -> None:
    """Flip the BC bead's status (and/or append a note) for a BC response.

    ADR-017 decision 4 status-transition contract, realized mechanically:
      clarify                -> blocked    (+ note summarizing the question)
      work_done(complete)    -> closed
      work_done(blocked)     -> blocked
      mechanism_observation  -> unchanged  (+ note recording the observation)

    Best-effort and a no-op when bd is unavailable or no paired BC bead
    exists for ``work_id`` (e.g. a respond whose inbox row was never observed
    via ``pending inbox``). Never raises into the respond path: a bd hiccup
    must not block the primary messaging emission.
    """
    if not bd_available(bc_root):
        return
    bead_id = find_bc_bead_id(bc_root, work_id)
    if bead_id is None:
        return
    new_status = status_for_response(message_type, status)
    args = ["update", bead_id]
    if new_status is not None:
        args += ["--status", new_status]
    if note:
        args += ["--append-notes", note]
    if len(args) == 2:
        # Nothing to change (mechanism_observation with no note text).
        return
    try:
        _run_bd(args, cwd=bc_root)
        _fsync_workspace(bc_root)
    except BdFacadeError:
        # Messaging emission already succeeded; surface nothing fatal here.
        # (The CLI layer logs a warning; see _cmd_respond_*.)
        raise


def nudge_note_text(reason: str, work_id: str, at_iso8601_utc: str) -> str:
    """Return the canonical nudge note string (ADR-015 / lead-xp5f decision 2).

    The EXACT format pinned by the scenarios is::

        nudge: reason=<reason> work_id=<work_id> at=<iso8601_utc>

    A single mechanism, a single format. The three structured fields
    (reason, work_id, at) are substring-parsable, which is what the scenario
    Then-steps assert. ``work_id`` is rendered verbatim; a bare nudge with no
    work_id passes the empty string here, but the nudge scenarios always
    carry a work_id when they assert the note.
    """
    return f"nudge: reason={reason} work_id={work_id} at={at_iso8601_utc}"


def append_note(work_id: str, text: str, *, root: Path) -> None:
    """Append ``text`` to bead ``work_id``'s notes field via ``bd note``.

    Wraps bd's native ``bd note <id> <text>`` command (shorthand for
    ``bd update <id> --append-notes``), which appends to the bead's notes
    field without disturbing any other field — in particular it does NOT
    touch ``dispatch_state`` or any ADR-011 canonical metadata key (lead-xp5f
    scenarios 9d0aee49 / e236edb2 / 4cc30de3: a nudge leaves dispatch_state
    unchanged and the bd record receives ONLY an appended note).

    ``root`` is the shop root whose ``.beads`` workspace holds the bead
    (the lead shop for a nudge that references a lead dispatch work_id).

    Best-effort guard: when bd is unavailable in this environment the call is
    a no-op, mirroring the other bd side-effect helpers — the messaging-layer
    delivery (the postgres nudge row) must not be blocked by a bd hiccup.
    Raises BdFacadeError only when bd is present but the note command fails.
    """
    if not bd_available(root):
        return
    _run_bd(["note", work_id, text], cwd=root)
    _fsync_workspace(root)


class BdFacadeError(RuntimeError):
    """Raised when a bd invocation fails in a way the CLI must surface."""


def bd_available(cwd: Path) -> bool:
    """Return True iff a bd workspace is reachable from ``cwd``.

    Used to make the bd side effects best-effort-but-loud: in environments
    where bd is not installed or no ``.beads`` workspace exists (which must
    never be the case for a real lead shop), the CLI can decide whether to
    skip the bd flip or fail. The scenarios all run against a real bd
    workspace, so this is primarily a guard for non-lead invocations.
    """
    if not _bd_on_path():
        return False
    # ADR-018 / lead-mxxm: on the lead host a BC's registered shop_root is
    # load-bearing for nothing and need not exist on disk. `_run_bd` scopes
    # cwd to that path, and subprocess raises (FileNotFoundError /
    # NotADirectoryError) — not the BdFacadeError callers catch — when cwd is
    # absent. A non-existent path can carry no `.beads` workspace, so treat it
    # as "no bd workspace reachable" rather than letting the probe raise.
    if not cwd.is_dir():
        return False
    # `bd list` exits non-zero if no workspace is discoverable.
    proc = _run_bd(["list", "--json"], cwd=cwd, check=False)
    return proc.returncode == 0


def _bd_on_path() -> bool:
    from shutil import which

    return which("bd") is not None


def _run_bd(
    args: list[str], *, cwd: Path, check: bool = True
) -> subprocess.CompletedProcess:
    """Run a bd subcommand with cwd scoped to the shop root.

    bd is configured with dolt.auto-commit=on in the shops' workspaces, so a
    create/update is durably committed to the embedded dolt store by the time
    the process returns zero. We additionally fsync the workspace directory
    (see :func:`_fsync_workspace`) to pin the Step-1 durability contract.
    """
    proc = subprocess.run(
        ["bd", *args],
        cwd=str(cwd),
        capture_output=True,
        text=True,
    )
    if check and proc.returncode != 0:
        raise BdFacadeError(
            f"bd {' '.join(args)} failed (exit {proc.returncode}): "
            f"{proc.stderr.strip() or proc.stdout.strip()}"
        )
    return proc


def _beads_dir(cwd: Path) -> Path | None:
    """Locate the ``.beads`` workspace by walking up from ``cwd``."""
    here = cwd.resolve()
    for cand in [here, *here.parents]:
        beads = cand / ".beads"
        if beads.is_dir():
            return beads
    return None


def _fsync_workspace(cwd: Path) -> None:
    """fsync the bd workspace so the Step-1 bd write is durable on disk
    BEFORE the Step-2 postgres deposit begins (lead-tuu5 / ADR-012 recovery
    premise). We fsync the embedded-dolt directory and the exported
    issues.jsonl if present, then the containing directory, so a crash
    between Steps 1 and 2 leaves a recoverable bd record of intent.
    """
    beads = _beads_dir(cwd)
    if beads is None:
        return
    targets: list[Path] = [beads]
    for name in ("issues.jsonl", "embeddeddolt"):
        p = beads / name
        if p.exists():
            targets.append(p)
    for path in targets:
        try:
            if path.is_dir():
                fd = os.open(str(path), os.O_RDONLY)
                try:
                    os.fsync(fd)
                finally:
                    os.close(fd)
            else:
                fd = os.open(str(path), os.O_RDONLY)
                try:
                    os.fsync(fd)
                finally:
                    os.close(fd)
        except OSError:
            # fsync of a directory is not portable everywhere; the dolt
            # auto-commit already provides durability. Best-effort.
            pass


def create_dispatch_bead(
    lead_root: Path,
    work_id: str,
    *,
    dispatched_to_bc: str,
    dispatch_message_type: str,
    scenario_hashes_pinned: list[str] | None = None,
    depends_on_dispatch: str | None = None,
    bc_origin_main_commit_at_dispatch: str | None = None,
    payload_ref: str | None = None,
    outbox_pending_at: str | None = None,
) -> None:
    """Step 1 of the bd-first send protocol.

    Creates a lead bd entry with id == ``work_id`` carrying the canonical
    dispatch field set as structured metadata at dispatch_state=outbox_pending,
    then fsyncs the workspace to disk before the caller proceeds to Step 2.

    Only non-empty optional fields are written, so a request_maintenance with
    no scenario hashes does not carry an empty ``scenario_hashes_pinned`` key.
    """
    metadata: dict[str, Any] = {
        KEY_DISPATCHED_TO_BC: dispatched_to_bc,
        KEY_DISPATCH_MESSAGE_TYPE: dispatch_message_type,
        KEY_DISPATCH_STATE: STATE_OUTBOX_PENDING,
    }
    if scenario_hashes_pinned:
        metadata[KEY_SCENARIO_HASHES_PINNED] = ",".join(scenario_hashes_pinned)
    if depends_on_dispatch:
        metadata[KEY_DEPENDS_ON_DISPATCH] = depends_on_dispatch
    if bc_origin_main_commit_at_dispatch:
        metadata[KEY_BC_ORIGIN_MAIN_COMMIT] = bc_origin_main_commit_at_dispatch
    if payload_ref:
        metadata[KEY_PAYLOAD_REF] = payload_ref
    if outbox_pending_at:
        metadata[KEY_OUTBOX_PENDING_AT] = outbox_pending_at

    title = f"dispatch {dispatch_message_type} -> {dispatched_to_bc} ({work_id})"
    # --force allows forcing an id whose prefix does not match the workspace
    # prefix (lead beads dispatched into a workspace whose default prefix may
    # differ). The metadata is written atomically with create.
    _run_bd(
        [
            "create",
            title,
            "--id",
            work_id,
            "--metadata",
            json.dumps(metadata),
            "--force",
        ],
        cwd=lead_root,
    )
    _fsync_workspace(lead_root)


def set_dispatch_state(lead_root: Path, work_id: str, state: str) -> None:
    """Flip a single ``dispatch_state`` metadata key in place.

    Uses ``bd update --set-metadata dispatch_state=<state>`` which replaces
    only that key, preserving the rest of the canonical field set.
    """
    _run_bd(
        ["update", work_id, "--set-metadata", f"{KEY_DISPATCH_STATE}={state}"],
        cwd=lead_root,
    )


def get_dispatch_bead(lead_root: Path, work_id: str) -> dict[str, Any] | None:
    """Return the bead's record (incl. ``metadata``) as a dict, or None."""
    proc = _run_bd(["show", work_id, "--json"], cwd=lead_root, check=False)
    if proc.returncode != 0 or not proc.stdout.strip():
        return None
    try:
        data = json.loads(proc.stdout)
    except json.JSONDecodeError:
        return None
    rec = data[0] if isinstance(data, list) else data
    return rec if isinstance(rec, dict) else None


def get_dispatch_metadata(lead_root: Path, work_id: str) -> dict[str, Any] | None:
    rec = get_dispatch_bead(lead_root, work_id)
    if rec is None:
        return None
    return rec.get("metadata") or {}


def list_dispatch_beads(lead_root: Path) -> list[dict[str, Any]]:
    """Return all bead records in the lead workspace with their metadata."""
    proc = _run_bd(["list", "--json"], cwd=lead_root, check=False)
    if proc.returncode != 0 or not proc.stdout.strip():
        return []
    try:
        data = json.loads(proc.stdout)
    except json.JSONDecodeError:
        return []
    rows = data if isinstance(data, list) else data.get("issues", [])
    return [r for r in rows if isinstance(r, dict)]


# ---------------------------------------------------------------------------
# Dispatch-dependency operations (PDR-010 / ADR-013).
#
# The lead architect records dispatch dependencies as bd depends-on edges via
# `bd dep add <dependent> <predecessor>`. shop-msg consults those edges (it
# never re-checks acyclicity at dispatch time — ADR-013 decision 8 makes the
# graph invariantly acyclic by construction, enforced bd-side).
# ---------------------------------------------------------------------------


def list_depends_on(lead_root: Path, work_id: str) -> list[str]:
    """Return the work_ids ``work_id`` directly depends on (its predecessors).

    Backed by ``bd dep list <work_id> --json``. Returns an empty list when the
    bead has no dependencies or does not exist. This is the introspection step
    shop-msg's strict mode walks; it trusts the graph is a DAG (ADR-013
    decision 8) so a simple one-hop enumeration of direct predecessors is the
    consultation contract the scenarios pin.
    """
    proc = _run_bd(["dep", "list", work_id, "--json"], cwd=lead_root, check=False)
    if proc.returncode != 0 or not proc.stdout.strip():
        return []
    try:
        data = json.loads(proc.stdout)
    except json.JSONDecodeError:
        return []
    rows = data if isinstance(data, list) else data.get("dependencies", [])
    return [r["id"] for r in rows if isinstance(r, dict) and r.get("id")]


def predecessor_dispatch_state(lead_root: Path, work_id: str) -> str | None:
    """Return the ``dispatch_state`` metadata of a predecessor bead, or None.

    None means the bead carries no dispatch_state metadata (e.g. a plain
    planning bead that was never dispatched). Strict mode treats a missing or
    non-``closed`` state as "predecessor not yet closed".
    """
    metadata = get_dispatch_metadata(lead_root, work_id)
    if not metadata:
        return None
    return metadata.get(KEY_DISPATCH_STATE)


def first_unclosed_predecessor(lead_root: Path, work_id: str) -> tuple[str, str] | None:
    """Return (predecessor_work_id, its_dispatch_state) for the first
    depends-on predecessor of ``work_id`` that is NOT at dispatch_state=closed,
    or None when every predecessor is closed (or there are none).

    The dispatch_state is reported as the bead's metadata value, defaulting to
    the literal string of bd's native status when no dispatch_state metadata is
    present, so the caller can name a concrete state in its refusal message.
    """
    for predecessor in list_depends_on(lead_root, work_id):
        state = predecessor_dispatch_state(lead_root, work_id=predecessor)
        if state != STATE_CLOSED:
            # Report a concrete state for the refusal message: prefer the
            # dispatch_state metadata; fall back to bd native status.
            reported = state
            if reported is None:
                rec = get_dispatch_bead(lead_root, predecessor)
                reported = (rec or {}).get("status") or "unknown"
            return predecessor, reported
    return None


def create_queued_dispatch_bead(
    lead_root: Path,
    work_id: str,
    *,
    dispatched_to_bc: str,
    dispatch_message_type: str,
    pending_dependency: str,
    scenario_hashes_pinned: list[str] | None = None,
    bc_origin_main_commit_at_dispatch: str | None = None,
    payload_ref: str | None = None,
    outbox_pending_at: str | None = None,
) -> None:
    """Queued-mode Step 1 (ADR-013 decision 4 / ADR-012 atomicity).

    Writes the lead bd entry at dispatch_state=outbox_pending carrying a
    ``pending_dependency`` pointer, in ONE ``bd create --metadata`` payload (a
    single atomic bd write — the postgres deposit is DEFERRED until the
    predecessor closes and a promote scan runs). No postgres row is inserted
    here; the durable queued intent lives in bd alone.
    """
    metadata: dict[str, Any] = {
        KEY_DISPATCHED_TO_BC: dispatched_to_bc,
        KEY_DISPATCH_MESSAGE_TYPE: dispatch_message_type,
        KEY_DISPATCH_STATE: STATE_OUTBOX_PENDING,
        KEY_PENDING_DEPENDENCY: pending_dependency,
    }
    if scenario_hashes_pinned:
        metadata[KEY_SCENARIO_HASHES_PINNED] = ",".join(scenario_hashes_pinned)
    if bc_origin_main_commit_at_dispatch:
        metadata[KEY_BC_ORIGIN_MAIN_COMMIT] = bc_origin_main_commit_at_dispatch
    if payload_ref:
        metadata[KEY_PAYLOAD_REF] = payload_ref
    if outbox_pending_at:
        metadata[KEY_OUTBOX_PENDING_AT] = outbox_pending_at

    title = (
        f"queued {dispatch_message_type} -> {dispatched_to_bc} "
        f"({work_id}) pending {pending_dependency}"
    )
    _run_bd(
        [
            "create",
            title,
            "--id",
            work_id,
            "--metadata",
            json.dumps(metadata),
            "--force",
        ],
        cwd=lead_root,
    )
    _fsync_workspace(lead_root)


def clear_pending_dependency(lead_root: Path, work_id: str) -> None:
    """Unset the ``pending_dependency`` metadata key (promote scan step)."""
    _run_bd(
        ["update", work_id, "--unset-metadata", KEY_PENDING_DEPENDENCY],
        cwd=lead_root,
    )
