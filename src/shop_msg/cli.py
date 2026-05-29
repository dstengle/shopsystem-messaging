"""shop-msg CLI entry point.

Subcommands:
    respond clarify --bc-root PATH --work-id ID --question TEXT
        Writes a Clarify message to the Postgres messages table as an
        outbox row. Raises on collision (duplicate bc/work_id/direction).
    respond work_done --bc-root PATH --work-id ID --status STATUS
                      [--scenario-hash HASH ...] [--summary TEXT]
        Writes a WorkDone message to the Postgres messages table.
    respond mechanism_observation --bc-root PATH --work-id ID --subject TEXT
                                  --body TEXT [--observed-during ID]
                                  [--evidence TEXT ...] [--proposed-action TEXT]
                                  [--provenance-ref REF]
        Writes a MechanismObservation message to the Postgres messages table.
        The optional --provenance-ref names a BC-side record (issue, document,
        commit) where long-form analysis lives; the wire schema does not
        constrain that reference to any particular tracker so the
        catalog stays decoupled from the BC's work-registry choice
        (lead-231 item C).
    send request_maintenance --bc-root PATH --work-id ID --description TEXT
                             [--acceptance-criterion TEXT ...]
                             [--file-hint TEXT ...]
        Writes a RequestMaintenance message to the Postgres messages table
        as an inbox row.
    send assign_scenarios --bc-root PATH --work-id ID --feature-title TEXT
                          --bc-tag NAME --scenario-file PATH ...
        Writes an AssignScenarios message to the Postgres messages table.
        Each --scenario-file becomes one ScenarioPayload. The hash for
        each scenario is computed by shelling out to the `scenarios hash`
        CLI (the canonicalization rule lives in the scenarios package,
        not here).
    send request_bugfix --bc-root PATH --work-id ID --description TEXT
                        [--feature-title TEXT --bc-tag NAME
                         --scenario-file PATH ...]
        Writes a RequestBugfix message to the Postgres messages table.
        Scenarios are optional.
    read outbox --bc-root PATH --work-id ID
        Reads the latest outbox row for a work_id from Postgres, validates
        it against the BCResponse union, and dumps the canonical YAML to
        stdout. Exits non-zero (with a stderr message) when no outbox
        row matches the work_id or validation fails.
    read inbox --bc-root PATH --work-id ID
        Reads the inbox row for a work_id from Postgres, validates it
        against the LeadMessage union, and dumps the canonical YAML to
        stdout. Exits non-zero (with a stderr message) when no inbox
        row matches the work_id or validation fails.
    pending inbox --bc-root PATH
        Enumerates inbox messages that have no matching outbox response
        via a Postgres query. Stdout is one line per pending message of
        the form "<work_id> <message_type>". Exit zero in both the
        empty and non-empty cases.
    pending outbox --lead-root PATH [--bc NAME]
        Lead-side counterpart: queries Postgres for outbox rows across
        sibling BC clones. With --bc NAME, scopes to a single BC; without
        it, every BC under repos/ is included.
    dump [--bc-root PATH] [--direction inbox|outbox] [--limit N]
        Operator debugging: dumps rows from the messages table to stdout
        as YAML. With --bc-root, scopes to that BC. With --direction,
        scopes to inbox or outbox. With --limit, caps the result count.
    prime --bc-root PATH
        Session-start orientation. Prints the current DSN, DB reachability,
        count and list of pending inbox messages, and a brief CLI reminder.
        Exits 0 when the DB is reachable; non-zero when unreachable.
    watch --bc-root PATH
        Monitor-compatible inbox watcher. Drains unprocessed inbox messages
        on startup (one '<work_id> <message_type>' line per pending item),
        emits a 'READY' sentinel line, then blocks on Postgres LISTEN waiting
        for new inbox inserts. Each NOTIFY produces one output line. Never
        exits under normal operation. Exits non-zero with a message naming
        the DSN when the database is unreachable at startup.
    watch --lead-root PATH
        Lead-side outbox watcher. Discovers BC repos under <lead-root>/repos/,
        drains existing outbox rows on startup, emits a 'READY' sentinel line,
        then blocks on Postgres LISTEN for new outbox inserts across all BCs.
        Each NOTIFY produces one '<work_id> <message_type>' output line.
    consume outbox --bc-root PATH --work-id ID --message-type TYPE
        Lead-side consumption: marks a specific outbox row (identified by BC,
        work_id, and message_type) as consumed so it no longer appears in
        'pending outbox' output. Exits zero on success; exits non-zero with a
        descriptive stderr message when no matching unconsumed outbox row exists.
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

import yaml

from pydantic import TypeAdapter, ValidationError

from catalog.schemas import (
    AssignScenarios,
    BCResponse,
    Clarify,
    LeadMessage,
    MechanismObservation,
    RequestBugfix,
    RequestMaintenance,
    ScenarioPayload,
    WorkDone,
)
from shop_msg import bd_facade
from shop_msg.storage import (
    CollisionError,
    OutboxDepositError,
    consume_outbox_message,
    delete_bc_messages,
    dispatch_inbox_row_exists,
    existing_lead_inbox_message_type,
    inbox_row_exists,
    insert_bc_response,
    insert_message,
    insert_raw_payload,
    outbox_row_exists,
    presence_status,
    query_pending_inbox,
    query_pending_lead_inbox,
    query_pending_outbox,
    read_inbox_message,
    read_lead_inbox_message,
    read_outbox_messages,
    registry_add,
    registry_list,
    registry_remove,
    registry_sync,
    resolve_lead_shop,
    resolve_shop_name,
    watch_inbox,
    watch_lead_inbox,
    watch_outbox_for_lead,
)

_response_adapter = TypeAdapter(BCResponse)
_lead_adapter = TypeAdapter(LeadMessage)

# ---------------------------------------------------------------------------
# Migration guards for removed flags
# ---------------------------------------------------------------------------


class _RemovedFlagAction(argparse.Action):
    """argparse Action that immediately errors out with a migration message."""

    def __call__(self, parser, namespace, values, option_string=None):
        flag = option_string or self.option_strings[0]
        if flag == "--bc-root":
            replacement = "--bc <name>"
        elif flag == "--lead-root":
            replacement = "--lead <name>"
        else:
            replacement = "--bc <name> or --lead <name>"
        print(
            f"shop-msg: {flag} is no longer supported. "
            f"Use {replacement} instead (name-based addressing, PDR-007).",
            file=sys.stderr,
        )
        sys.exit(1)


def _add_removed_flag(parser: argparse.ArgumentParser, flag: str) -> None:
    """Register a removed flag so it produces a migration error instead of
    'unrecognized arguments'."""
    parser.add_argument(
        flag,
        nargs="?",
        action=_RemovedFlagAction,
        help=argparse.SUPPRESS,
    )


def _walk_up_resolve_shop(start: Path | None = None) -> tuple[str, str]:
    """Walk up from CWD looking for the nearest .claude/shop/ marker.

    Returns ``(canonical_name, shop_type)`` read literally from
    ``.claude/shop/name.md`` and ``.claude/shop/type.md`` of the nearest
    ancestor that contains both files.

    Exits non-zero with a diagnostic to stderr when:
      - No ancestor (up to the filesystem root) contains a
        ``.claude/shop/`` directory with both ``name.md`` and ``type.md``.
      - The nearest ``.claude/shop/`` is partial (only one of the two
        files is present).

    The shop_type value is one of ``"bc"`` or ``"lead"``; values outside
    this set are surfaced as an error so a typo in type.md does not
    silently route a shop into the wrong mode.
    """
    cwd = (start or Path.cwd()).resolve()

    cur = cwd
    while True:
        marker = cur / ".claude" / "shop"
        if marker.is_dir():
            name_path = marker / "name.md"
            type_path = marker / "type.md"
            has_name = name_path.is_file()
            has_type = type_path.is_file()
            if has_name and has_type:
                name = name_path.read_text().strip()
                shop_type = type_path.read_text().strip()
                if shop_type not in ("bc", "lead"):
                    print(
                        f"shop-msg: shop marker at {marker} has unrecognized "
                        f"shop type {shop_type!r}; expected 'bc' or 'lead'.",
                        file=sys.stderr,
                    )
                    sys.exit(1)
                return name, shop_type
            # Partial marker (one file but not the other) — the resolved
            # marker is incomplete; do not silently fall through to a
            # higher ancestor and do not treat it as either shop type.
            missing = "type.md" if has_name else "name.md"
            print(
                f"shop-msg: shop marker at {marker} is incomplete: "
                f"missing {missing!r}. A .claude/shop/ marker must contain "
                f"both name.md and type.md.",
                file=sys.stderr,
            )
            sys.exit(1)
        if cur.parent == cur:
            break
        cur = cur.parent

    print(
        f"shop-msg: no shop was found by walking up from the current "
        f"directory {cwd!s}; no ancestor contains a .claude/shop/ "
        f"directory with both name.md and type.md.\n"
        f"Remediation: cd into a shop directory (one whose .claude/shop/ "
        f"contains both name.md and type.md), or pass an explicit "
        f"--bc <name> or --lead <name> flag.",
        file=sys.stderr,
    )
    sys.exit(1)


def _apply_cwd_resolution(args: argparse.Namespace) -> None:
    """Populate args.bc or args.lead from CWD if neither was given.

    Used by the bare-invocation surface (``shop-msg prime / pending / read /
    respond / watch``): when neither addressing flag is supplied, walk up
    from CWD to find the invoking shop's .claude/shop/ marker, then
    populate the appropriate args attribute based on the shop type read
    from type.md (PDR-008).

    No-op when one of the flags is already set (explicit-flag precedence).
    """
    bc = getattr(args, "bc", None)
    lead = getattr(args, "lead", None)
    if bc is not None or lead is not None:
        return
    name, shop_type = _walk_up_resolve_shop()
    if shop_type == "lead":
        args.lead = name
    else:
        args.bc = name


def _resolve_bc(args: argparse.Namespace) -> str:
    """Resolve --bc <name> to the bc_root path via the registry.

    Exits non-zero if the name is not registered.
    """
    name = args.bc
    path = resolve_shop_name(name)
    if path is None:
        print(
            f"shop-msg: shop name {name!r} is not registered in the registry. "
            f"Run 'shop-msg registry add' to register it first.",
            file=sys.stderr,
        )
        sys.exit(1)
    return str(Path(path).resolve())


def _resolve_lead(args: argparse.Namespace) -> str:
    """Resolve --lead <name> to the lead_root path via the registry.

    Exits non-zero if the name is not registered.
    """
    name = args.lead
    path = resolve_shop_name(name)
    if path is None:
        print(
            f"shop-msg: lead name {name!r} is not registered in the registry. "
            f"Run 'shop-msg registry add' to register it first.",
            file=sys.stderr,
        )
        sys.exit(1)
    return str(Path(path).resolve())


def _bc_origin_main_commit(bc_root: str) -> str | None:
    """Return the short origin/main HEAD SHA of the BC clone, or None.

    Recorded on the lead bd entry at dispatch time as
    ``bc_origin_main_commit_at_dispatch`` (ADR-011 field mapping) so the lead
    can later answer "what BC commit was current when we dispatched this?".
    Best-effort: returns None when the path is not a git clone or git is not
    available. Tries origin/main first, falling back to HEAD.
    """
    root = Path(bc_root)
    if not (root / ".git").exists():
        return None
    for ref in ("origin/main", "HEAD"):
        try:
            proc = subprocess.run(
                ["git", "rev-parse", "--short", ref],
                cwd=str(root),
                capture_output=True,
                text=True,
            )
        except OSError:
            return None
        if proc.returncode == 0 and proc.stdout.strip():
            return proc.stdout.strip()
    return None


def _resolve_registered_lead() -> str | None:
    """Return the shop_root for the registered lead shop, or None.

    Used by ``respond`` commands to route responses to the lead's inbox.
    """
    path = resolve_lead_shop()
    if path is None:
        return None
    return str(Path(path).resolve())


def _compute_scenario_hash(gherkin_body: str) -> str:
    """Shell out to `scenarios hash` to canonicalize and hash a scenario body.

    The package boundary is intentional: the canonicalization rule belongs
    to the scenarios package, and shop-msg composes it as an external tool
    just like any other consumer would.
    """
    result = subprocess.run(
        ["scenarios", "hash"],
        input=gherkin_body,
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.strip()


def _cmd_respond_clarify(args: argparse.Namespace) -> int:
    bc_root = _resolve_bc(args)

    if not args.question:
        print(
            "shop-msg respond clarify: question must not be empty",
            file=sys.stderr,
        )
        return 1

    if not args.work_id or "/" in args.work_id or ".." in args.work_id:
        print(
            "shop-msg respond clarify: refusing unsafe work_id",
            file=sys.stderr,
        )
        return 1

    lead_root = _resolve_registered_lead()
    if lead_root is None:
        print(
            "shop-msg respond clarify: no lead shop is registered in the registry. "
            "Run 'shop-msg registry add --lead-shop' to register the lead first.",
            file=sys.stderr,
        )
        return 1

    message = Clarify(
        message_type="clarify",
        work_id=args.work_id,
        question=args.question,
    )

    try:
        insert_bc_response(
            lead_root,
            bc_root,
            args.work_id,
            "clarify",
            message.model_dump(),
            force=getattr(args, "force", False),
        )
    except CollisionError:
        existing = existing_lead_inbox_message_type(
            lead_root, args.work_id, "clarify"
        ) or "clarify"
        print(
            f"shop-msg respond clarify: refusing to overwrite existing "
            f"{existing} response for work_id={args.work_id!r} "
            f"(use --force to replace)",
            file=sys.stderr,
        )
        return 1

    # ADR-017 decision 4 (lead-sn1e): the BC bead paired with this work_id
    # flips to "blocked" and gets a note summarizing the question, as a CLI
    # side effect of the same respond invocation. Best-effort and scoped to
    # the BC's own bd workspace; never overturns the successful emission.
    _apply_bc_bead_response(
        bc_root,
        args.work_id,
        message_type="clarify",
        note=f"clarify: {args.question}",
        op_name="respond clarify",
    )
    return 0


def _apply_bc_bead_response(
    bc_root: str,
    work_id: str,
    *,
    message_type: str,
    note: str | None = None,
    status: str | None = None,
    op_name: str = "respond",
) -> None:
    """Apply the ADR-017 decision-4 BC-bead status side effect for a respond.

    Runs AFTER the messaging emission has succeeded. Best-effort: a bd hiccup
    is reported on stderr but does not change the command's exit status, since
    the primary messaging action (the lead-inbox deposit) already landed.
    """
    bc_root_path = Path(bc_root)
    if not bd_facade.bd_available(bc_root_path):
        return
    try:
        bd_facade.apply_response_side_effect(
            bc_root_path,
            work_id,
            message_type=message_type,
            status=status,
            note=note,
        )
    except bd_facade.BdFacadeError as exc:
        print(
            f"shop-msg {op_name}: messaging emission succeeded but the BC-side "
            f"bead status side effect failed for work_id={work_id!r}: {exc}",
            file=sys.stderr,
        )


def _cmd_respond_work_done(args: argparse.Namespace) -> int:
    bc_root = _resolve_bc(args)

    lead_root = _resolve_registered_lead()
    if lead_root is None:
        print(
            "shop-msg respond work_done: no lead shop is registered in the registry. "
            "Run 'shop-msg registry add --lead-shop' to register the lead first.",
            file=sys.stderr,
        )
        return 1

    message = WorkDone(
        message_type="work_done",
        work_id=args.work_id,
        status=args.status,
        summary=args.summary,
        scenario_hashes=list(args.scenario_hash or []),
    )

    try:
        insert_bc_response(
            lead_root,
            bc_root,
            args.work_id,
            "work_done",
            message.model_dump(),
            force=getattr(args, "force", False),
        )
    except CollisionError:
        existing = existing_lead_inbox_message_type(
            lead_root, args.work_id, "work_done"
        ) or "work_done"
        print(
            f"shop-msg respond work_done: refusing to overwrite existing "
            f"{existing} response for work_id={args.work_id!r} "
            f"(use --force to replace)",
            file=sys.stderr,
        )
        return 1

    # ADR-017 decision 4 (lead-sn1e): work_done(complete) -> BC bead closed;
    # work_done(blocked|partial) -> blocked. CLI side effect of the emission.
    _apply_bc_bead_response(
        bc_root,
        args.work_id,
        message_type="work_done",
        status=args.status,
        op_name="respond work_done",
    )
    return 0


def _cmd_respond_mechanism_observation(args: argparse.Namespace) -> int:
    bc_root = _resolve_bc(args)

    # Path-safety: refuse work_ids that would escape safe naming.
    if "/" in args.work_id or ".." in args.work_id or not args.work_id:
        print(
            f"shop-msg respond mechanism_observation: refusing unsafe "
            f"work_id {args.work_id!r}",
            file=sys.stderr,
        )
        return 1

    lead_root = _resolve_registered_lead()
    if lead_root is None:
        print(
            "shop-msg respond mechanism_observation: no lead shop is registered "
            "in the registry. Run 'shop-msg registry add --lead-shop' first.",
            file=sys.stderr,
        )
        return 1

    message = MechanismObservation(
        message_type="mechanism_observation",
        subject=args.subject,
        observed_during=args.observed_during,
        body=args.body,
        evidence=list(args.evidence) if args.evidence else None,
        proposed_action=args.proposed_action,
        provenance_ref=args.provenance_ref,
    )

    try:
        insert_bc_response(
            lead_root,
            bc_root,
            args.work_id,
            "mechanism_observation",
            message.model_dump(exclude_none=True),
            force=getattr(args, "force", False),
        )
    except CollisionError:
        existing = existing_lead_inbox_message_type(
            lead_root, args.work_id, "mechanism_observation"
        ) or "mechanism_observation"
        print(
            f"shop-msg respond mechanism_observation: refusing to overwrite "
            f"existing {existing} response for work_id={args.work_id!r} "
            f"(use --force to replace)",
            file=sys.stderr,
        )
        return 1

    # ADR-017 decision 4 (lead-sn1e): mechanism_observation leaves the BC
    # bead's status UNCHANGED but appends a note recording the observation.
    # (The scenario illustrates this with a "--note" flag; the real CLI
    # carries the observation as --subject/--body, so the appended note is
    # composed from those — see lead-sn1e mechanism_observation surfacing.)
    _apply_bc_bead_response(
        bc_root,
        args.work_id,
        message_type="mechanism_observation",
        note=f"mechanism_observation: {args.subject} — {args.body}",
        op_name="respond mechanism_observation",
    )
    return 0


def _resolve_send_sender() -> str | None:
    """Resolve the sender's canonical name for `shop-msg send`.

    Walks up from CWD to find the nearest .claude/shop/ marker and returns
    the canonical name read from ``name.md``. Unlike the bare-invocation
    resolution used by prime/pending/read/respond/watch, this is a soft
    lookup: when no marker is found we return None instead of exiting,
    because the sender's identity is supplementary context — the recipient
    flag is what matters for delivery.

    The recipient address is NEVER resolved here; that remains explicit
    via --bc / --lead (PDR-008, scenario 6492effd22a6d3e7).
    """
    try:
        name, _shop_type = _walk_up_resolve_shop()
    except SystemExit:
        # No marker found — the sender's identity is not derivable, but
        # send still proceeds (the recipient flag is what delivery uses).
        return None
    return name


def _bd_first_send(
    *,
    command: str,
    bc_root: str,
    bc_name: str,
    work_id: str,
    message_type: str,
    payload: dict,
    scenario_hashes_pinned: list[str] | None,
    depends_on_dispatch: str | None,
    bc_origin_main_commit: str | None,
    payload_ref: str | None,
    queue_on_dependency: bool = False,
) -> int:
    """Run the bd-first 3-step send protocol (PDR-010 / ADR-012).

    Step 1: write the lead bd entry at dispatch_state=outbox_pending carrying
            the canonical field set as structured metadata, fsynced to disk.
    Step 2: insert the postgres outbox (inbox-direction) row.
    Step 3: flip the bd entry to dispatch_state=dispatched.

    The Step-3 flip is GUARDED by Step-2 success: when the postgres deposit
    raises (OutboxDepositError), the bd entry is LEFT at outbox_pending and
    the command exits non-zero naming the failure, so a later
    ``shop-msg sweep`` can recover it (lead-tuu5 scenario ef558dc7233466d8).
    bd never lies about transmission state.

    When no lead shop is registered (a non-lead invocation, or a bare test of
    the messaging layer), the bd steps are skipped and only the postgres
    deposit runs, preserving the pre-lead-tuu5 behavior for callers that do
    not participate in the bd dispatch lifecycle.
    """
    lead_root_str = _resolve_registered_lead()
    use_bd = lead_root_str is not None and bd_facade.bd_available(Path(lead_root_str))
    lead_root = Path(lead_root_str) if lead_root_str else None

    # Dispatch-dependency consultation (PDR-010 / ADR-013). Before any write,
    # consult the bd depends-on edges of this work_id. The graph is invariantly
    # acyclic by construction (ADR-013 decision 8: bd rejects cycles, shop-msg
    # does NOT re-check), so a one-hop predecessor enumeration is safe.
    if use_bd:
        unmet = bd_facade.first_unclosed_predecessor(lead_root, work_id)
        if unmet is not None:
            predecessor, pred_state = unmet
            if not queue_on_dependency:
                # Strict mode (default): TOTAL refusal — no postgres artifact
                # and no bd artifact. We refuse BEFORE Step 1 so re-running
                # after the predecessor closes is the same as a first run.
                print(
                    f"shop-msg send {command}: refusing to dispatch "
                    f"{work_id!r}: predecessor {predecessor!r} is at "
                    f"dispatch_state={pred_state!r}, not 'closed'. "
                    f"Use --queue-on-dependency to defer until it closes.",
                    file=sys.stderr,
                )
                return 1
            # Queued mode (ADR-013 decision 4): defer the postgres deposit.
            # Write the queued intent to bd alone, as a single atomic
            # `bd create --metadata` payload (ADR-012 atomicity), then return.
            outbox_pending_at = (
                __import__("datetime")
                .datetime.now(__import__("datetime").timezone.utc)
            ).isoformat()
            try:
                bd_facade.create_queued_dispatch_bead(
                    lead_root,
                    work_id,
                    dispatched_to_bc=bc_name,
                    dispatch_message_type=message_type,
                    pending_dependency=predecessor,
                    scenario_hashes_pinned=scenario_hashes_pinned,
                    bc_origin_main_commit_at_dispatch=bc_origin_main_commit,
                    payload_ref=payload_ref,
                    outbox_pending_at=outbox_pending_at,
                )
            except bd_facade.BdFacadeError as exc:
                print(
                    f"shop-msg send {command}: queued-mode bd write failed: "
                    f"{exc}",
                    file=sys.stderr,
                )
                return 1
            print(
                f"shop-msg send {command}: dispatch {work_id!r} queued behind "
                f"{predecessor!r}; postgres deposit deferred until it closes "
                f"(run `shop-msg promote` after closing the predecessor)."
            )
            return 0

    # Step 1: bd write (durable) BEFORE any postgres write.
    if use_bd:
        outbox_pending_at = (
            __import__("datetime").datetime.now(__import__("datetime").timezone.utc)
        ).isoformat()
        try:
            bd_facade.create_dispatch_bead(
                lead_root,
                work_id,
                dispatched_to_bc=bc_name,
                dispatch_message_type=message_type,
                scenario_hashes_pinned=scenario_hashes_pinned,
                depends_on_dispatch=depends_on_dispatch,
                bc_origin_main_commit_at_dispatch=bc_origin_main_commit,
                payload_ref=payload_ref,
                outbox_pending_at=outbox_pending_at,
            )
        except bd_facade.BdFacadeError as exc:
            print(f"shop-msg send {command}: bd Step 1 failed: {exc}", file=sys.stderr)
            return 1

    # Step 2: postgres deposit.
    try:
        insert_message(
            bc_root,
            work_id,
            "inbox",
            message_type,
            payload,
            notify=True,
        )
    except CollisionError:
        print(
            f"shop-msg send {command}: refusing to overwrite existing "
            f"inbox entry for work_id={work_id!r}",
            file=sys.stderr,
        )
        return 1
    except OutboxDepositError as exc:
        # Step 2 failed: leave the bd entry at outbox_pending (do NOT flip).
        print(
            f"shop-msg send {command}: postgres deposit failed; bd entry "
            f"{work_id!r} left at dispatch_state=outbox_pending for sweep "
            f"recovery. {exc}",
            file=sys.stderr,
        )
        return 1

    # Step 3: bd flip to dispatched (guarded by Step 2 success).
    if use_bd:
        try:
            bd_facade.set_dispatch_state(
                lead_root, work_id, bd_facade.STATE_DISPATCHED
            )
        except bd_facade.BdFacadeError as exc:
            print(
                f"shop-msg send {command}: postgres deposit succeeded but bd "
                f"Step 3 flip failed: {exc}",
                file=sys.stderr,
            )
            return 1
    return 0


def _scenario_hashes_from_payload(scenarios: list[ScenarioPayload]) -> list[str] | None:
    hashes = [s.hash for s in scenarios]
    return hashes or None


def _load_payload_file(path_str: str, message_type: str, work_id: str) -> dict:
    """Load a --payload YAML/JSON file that pins a complete lead-to-BC message.

    The file is the authoritative content source: it carries the message body
    (description, scenarios with pre-computed hashes, etc.). The CLI overrides
    message_type and work_id from the command-line flags so the on-wire row is
    keyed consistently, and validates the result against the matching schema.
    Returns the validated payload dict ready for the postgres deposit.
    """
    raw = Path(path_str).read_text()
    data = yaml.safe_load(raw) or {}
    if not isinstance(data, dict):
        raise ValueError(
            f"--payload file {path_str!r} must contain a YAML/JSON mapping"
        )
    data["message_type"] = message_type
    data["work_id"] = work_id
    data.setdefault("from_shop", _resolve_send_sender())
    model_cls = {
        "request_maintenance": RequestMaintenance,
        "request_bugfix": RequestBugfix,
        "assign_scenarios": AssignScenarios,
    }[message_type]
    message = model_cls(**data)
    return message.model_dump(exclude_none=True)


def _scenario_hashes_from_dict(payload: dict) -> list[str] | None:
    scenarios = payload.get("scenarios") or []
    hashes = [s.get("hash") for s in scenarios if isinstance(s, dict) and s.get("hash")]
    return hashes or None


def _cmd_send_request_maintenance(args: argparse.Namespace) -> int:
    bc_root = _resolve_bc(args)

    if getattr(args, "payload", None):
        payload = _load_payload_file(
            args.payload, "request_maintenance", args.work_id
        )
        hashes = None
    else:
        if args.description is None:
            print(
                "shop-msg send request_maintenance: --description is required "
                "unless --payload is supplied",
                file=sys.stderr,
            )
            return 2
        acceptance_criteria = list(args.acceptance_criterion or []) or None
        file_hints = list(args.file_hint or []) or None
        message = RequestMaintenance(
            message_type="request_maintenance",
            work_id=args.work_id,
            description=args.description,
            acceptance_criteria=acceptance_criteria,
            file_hints=file_hints,
            from_shop=_resolve_send_sender(),
        )
        payload = message.model_dump(exclude_none=True)
        hashes = None

    return _bd_first_send(
        command="request_maintenance",
        bc_root=bc_root,
        bc_name=args.bc,
        work_id=args.work_id,
        message_type="request_maintenance",
        payload=payload,
        scenario_hashes_pinned=hashes,
        depends_on_dispatch=getattr(args, "depends_on", None),
        bc_origin_main_commit=_bc_origin_main_commit(bc_root),
        payload_ref=getattr(args, "payload", None),
        queue_on_dependency=getattr(args, "queue_on_dependency", False),
    )


def _build_scenario_payload(
    path_str: str, feature_title: str, bc_tag: str
) -> ScenarioPayload:
    """Read a scenario body file, wrap it with the standard Feature header
    and tags, and hash the wrapped result. Shared between assign_scenarios
    and request_bugfix because both messages embed the same
    ScenarioPayload shape.

    lead-018 tightened the ScenarioPayload schema so that
    `hash == canonical_hash(gherkin)`. The `gherkin` field stores the
    wrapped text (Feature header + tag line + body), so we must hash the
    wrapped text — hashing the inner body alone would put the CLI on the
    wrong side of the schema invariant.

    Because the canonicalization rule strips `@scenario_hash:` lines
    before hashing, the chicken-and-egg of "the tag line includes the
    hash we're about to compute" resolves naturally: we can construct
    the wrapped text using the eventual hash as a placeholder, hash it,
    then replace the placeholder — but the simpler form below works
    because the canonicalization strips `@scenario_hash:<anything>`
    lines unconditionally. So we build the wrapped text with a sentinel
    hash, compute the canonical hash of that string (the sentinel line
    is dropped by canonicalization), and emit the same wrapped string
    with the now-known hash substituted in. The substitution does not
    perturb the canonical hash because both forms differ only in the
    stripped `@scenario_hash:` line.
    """
    body = Path(path_str).read_text()
    sentinel = "0" * 16
    sentinel_tags = [f"@scenario_hash:{sentinel}", f"@bc:{bc_tag}"]
    sentinel_tagged = (
        f"Feature: {feature_title}\n"
        f"\n"
        f"  {' '.join(sentinel_tags)}\n"
        f"  {body}\n"
    )
    scen_hash = _compute_scenario_hash(sentinel_tagged)
    tags = [f"@scenario_hash:{scen_hash}", f"@bc:{bc_tag}"]
    tagged = (
        f"Feature: {feature_title}\n"
        f"\n"
        f"  {' '.join(tags)}\n"
        f"  {body}\n"
    )
    return ScenarioPayload(hash=scen_hash, tags=tags, gherkin=tagged)


def _cmd_send_request_bugfix(args: argparse.Namespace) -> int:
    bc_root = _resolve_bc(args)

    if getattr(args, "payload", None):
        payload = _load_payload_file(args.payload, "request_bugfix", args.work_id)
        return _bd_first_send(
            command="request_bugfix",
            bc_root=bc_root,
            bc_name=args.bc,
            work_id=args.work_id,
            message_type="request_bugfix",
            payload=payload,
            scenario_hashes_pinned=_scenario_hashes_from_dict(payload),
            depends_on_dispatch=getattr(args, "depends_on", None),
            bc_origin_main_commit=_bc_origin_main_commit(bc_root),
            payload_ref=args.payload,
            queue_on_dependency=getattr(args, "queue_on_dependency", False),
        )

    if args.description is None:
        print(
            "shop-msg send request_bugfix: --description is required unless "
            "--payload is supplied",
            file=sys.stderr,
        )
        return 2

    scenario_files = list(args.scenario_file or [])
    # --feature-title and --bc-tag are conditionally required: only when
    # at least one --scenario-file is supplied. argparse cannot express
    # "required iff" directly, so enforce post-parse here.
    if scenario_files and (args.feature_title is None or args.bc_tag is None):
        print(
            "shop-msg send request_bugfix: --feature-title and --bc-tag are "
            "required when --scenario-file is supplied",
            file=sys.stderr,
        )
        return 2

    scenarios_payload: list[ScenarioPayload] = [
        _build_scenario_payload(path_str, args.feature_title, args.bc_tag)
        for path_str in scenario_files
    ]

    message = RequestBugfix(
        message_type="request_bugfix",
        work_id=args.work_id,
        description=args.description,
        scenarios=scenarios_payload,
        from_shop=_resolve_send_sender(),
    )

    return _bd_first_send(
        command="request_bugfix",
        bc_root=bc_root,
        bc_name=args.bc,
        work_id=args.work_id,
        message_type="request_bugfix",
        payload=message.model_dump(exclude_none=True),
        scenario_hashes_pinned=_scenario_hashes_from_payload(scenarios_payload),
        depends_on_dispatch=getattr(args, "depends_on", None),
        bc_origin_main_commit=_bc_origin_main_commit(bc_root),
        payload_ref=getattr(args, "payload", None),
        queue_on_dependency=getattr(args, "queue_on_dependency", False),
    )


def _cmd_send_assign_scenarios(args: argparse.Namespace) -> int:
    bc_root = _resolve_bc(args)

    if getattr(args, "payload", None):
        payload = _load_payload_file(args.payload, "assign_scenarios", args.work_id)
        return _bd_first_send(
            command="assign_scenarios",
            bc_root=bc_root,
            bc_name=args.bc,
            work_id=args.work_id,
            message_type="assign_scenarios",
            payload=payload,
            scenario_hashes_pinned=_scenario_hashes_from_dict(payload),
            depends_on_dispatch=getattr(args, "depends_on", None),
            bc_origin_main_commit=_bc_origin_main_commit(bc_root),
            payload_ref=args.payload,
            queue_on_dependency=getattr(args, "queue_on_dependency", False),
        )

    if not args.scenario_file or args.feature_title is None or args.bc_tag is None:
        print(
            "shop-msg send assign_scenarios: --scenario-file, --feature-title, "
            "and --bc-tag are required unless --payload is supplied",
            file=sys.stderr,
        )
        return 2

    scenario_files = list(args.scenario_file or [])
    scenarios_payload: list[ScenarioPayload] = [
        _build_scenario_payload(path_str, args.feature_title, args.bc_tag)
        for path_str in scenario_files
    ]

    message = AssignScenarios(
        message_type="assign_scenarios",
        work_id=args.work_id,
        scenarios=scenarios_payload,
        from_shop=_resolve_send_sender(),
    )

    return _bd_first_send(
        command="assign_scenarios",
        bc_root=bc_root,
        bc_name=args.bc,
        work_id=args.work_id,
        message_type="assign_scenarios",
        payload=message.model_dump(exclude_none=True),
        scenario_hashes_pinned=_scenario_hashes_from_payload(scenarios_payload),
        depends_on_dispatch=getattr(args, "depends_on", None),
        bc_origin_main_commit=_bc_origin_main_commit(bc_root),
        payload_ref=getattr(args, "payload", None),
        queue_on_dependency=getattr(args, "queue_on_dependency", False),
    )


def _cmd_read_outbox(args: argparse.Namespace) -> int:
    bc_root = _resolve_bc(args)
    rows = read_outbox_messages(bc_root, args.work_id)
    if not rows:
        print(
            f"shop-msg read outbox: no outbox response found for "
            f"work_id={args.work_id!r} in bc={bc_root!r}",
            file=sys.stderr,
        )
        return 1
    # Use the most-recently-inserted row (last in created_at order).
    raw = rows[-1]
    try:
        message = _response_adapter.validate_python(raw)
    except ValidationError as e:
        print(
            f"shop-msg read outbox: validation failed for "
            f"work_id={args.work_id!r}:\n{e}",
            file=sys.stderr,
        )
        return 1
    print(f"valid {message.message_type} from {args.work_id}:")
    print(yaml.safe_dump(message.model_dump(exclude_none=True), sort_keys=False))
    return 0


def _cmd_read_inbox(args: argparse.Namespace) -> int:
    bc_root = _resolve_bc(args)
    raw = read_inbox_message(bc_root, args.work_id)
    if raw is None:
        # Same pattern as `read outbox`: a missing row is the
        # caller's mistake, not a schema problem; surface a phrase the
        # step definitions can substring-check ("no inbox message").
        print(
            f"shop-msg read inbox: no inbox message found for "
            f"work_id={args.work_id!r} in bc={bc_root!r}",
            file=sys.stderr,
        )
        return 1
    try:
        message = _lead_adapter.validate_python(raw)
    except ValidationError as e:
        # The "validation failed" phrase is load-bearing — the step
        # definition for the schema-validation scenario substring-checks
        # it. Keep the wording aligned with `read outbox`'s sibling
        # branch so a future consolidation doesn't drift one without
        # the other.
        print(
            f"shop-msg read inbox: validation failed for "
            f"work_id={args.work_id!r}:\n{e}",
            file=sys.stderr,
        )
        return 1
    print(f"valid {message.message_type} from {args.work_id}.yaml:")
    print(yaml.safe_dump(message.model_dump(exclude_none=True), sort_keys=False))
    return 0


def _cmd_pending_inbox(args: argparse.Namespace) -> int:
    """Enumerate inbox messages that have no matching outbox response.

    Queries Postgres using the pending-query SQL that replaces the old
    directory-glob walk.

    Registry staleness guard: before querying, verify that the path
    resolved from the registry actually exists on disk.  If it does not,
    the registry entry is stale (e.g. a test fixture overwrote it with a
    tmp path that has since been deleted) and the query would silently
    return zero rows even though the real inbox messages live under a
    different path.  Emitting a clear error surfaces the problem instead
    of hiding it.
    """
    bc_root = _resolve_bc(args)
    resolved = Path(bc_root)
    if not resolved.exists():
        print(
            f"shop-msg: registry entry for {args.bc!r} points to path "
            f"{bc_root!r} which does not exist on disk.  The registry may "
            f"be stale.  Re-register the BC with:\n"
            f"  shop-msg registry add {args.bc} <correct-path>",
            file=sys.stderr,
        )
        return 1
    rows = query_pending_inbox(bc_root)
    bc_root_path = Path(bc_root)
    bd_ok = bd_facade.bd_available(bc_root_path)
    for work_id, message_type in rows:
        # ADR-017 / lead-sn1e: observing an unprocessed inbox row creates a
        # paired BC-side bead in the BC's OWN bd workspace as a CLI side
        # effect (bead-creation-on-FIRST-observation-only; idempotent on
        # re-observation). The bead's type follows the message_type mapping
        # and its title is derived from the inbox payload. Best-effort: a
        # bd-less environment just lists the rows (pre-lead-sn1e behavior).
        if bd_ok:
            try:
                title = _bc_bead_title(bc_root, work_id, message_type)
                bd_facade.create_bc_bead_on_observation(
                    bc_root_path,
                    work_id,
                    message_type=message_type,
                    title=title,
                )
            except bd_facade.BdFacadeError as exc:
                print(
                    f"shop-msg pending inbox: listed the inbox row for "
                    f"work_id={work_id!r} but BC-side bead creation failed: "
                    f"{exc}",
                    file=sys.stderr,
                )
        print(f"{work_id} {message_type}")
    return 0


def _bc_bead_title(bc_root: str, work_id: str, message_type: str) -> str:
    """Derive a BC-bead title from the inbox payload (ADR-017 decision 2).

    For message types carrying a ``description`` (request_bugfix,
    request_maintenance) the title is the description text. assign_scenarios
    carries no description, so the title falls back to a stable label naming
    the message type and the lead work_id.
    """
    raw = read_inbox_message(bc_root, work_id)
    if isinstance(raw, dict):
        desc = raw.get("description")
        if isinstance(desc, str) and desc.strip():
            return desc.strip()
    return f"{message_type} from lead ({work_id})"


def _cmd_pending_lead_inbox(args: argparse.Namespace) -> int:
    """Enumerate BC responses in the lead's inbox.

    Lead-side counterpart to ``pending inbox --bc``.  Queries Postgres for
    inbox rows stored under the lead's namespace — these are BC responses
    that arrived via ``shop-msg respond`` under the new routing model
    (Brief-006 scope C / lead-e9x).
    """
    lead_root = _resolve_lead(args)
    rows = query_pending_lead_inbox(lead_root)
    for work_id, message_type in rows:
        print(f"{work_id} {message_type}")
    return 0


def _cmd_read_lead_inbox(args: argparse.Namespace) -> int:
    """Read and validate a BC response from the lead's inbox.

    Lead-side counterpart to ``read inbox --bc``.  Reads the row stored
    under the lead's namespace for the given work_id and validates it
    against the BCResponse schema union.
    """
    lead_root = _resolve_lead(args)
    raw = read_lead_inbox_message(lead_root, args.work_id)
    if raw is None:
        print(
            f"shop-msg read inbox: no inbox message found for "
            f"work_id={args.work_id!r} in lead={lead_root!r}",
            file=sys.stderr,
        )
        return 1
    try:
        message = _response_adapter.validate_python(raw)
    except ValidationError as e:
        print(
            f"shop-msg read inbox: validation failed for "
            f"work_id={args.work_id!r}:\n{e}",
            file=sys.stderr,
        )
        return 1
    print(f"valid {message.message_type} from {args.work_id}:")
    print(yaml.safe_dump(message.model_dump(exclude_none=True), sort_keys=False))
    return 0


def _cmd_pending_outbox(args: argparse.Namespace) -> int:
    """Enumerate pending outbox responses across sibling BC clones.

    Lead-side counterpart to `pending inbox`. Queries Postgres for
    outbox rows whose bc path sits under <lead-root>/repos/.
    """
    lead_root = _resolve_lead(args)
    rows = query_pending_outbox(lead_root, bc_filter=args.bc_name)
    for work_id, message_type, bc_name in rows:
        print(f"{work_id} {message_type} {bc_name}")
    return 0


def _cmd_consume_outbox(args: argparse.Namespace) -> int:
    """Mark a specific outbox row as consumed (lead side).

    After consumption the row no longer appears in 'pending outbox' output.
    Exits non-zero if no matching unconsumed outbox row exists.
    """
    bc_root = _resolve_bc(args)
    found = consume_outbox_message(bc_root, args.work_id, args.message_type)
    if not found:
        print(
            f"shop-msg consume outbox: no unconsumed outbox row found for "
            f"work_id={args.work_id!r} message_type={args.message_type!r} "
            f"in bc={bc_root!r}",
            file=sys.stderr,
        )
        return 1

    # lead-tuu5 scenario fcdd854bfba8f2a2 (ADR-016): the consume CLI itself
    # flips the lead bd entry dispatch_state bc_emitted -> consumed via the
    # bd_facade, under the same atomicity boundary as the messaging-layer
    # release above. The agent runs ONE command; the CLI performs both the
    # messaging action and the paired bd update — no separate agent
    # `bd update` step is required. Best-effort and scoped to a registered
    # lead with a reachable bd workspace; a bd-less invocation just performs
    # the messaging release (pre-lead-tuu5 behavior).
    lead_root_str = _resolve_registered_lead()
    if lead_root_str is not None:
        lead_root = Path(lead_root_str)
        if (
            bd_facade.bd_available(lead_root)
            and bd_facade.get_dispatch_bead(lead_root, args.work_id) is not None
        ):
            try:
                bd_facade.set_dispatch_state(
                    lead_root, args.work_id, bd_facade.STATE_CONSUMED
                )
            except bd_facade.BdFacadeError as exc:
                print(
                    f"shop-msg consume outbox: messaging release succeeded but "
                    f"bd dispatch_state flip to consumed failed: {exc}",
                    file=sys.stderr,
                )
                return 1
    return 0


def _sweep_is_stale(metadata: dict, threshold_seconds: int) -> bool:
    """Return True iff the outbox_pending bead is older than the threshold.

    Uses the ``outbox_pending_at`` ISO timestamp recorded at Step 1. When the
    timestamp is absent or unparseable, the bead is treated as stale (a bead
    stuck at outbox_pending with no timestamp is exactly the corrupt-intent
    case the sweeper exists to recover).
    """
    import datetime as _dt

    raw = metadata.get(bd_facade.KEY_OUTBOX_PENDING_AT)
    if not raw:
        return True
    try:
        ts = _dt.datetime.fromisoformat(raw)
    except ValueError:
        return True
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=_dt.timezone.utc)
    age = (_dt.datetime.now(_dt.timezone.utc) - ts).total_seconds()
    return age >= threshold_seconds


def _cmd_sweep(args: argparse.Namespace) -> int:
    """Recover lead bd entries stuck at dispatch_state=outbox_pending.

    For each stale outbox_pending bead in the shop's bd workspace, reconcile
    against the actual postgres state (lead-tuu5 scenarios 236eb236a79beb84,
    97b6112b1aedd7ae). The reconciliation rule is shop-msg-wins for "was the
    message sent" (PDR-010 decision 3): the postgres outbox row's existence
    is authoritative.

    - Deposit ALREADY landed -> bd-flip-only recovery (no duplicate row).
    - Deposit NEVER landed    -> re-deposit from the bd payload reference,
                                  then flip. The re-deposit is guarded against
                                  double-write by the postgres UNIQUE
                                  constraint (a concurrent deposit makes the
                                  retry a CollisionError, which is swallowed
                                  so the sweeper still proceeds to the flip).

    Idempotent: a second sweep finds nothing at outbox_pending and is a no-op.
    """
    shop_root_str = resolve_shop_name(args.shop)
    if shop_root_str is None:
        print(
            f"shop-msg sweep: shop name {args.shop!r} is not registered in the "
            f"registry.",
            file=sys.stderr,
        )
        return 1
    shop_root = Path(shop_root_str).resolve()
    threshold = args.threshold_seconds

    if not bd_facade.bd_available(shop_root):
        print(
            f"shop-msg sweep: no reachable bd workspace under {shop_root!r}; "
            f"nothing to sweep.",
            file=sys.stderr,
        )
        return 0

    recovered = 0
    for bead in bd_facade.list_dispatch_beads(shop_root):
        metadata = bead.get("metadata") or {}
        if metadata.get(bd_facade.KEY_DISPATCH_STATE) != bd_facade.STATE_OUTBOX_PENDING:
            continue
        if not _sweep_is_stale(metadata, threshold):
            continue
        # ADR-013 sweep/queued-dispatch interaction (lead-p0ez): a queued bead
        # carries pending_dependency AND ages into outbox_pending staleness
        # naturally. Sweep MUST NOT promote it past an open predecessor — that
        # would silently defeat ADR-013's central dependency-gating guarantee.
        # If pending_dependency is set AND that predecessor is NOT at
        # dispatch_state=closed, skip the bead entirely: no postgres deposit,
        # no bd state mutation. (A normal stuck outbox_pending bead with NO
        # pending_dependency is still swept/recovered exactly as before per
        # lead-tuu5.)
        pending_dependency = metadata.get(bd_facade.KEY_PENDING_DEPENDENCY)
        if pending_dependency:
            predecessor_state = bd_facade.predecessor_dispatch_state(
                shop_root, pending_dependency
            )
            if predecessor_state != bd_facade.STATE_CLOSED:
                continue
        work_id = bead.get("id")
        bc_name = metadata.get(bd_facade.KEY_DISPATCHED_TO_BC)
        message_type = metadata.get(bd_facade.KEY_DISPATCH_MESSAGE_TYPE)
        if not (work_id and bc_name and message_type):
            continue
        bc_root_str = resolve_shop_name(bc_name)
        if bc_root_str is None:
            print(
                f"shop-msg sweep: bead {work_id} references unregistered BC "
                f"{bc_name!r}; skipping.",
                file=sys.stderr,
            )
            continue
        bc_root = str(Path(bc_root_str).resolve())

        if not dispatch_inbox_row_exists(bc_root, work_id, message_type):
            # Deposit never landed: re-deposit from the payload reference.
            payload_ref = metadata.get(bd_facade.KEY_PAYLOAD_REF)
            if not payload_ref or not Path(payload_ref).exists():
                print(
                    f"shop-msg sweep: bead {work_id} has no usable payload "
                    f"reference to reconstruct the postgres deposit; skipping.",
                    file=sys.stderr,
                )
                continue
            try:
                payload = _load_payload_file(payload_ref, message_type, work_id)
            except (ValueError, ValidationError, OSError) as exc:
                print(
                    f"shop-msg sweep: bead {work_id} payload reconstruction "
                    f"failed: {exc}; skipping.",
                    file=sys.stderr,
                )
                continue
            try:
                insert_message(
                    bc_root, work_id, "inbox", message_type, payload, notify=True
                )
            except CollisionError:
                # A concurrent sweep already deposited; the UNIQUE constraint
                # guarded the double-write. Proceed to the flip.
                pass

        # Deposit is present (already-landed, just re-deposited, or a
        # concurrent sweep landed it): flip bd to dispatched.
        try:
            bd_facade.set_dispatch_state(
                shop_root, work_id, bd_facade.STATE_DISPATCHED
            )
        except bd_facade.BdFacadeError as exc:
            print(
                f"shop-msg sweep: bead {work_id} flip to dispatched failed: "
                f"{exc}",
                file=sys.stderr,
            )
            return 1
        recovered += 1

    print(f"shop-msg sweep: recovered {recovered} outbox_pending bead(s).")
    return 0


def _promote_one(
    shop_root: Path, bead: dict, closed_work_id: str
) -> str:
    """Attempt to promote a single queued bead whose pending_dependency is
    the just-closed predecessor. Returns one of: "promoted", "still-queued",
    "noop". Idempotent: an already-dispatched bead is a no-op.

    A queued bead is released only when ALL of its depends-on edges are at
    dispatch_state=closed (ADR-013 decision 6). When the closing predecessor
    was one of several open predecessors, the pending_dependency pointer for
    the closed one is cleared but the bead remains outbox_pending if any OTHER
    predecessor is still not closed.
    """
    metadata = bead.get("metadata") or {}
    work_id = bead.get("id")
    state = metadata.get(bd_facade.KEY_DISPATCH_STATE)
    pending = metadata.get(bd_facade.KEY_PENDING_DEPENDENCY)

    # Idempotency: only outbox_pending beads pointing at the closed predecessor
    # are candidates. An already-promoted (dispatched) bead is a no-op.
    if state != bd_facade.STATE_OUTBOX_PENDING or pending != closed_work_id:
        return "noop"

    # Re-consult ALL depends-on edges: release only if every predecessor is
    # now closed.
    unmet = bd_facade.first_unclosed_predecessor(shop_root, work_id)
    if unmet is not None:
        # Another predecessor is still open. Clear the pointer for the closed
        # one (it is satisfied) but leave the bead queued behind the remaining
        # open predecessor.
        remaining_pred, _state = unmet
        # Re-point pending_dependency at the still-open predecessor so the
        # next close of THAT predecessor re-triggers this bead.
        bd_facade._run_bd(
            [
                "update",
                work_id,
                "--set-metadata",
                f"{bd_facade.KEY_PENDING_DEPENDENCY}={remaining_pred}",
            ],
            cwd=shop_root,
        )
        return "still-queued"

    # All predecessors closed: deposit the deferred postgres row, flip to
    # dispatched, and clear pending_dependency.
    bc_name = metadata.get(bd_facade.KEY_DISPATCHED_TO_BC)
    message_type = metadata.get(bd_facade.KEY_DISPATCH_MESSAGE_TYPE)
    payload_ref = metadata.get(bd_facade.KEY_PAYLOAD_REF)
    if not (bc_name and message_type and payload_ref):
        print(
            f"shop-msg promote: queued bead {work_id} is missing a "
            f"dispatched_to_bc / dispatch_message_type / payload_ref needed to "
            f"deposit; skipping.",
            file=sys.stderr,
        )
        return "noop"
    bc_root_str = resolve_shop_name(bc_name)
    if bc_root_str is None:
        print(
            f"shop-msg promote: bead {work_id} references unregistered BC "
            f"{bc_name!r}; skipping.",
            file=sys.stderr,
        )
        return "noop"
    bc_root = str(Path(bc_root_str).resolve())
    if not Path(payload_ref).exists():
        print(
            f"shop-msg promote: bead {work_id} payload reference "
            f"{payload_ref!r} no longer exists; skipping.",
            file=sys.stderr,
        )
        return "noop"
    try:
        payload = _load_payload_file(payload_ref, message_type, work_id)
    except (ValueError, ValidationError, OSError) as exc:
        print(
            f"shop-msg promote: bead {work_id} payload reconstruction failed: "
            f"{exc}; skipping.",
            file=sys.stderr,
        )
        return "noop"
    try:
        insert_message(bc_root, work_id, "inbox", message_type, payload, notify=True)
    except CollisionError:
        # A prior promote already deposited this row (idempotency under the
        # postgres UNIQUE constraint). Proceed to ensure the bd state is
        # consistent (flip + clear) so a partially-completed prior promote is
        # finished.
        pass
    bd_facade.set_dispatch_state(shop_root, work_id, bd_facade.STATE_DISPATCHED)
    bd_facade.clear_pending_dependency(shop_root, work_id)
    return "promoted"


def _cmd_promote(args: argparse.Namespace) -> int:
    """Promote queued dispatches whose predecessor has just closed.

    The trigger event is the closure of a predecessor bead (PDR-010 /
    ADR-013): the architect runs `bd close <predecessor>` (transitioning its
    dispatch_state to closed), then this scan enumerates every bd entry with
    pending_dependency=<predecessor> and, for each whose depends-on edges are
    ALL closed, deposits the deferred postgres outbox row and flips the bead
    from outbox_pending to dispatched (decision 6 idempotency: a second
    invocation is a no-op on already-dispatched beads).

    --set-closed marks the predecessor's dispatch_state=closed as part of the
    same command (the deterministic seam the close-triggers-promote contract
    relies on), so a test does not depend on a native bd-close hook.
    """
    shop_root_str = resolve_shop_name(args.shop)
    if shop_root_str is None:
        print(
            f"shop-msg promote: shop name {args.shop!r} is not registered in "
            f"the registry.",
            file=sys.stderr,
        )
        return 1
    shop_root = Path(shop_root_str).resolve()
    if not bd_facade.bd_available(shop_root):
        print(
            f"shop-msg promote: no reachable bd workspace under {shop_root!r}; "
            f"nothing to promote.",
            file=sys.stderr,
        )
        return 0

    closed_work_id = args.closed

    # Optionally mark the predecessor's dispatch_state=closed (the architect's
    # close-step). Idempotent: setting closed when already closed is harmless.
    if getattr(args, "set_closed", False):
        if bd_facade.get_dispatch_bead(shop_root, closed_work_id) is not None:
            bd_facade.set_dispatch_state(
                shop_root, closed_work_id, bd_facade.STATE_CLOSED
            )

    promoted = 0
    for bead in bd_facade.list_dispatch_beads(shop_root):
        outcome = _promote_one(shop_root, bead, closed_work_id)
        if outcome == "promoted":
            promoted += 1
    print(
        f"shop-msg promote: closed {closed_work_id!r}; promoted {promoted} "
        f"queued dispatch(es)."
    )
    return 0


def _cmd_prime(args: argparse.Namespace) -> int:
    """Session-start orientation for BC agents.

    Prints:
      1. Current DSN
      2. DB reachability (yes / no — <error>)
      3. Count of unprocessed inbox messages
      4. Each pending work_id + message_type
      5. A brief reminder block about the CLI

    Exit 0 when DB is reachable; non-zero when unreachable.
    """
    from shop_msg.storage import _get_dsn, query_pending_inbox

    bc_root = _resolve_bc(args)
    bc_name = args.bc
    dsn = _get_dsn()

    print(f"DSN: {dsn}")

    # Probe connectivity.
    try:
        pending_rows = query_pending_inbox(bc_root)
        db_reachable = True
    except Exception as exc:
        print(f"DB reachable: no — {exc}")
        print()
        _print_prime_reminder(bc_name)
        return 1

    print("DB reachable: yes")
    print()

    count = len(pending_rows)
    print(f"Pending inbox messages: {count}")
    for work_id, message_type in pending_rows:
        print(f"  {work_id}  {message_type}")
    print()

    _print_prime_reminder(bc_name)
    return 0


def _print_prime_reminder(bc_name: str) -> None:
    print(
        "Use shop-msg CLI commands — inbox/outbox are in postgres, not on the filesystem.\n"
        "Key commands:\n"
        f"  shop-msg pending inbox --bc {bc_name}\n"
        f"  shop-msg read inbox --bc {bc_name} --work-id <id>\n"
        "  shop-msg respond clarify | work_done | mechanism_observation ..."
    )


def _cmd_prime_lead(args: argparse.Namespace) -> int:
    """Session-start orientation for lead shop agents.

    Prints:
      1. Current DSN
      2. DB reachability (yes / no — <error>)
      3. Count of unconsumed BC responses in the lead's inbox
      4. Each pending work_id + message_type
      5. A brief reminder block with key commands for the lead role

    Exit 0 when DB is reachable; non-zero when unreachable.
    """
    from shop_msg.storage import _get_dsn, query_pending_lead_inbox

    lead_root = _resolve_lead(args)
    lead_name = args.lead
    dsn = _get_dsn()

    print(f"DSN: {dsn}")

    # Probe connectivity.
    try:
        pending_rows = query_pending_lead_inbox(lead_root)
        db_reachable = True
    except Exception as exc:
        print(f"DB reachable: no — {exc}")
        print()
        _print_prime_lead_reminder(lead_name)
        return 1

    print("DB reachable: yes")
    print()

    count = len(pending_rows)
    print(f"Pending outbox responses: {count}")
    for work_id, message_type in pending_rows:
        print(f"  {work_id}  {message_type}")
    print()

    _print_prime_lead_reminder(lead_name)
    return 0


def _print_prime_lead_reminder(lead_name: str) -> None:
    print(
        "Use shop-msg CLI commands — inbox/outbox are in postgres, not on the filesystem.\n"
        "Key commands:\n"
        f"  shop-msg pending inbox --lead {lead_name}\n"
        f"  shop-msg read inbox --lead {lead_name} --work-id <id>\n"
        "  shop-msg respond clarify  # lead answers BC questions\n"
        "  shop-msg respond work_done | mechanism_observation ..."
    )


def _cmd_prime_dispatch(args: argparse.Namespace) -> int:
    """Dispatch prime to BC mode (--bc) or lead mode (--lead)."""
    if getattr(args, "lead", None):
        return _cmd_prime_lead(args)
    return _cmd_prime(args)


def _cmd_dump(args: argparse.Namespace) -> int:
    """Operator debugging: dump rows from the messages table."""
    import json as _json
    from shop_msg.storage import _connect, _bc_id

    bc_root: str | None = None
    if hasattr(args, "bc") and args.bc:
        bc_root = resolve_shop_name(args.bc)
        if bc_root is None:
            print(
                f"shop-msg dump: shop name {args.bc!r} not registered in registry",
                file=sys.stderr,
            )
            return 1
        bc_root = str(Path(bc_root).resolve())
    direction = args.direction
    limit = args.limit or 100

    with _connect() as conn:
        with conn.cursor() as cur:
            conditions = []
            params: list = []
            if bc_root is not None:
                conditions.append("bc = %s")
                params.append(_bc_id(bc_root))
            if direction is not None:
                conditions.append("direction = %s")
                params.append(direction)
            where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
            cur.execute(
                f"SELECT id, bc, work_id, direction, message_type, payload, created_at "
                f"FROM messages {where} ORDER BY created_at LIMIT %s",
                params + [limit],
            )
            rows = cur.fetchall()

    for row in rows:
        print(yaml.safe_dump(dict(row), sort_keys=False, default_flow_style=False))
        print("---")
    return 0


def _cmd_watch(args: argparse.Namespace) -> int:
    """Drain pending messages then LISTEN for new ones.

    Each line emitted to stdout is of the form:
        <work_id> <message_type>

    A special "READY" sentinel line is emitted after the startup drain and
    before entering the LISTEN loop so that callers (e.g. the Monitor harness
    or BDD step definitions) can reliably detect when the drain phase is done.

    Exits non-zero with a descriptive message to stderr if the database is
    unreachable at startup.

    Modes:
      --bc <name>    Inbox watcher for a single BC (resolves via registry).
      --lead <name>  Outbox watcher across all BCs under the lead root.
    """
    try:
        if hasattr(args, "lead") and args.lead is not None:
            lead_root = _resolve_lead(args)
            watch_lead_inbox(lead_root)
            return 0

        bc_root = _resolve_bc(args)
        watch_inbox(bc_root)
        return 0
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 1


def _cmd_bc_status(args: argparse.Namespace) -> int:
    """Report presence classification of BCs from the bc_presence heartbeat table.

    (PDR-010 / ADR-014.) Each BC is classified by the age of its most recent
    heartbeat: online (<90s), stale ([90s,300s)), offline (>=300s). A BC with no
    heartbeat row is reported offline with no last_seen_at (fail-safe rollout-window
    posture: never observed alive => offline).

    With --bc <name>, reports exactly that one BC (and synthesises an offline row
    when it has never been watched). Without it, reports every BC with a presence
    row, ordered by name.

    Output: one line per BC of the form
        <bc_name> <classification> <seconds_since_last_seen>
    where the age is rendered as an integer second count, or "-" when never seen.
    """
    bc_name = getattr(args, "bc", None)
    rows = presence_status(bc_name)
    for row in rows:
        age = row["seconds_since_last_seen"]
        age_str = "-" if age is None else str(int(round(age)))
        print(f"{row['bc_name']} {row['classification']} {age_str}")
    return 0


def _cmd_registry_add(args: argparse.Namespace) -> int:
    """Register a shop by canonical name."""
    shop_type = "lead" if getattr(args, "lead_shop", False) else "bc"
    registry_add(args.name, args.shop_root, shop_type=shop_type)
    return 0


def _cmd_registry_remove(args: argparse.Namespace) -> int:
    """Remove a shop from the registry by canonical name."""
    found = registry_remove(args.name)
    if not found:
        print(
            f"shop-msg registry remove: {args.name!r} was not found in the registry",
            file=sys.stdout,
        )
    return 0


def _cmd_registry_list(args: argparse.Namespace) -> int:
    """List all registered shops."""
    entries = registry_list()
    for name, shop_root, shop_type in entries:
        print(f"{name} {shop_root} {shop_type}")
    return 0


def _cmd_registry_sync(args: argparse.Namespace) -> int:
    """Synchronise the registry from a BC manifest file."""
    registry_sync(args.manifest)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="shop-msg")
    sub = parser.add_subparsers(dest="command", required=True)

    respond = sub.add_parser("respond", help="write a BC response message")
    respond_sub = respond.add_subparsers(dest="response_type", required=True)

    clarify = respond_sub.add_parser("clarify", help="write a clarify response")
    clarify.add_argument(
        "--bc",
        default=None,
        help=(
            "canonical BC name (resolved via registry). Optional: when "
            "omitted, the BC name is resolved from CWD via .claude/shop/ "
            "marker walk-up (PDR-008)."
        ),
    )
    _add_removed_flag(clarify, "--bc-root")
    clarify.add_argument("--work-id", required=True, help="work_id from the lead message")
    clarify.add_argument("--question", required=True, help="clarifying question text")
    clarify.add_argument(
        "--force",
        action="store_true",
        help=(
            "replace an existing same-message_type lead-inbox response for this "
            "work_id (recovery path; lead-2id). Without --force the command "
            "refuses on collision."
        ),
    )
    clarify.set_defaults(func=_cmd_respond_clarify, _cwd_resolves=True)

    work_done = respond_sub.add_parser("work_done", help="write a work_done response")
    work_done.add_argument(
        "--bc",
        default=None,
        help=(
            "canonical BC name (resolved via registry). Optional: when "
            "omitted, the BC name is resolved from CWD via .claude/shop/ "
            "marker walk-up (PDR-008)."
        ),
    )
    _add_removed_flag(work_done, "--bc-root")
    work_done.add_argument("--work-id", required=True, help="work_id from the lead message")
    work_done.add_argument(
        "--status",
        required=True,
        choices=["complete", "partial", "blocked"],
        help="work_done status",
    )
    work_done.add_argument(
        "--scenario-hash",
        action="append",
        default=None,
        help="scenario hash echoed back to the lead (repeatable)",
    )
    work_done.add_argument(
        "--summary",
        default=None,
        help="optional free-form summary of what was done",
    )
    work_done.add_argument(
        "--force",
        action="store_true",
        help=(
            "replace an existing same-message_type lead-inbox response for this "
            "work_id (recovery path; lead-2id). Without --force the command "
            "refuses on collision."
        ),
    )
    work_done.set_defaults(func=_cmd_respond_work_done, _cwd_resolves=True)

    mech_obs = respond_sub.add_parser(
        "mechanism_observation",
        help="surface a BC observation about the shop-system mechanism",
    )
    mech_obs.add_argument(
        "--bc",
        default=None,
        help=(
            "canonical BC name (resolved via registry). Optional: when "
            "omitted, the BC name is resolved from CWD via .claude/shop/ "
            "marker walk-up (PDR-008)."
        ),
    )
    _add_removed_flag(mech_obs, "--bc-root")
    mech_obs.add_argument(
        "--work-id", required=True,
        help=(
            "work_id naming this response in the BC's outbox. Mirrors the "
            "respond clarify / respond work_done flag of the same name."
        ),
    )
    mech_obs.add_argument(
        "--subject", required=True,
        help="one-line summary of the mechanism observation",
    )
    mech_obs.add_argument(
        "--body", required=True,
        help="markdown body: what was observed and why it's load-bearing",
    )
    mech_obs.add_argument(
        "--observed-during", default=None,
        help="lead-issued work_id the BC was working when this surfaced (optional)",
    )
    mech_obs.add_argument(
        "--evidence", action="append", default=None,
        help="verifiable pointer (file:line, template ref, package name); repeatable",
    )
    mech_obs.add_argument(
        "--proposed-action", default=None,
        help="BC's hypothesis for what to change (optional)",
    )
    mech_obs.add_argument(
        "--provenance-ref", default=None,
        help=(
            "optional, tracker-neutral pointer to a BC-side record where "
            "long-form analysis lives (e.g. an issue id, a doc path). The "
            "wire schema does not constrain this to any particular "
            "tracker (lead-231 item C)."
        ),
    )
    mech_obs.add_argument(
        "--force",
        action="store_true",
        help=(
            "replace an existing same-message_type lead-inbox response for this "
            "work_id (recovery path; lead-2id). Without --force the command "
            "refuses on collision."
        ),
    )
    mech_obs.set_defaults(
        func=_cmd_respond_mechanism_observation, _cwd_resolves=True
    )

    send = sub.add_parser("send", help="write a lead-to-BC message into a BC's inbox")
    send_sub = send.add_subparsers(dest="message_type", required=True)

    request_maintenance = send_sub.add_parser(
        "request_maintenance", help="write a request_maintenance message"
    )
    request_maintenance.add_argument("--bc", required=True, help="canonical BC name (resolved via registry)")
    _add_removed_flag(request_maintenance, "--bc-root")
    request_maintenance.add_argument(
        "--work-id", required=True, help="work_id identifying this assignment"
    )
    request_maintenance.add_argument(
        "--description",
        default=None,
        help="description of the work being requested (required unless --payload)",
    )
    request_maintenance.add_argument(
        "--acceptance-criterion",
        action="append",
        default=None,
        help="measurable acceptance criterion (repeatable)",
    )
    request_maintenance.add_argument(
        "--file-hint",
        action="append",
        default=None,
        help="file path hint relevant to the work (repeatable)",
    )
    request_maintenance.add_argument(
        "--payload",
        default=None,
        help=(
            "path to a YAML/JSON file pinning the complete message body; "
            "when supplied it is the authoritative content source and the "
            "per-field flags are not required"
        ),
    )
    request_maintenance.add_argument(
        "--depends-on",
        default=None,
        help=(
            "work_id of a prior dispatch this one depends on; recorded on the "
            "lead bd entry as depends_on_dispatch metadata"
        ),
    )
    request_maintenance.add_argument(
        "--queue-on-dependency",
        action="store_true",
        help=(
            "when a bd depends-on predecessor is not yet at "
            "dispatch_state=closed, defer the postgres deposit: write a queued "
            "bd entry (dispatch_state=outbox_pending, pending_dependency=<pred>) "
            "instead of refusing. The deposit lands via `shop-msg promote` once "
            "the predecessor closes (PDR-010 / ADR-013 decision 4)"
        ),
    )
    request_maintenance.set_defaults(func=_cmd_send_request_maintenance)

    assign_scenarios = send_sub.add_parser(
        "assign_scenarios", help="write an assign_scenarios message"
    )
    assign_scenarios.add_argument("--bc", required=True, help="canonical BC name (resolved via registry)")
    _add_removed_flag(assign_scenarios, "--bc-root")
    assign_scenarios.add_argument(
        "--work-id", required=True, help="work_id identifying this assignment"
    )
    assign_scenarios.add_argument(
        "--feature-title",
        default=None,
        help=(
            "title used in the wrapping `Feature:` line for each scenario; "
            "required unless --payload is supplied"
        ),
    )
    assign_scenarios.add_argument(
        "--bc-tag",
        default=None,
        help=(
            "BC name used in the @bc:<name> scenario tag; required unless "
            "--payload is supplied"
        ),
    )
    assign_scenarios.add_argument(
        "--scenario-file",
        action="append",
        default=None,
        help=(
            "path to a file containing one scenario body (repeatable); "
            "required unless --payload is supplied"
        ),
    )
    assign_scenarios.add_argument(
        "--payload",
        default=None,
        help=(
            "path to a YAML/JSON file pinning the complete assign_scenarios "
            "message body (scenarios with pre-computed hashes); the "
            "authoritative content source when supplied"
        ),
    )
    assign_scenarios.add_argument(
        "--depends-on",
        default=None,
        help="work_id of a prior dispatch this one depends on (bd metadata)",
    )
    assign_scenarios.add_argument(
        "--queue-on-dependency",
        action="store_true",
        help=(
            "when a bd depends-on predecessor is not yet at "
            "dispatch_state=closed, defer the postgres deposit and write a "
            "queued bd entry instead of refusing (PDR-010 / ADR-013 decision 4)"
        ),
    )
    assign_scenarios.set_defaults(func=_cmd_send_assign_scenarios)

    request_bugfix = send_sub.add_parser(
        "request_bugfix", help="write a request_bugfix message"
    )
    request_bugfix.add_argument("--bc", required=True, help="canonical BC name (resolved via registry)")
    _add_removed_flag(request_bugfix, "--bc-root")
    request_bugfix.add_argument(
        "--work-id", required=True, help="work_id identifying this assignment"
    )
    request_bugfix.add_argument(
        "--description",
        default=None,
        help="plain-language description of the fix (required unless --payload)",
    )
    request_bugfix.add_argument(
        "--feature-title",
        default=None,
        help=(
            "title used in the wrapping `Feature:` line for each scenario; "
            "required iff at least one --scenario-file is supplied"
        ),
    )
    request_bugfix.add_argument(
        "--bc-tag",
        default=None,
        help=(
            "BC name used in the @bc:<name> scenario tag; required iff "
            "at least one --scenario-file is supplied"
        ),
    )
    request_bugfix.add_argument(
        "--scenario-file",
        action="append",
        default=None,
        help="path to a file containing one scenario body (repeatable, optional)",
    )
    request_bugfix.add_argument(
        "--payload",
        default=None,
        help=(
            "path to a YAML/JSON file pinning the complete request_bugfix "
            "message body (scenarios with pre-computed hashes); the "
            "authoritative content source when supplied"
        ),
    )
    request_bugfix.add_argument(
        "--depends-on",
        default=None,
        help="work_id of a prior dispatch this one depends on (bd metadata)",
    )
    request_bugfix.add_argument(
        "--queue-on-dependency",
        action="store_true",
        help=(
            "when a bd depends-on predecessor is not yet at "
            "dispatch_state=closed, defer the postgres deposit and write a "
            "queued bd entry instead of refusing (PDR-010 / ADR-013 decision 4)"
        ),
    )
    request_bugfix.set_defaults(func=_cmd_send_request_bugfix)

    read = sub.add_parser("read", help="read a message from a BC's mailboxes")
    read_sub = read.add_subparsers(dest="read_target", required=True)

    read_outbox = read_sub.add_parser(
        "outbox", help="read and validate a BC response from its outbox"
    )
    read_outbox.add_argument(
        "--bc",
        default=None,
        help=(
            "canonical BC name (resolved via registry). Optional: when "
            "omitted, the BC name is resolved from CWD via .claude/shop/ "
            "marker walk-up (PDR-008)."
        ),
    )
    _add_removed_flag(read_outbox, "--bc-root")
    read_outbox.add_argument(
        "--work-id", required=True, help="work_id whose response to read"
    )
    read_outbox.set_defaults(func=_cmd_read_outbox, _cwd_resolves=True)

    read_inbox = read_sub.add_parser(
        "inbox",
        help=(
            "read and validate a message from a BC's inbox (--bc) "
            "or a BC response from the lead's inbox (--lead)"
        ),
    )
    # The mutually-exclusive group is *not required* at parse time so that
    # bare invocations (PDR-008) can populate one of the flags via CWD
    # walk-up before dispatch. argparse still enforces that both cannot be
    # set simultaneously.
    _read_inbox_mode = read_inbox.add_mutually_exclusive_group(required=False)
    _read_inbox_mode.add_argument(
        "--bc",
        default=None,
        help=(
            "canonical BC name (BC-side inbox mode). Optional: when neither "
            "--bc nor --lead is supplied, the shop is resolved from CWD via "
            ".claude/shop/ marker walk-up (PDR-008)."
        ),
    )
    _read_inbox_mode.add_argument(
        "--lead",
        default=None,
        help=(
            "canonical lead shop name (lead-side inbox mode). Optional: see "
            "--bc help above."
        ),
    )
    _add_removed_flag(read_inbox, "--bc-root")
    read_inbox.add_argument(
        "--work-id", required=True, help="work_id whose inbox message to read"
    )

    def _dispatch_read_inbox(args: argparse.Namespace) -> int:
        if args.lead is not None:
            return _cmd_read_lead_inbox(args)
        return _cmd_read_inbox(args)

    read_inbox.set_defaults(func=_dispatch_read_inbox, _cwd_resolves=True)

    pending = sub.add_parser(
        "pending", help="enumerate pending mailbox entries (queries, not gates)"
    )
    pending_sub = pending.add_subparsers(dest="pending_target", required=True)

    pending_inbox = pending_sub.add_parser(
        "inbox",
        help=(
            "list inbox messages with no matching outbox response (BC side: --bc); "
            "or list BC responses in the lead's inbox (lead side: --lead)"
        ),
    )
    # Not required at parse time — bare invocations resolve via CWD walk-up
    # (PDR-008). argparse still enforces that both cannot be set together.
    _pending_inbox_mode = pending_inbox.add_mutually_exclusive_group(required=False)
    _pending_inbox_mode.add_argument(
        "--bc",
        default=None,
        help=(
            "canonical BC name (BC-side inbox mode). Optional: when neither "
            "--bc nor --lead is supplied, the shop is resolved from CWD via "
            ".claude/shop/ marker walk-up (PDR-008)."
        ),
    )
    _pending_inbox_mode.add_argument(
        "--lead",
        default=None,
        help=(
            "canonical lead shop name (lead-side inbox mode). Optional: see "
            "--bc help above."
        ),
    )
    _add_removed_flag(pending_inbox, "--bc-root")

    def _dispatch_pending_inbox(args: argparse.Namespace) -> int:
        if args.lead is not None:
            return _cmd_pending_lead_inbox(args)
        return _cmd_pending_inbox(args)

    pending_inbox.set_defaults(func=_dispatch_pending_inbox, _cwd_resolves=True)

    pending_outbox = pending_sub.add_parser(
        "outbox",
        help="list pending outbox responses across sibling BC clones (lead side)",
    )
    pending_outbox.add_argument(
        "--lead",
        default=None,
        help=(
            "canonical lead shop name (resolved via registry to lead root "
            "path). Optional: when omitted, the lead shop is resolved from "
            "CWD via .claude/shop/ marker walk-up (PDR-008)."
        ),
    )
    _add_removed_flag(pending_outbox, "--lead-root")
    pending_outbox.add_argument(
        "--bc-name",
        default=None,
        dest="bc_name",
        help="restrict to a single BC by canonical name",
    )
    pending_outbox.set_defaults(func=_cmd_pending_outbox, _cwd_resolves=True)

    consume = sub.add_parser(
        "consume",
        help="mark a mailbox entry as consumed (lead side)",
    )
    consume_sub = consume.add_subparsers(dest="consume_target", required=True)

    consume_outbox = consume_sub.add_parser(
        "outbox",
        help=(
            "mark a specific outbox row as consumed so it no longer appears "
            "in 'pending outbox' output"
        ),
    )
    consume_outbox.add_argument(
        "--bc",
        required=True,
        help="canonical BC name (resolved via registry)",
    )
    _add_removed_flag(consume_outbox, "--bc-root")
    consume_outbox.add_argument(
        "--work-id",
        required=True,
        help="work_id of the outbox row to consume",
    )
    consume_outbox.add_argument(
        "--message-type",
        required=True,
        choices=["clarify", "work_done", "mechanism_observation"],
        help="message_type of the outbox row to consume",
    )
    consume_outbox.set_defaults(func=_cmd_consume_outbox)

    sweep = sub.add_parser(
        "sweep",
        help=(
            "recover lead bd entries stuck at dispatch_state=outbox_pending by "
            "reconciling against the actual postgres outbox state"
        ),
    )
    sweep.add_argument(
        "--shop",
        required=True,
        help="canonical shop name whose bd workspace to sweep (resolved via registry)",
    )
    sweep.add_argument(
        "--threshold-seconds",
        type=int,
        default=bd_facade.DEFAULT_SWEEP_THRESHOLD_SECONDS,
        help=(
            "only sweep outbox_pending beads older than this many seconds "
            f"(default {bd_facade.DEFAULT_SWEEP_THRESHOLD_SECONDS})"
        ),
    )
    sweep.set_defaults(func=_cmd_sweep)

    promote = sub.add_parser(
        "promote",
        help=(
            "promote queued dispatches whose predecessor has just closed: "
            "deposit the deferred postgres outbox row and flip the bd entry "
            "from outbox_pending to dispatched (PDR-010 / ADR-013)"
        ),
    )
    promote.add_argument(
        "--shop",
        required=True,
        help=(
            "canonical shop name whose bd workspace holds the queued entries "
            "(resolved via registry)"
        ),
    )
    promote.add_argument(
        "--closed",
        required=True,
        help="work_id of the predecessor that just closed (the promote trigger)",
    )
    promote.add_argument(
        "--set-closed",
        action="store_true",
        help=(
            "also mark the predecessor's dispatch_state=closed as part of this "
            "command (the architect's close-step)"
        ),
    )
    promote.set_defaults(func=_cmd_promote)

    prime = sub.add_parser(
        "prime",
        help="session-start orientation: DSN, DB health, pending inbox, and CLI reminder",
    )
    # Not required at parse time — bare invocations resolve via CWD walk-up
    # (PDR-008). argparse still enforces that both cannot be set together.
    _prime_mode = prime.add_mutually_exclusive_group(required=False)
    _prime_mode.add_argument(
        "--bc",
        default=None,
        help=(
            "canonical BC name (resolved via registry). Optional: when "
            "neither --bc nor --lead is supplied, the shop is resolved "
            "from CWD via .claude/shop/ marker walk-up (PDR-008)."
        ),
    )
    _prime_mode.add_argument(
        "--lead",
        default=None,
        help=(
            "canonical lead shop name (lead mode). Optional: see --bc "
            "help above."
        ),
    )
    _add_removed_flag(prime, "--bc-root")
    prime.set_defaults(func=_cmd_prime_dispatch, _cwd_resolves=True)

    dump = sub.add_parser(
        "dump",
        help="operator debugging: dump messages table rows to stdout as YAML",
    )
    dump.add_argument("--bc", default=None, help="restrict to this BC (canonical name)")
    _add_removed_flag(dump, "--bc-root")
    dump.add_argument(
        "--direction",
        default=None,
        choices=["inbox", "outbox"],
        help="restrict to inbox or outbox rows",
    )
    dump.add_argument(
        "--limit",
        type=int,
        default=100,
        help="maximum number of rows to return (default 100)",
    )
    dump.set_defaults(func=_cmd_dump)

    watch = sub.add_parser(
        "watch",
        help=(
            "Monitor-compatible watcher: drain pending messages then "
            "LISTEN for new ones, printing one '<work_id> <message_type>' "
            "line per event to stdout. Never exits unless the DB is unreachable. "
            "Use --bc <name> for inbox watching (BC mode) or --lead <name> for "
            "outbox watching across all BCs (lead mode)."
        ),
    )
    # Not required at parse time — bare invocations resolve via CWD walk-up
    # (PDR-008). argparse still enforces that both cannot be set together.
    _watch_mode = watch.add_mutually_exclusive_group(required=False)
    _watch_mode.add_argument(
        "--bc",
        default=None,
        help=(
            "canonical BC name (inbox watch mode). Optional: when neither "
            "--bc nor --lead is supplied, the shop is resolved from CWD via "
            ".claude/shop/ marker walk-up (PDR-008)."
        ),
    )
    _watch_mode.add_argument(
        "--lead",
        default=None,
        help=(
            "canonical lead shop name (outbox watch mode). Optional: see "
            "--bc help above."
        ),
    )
    # Register removed flags on watch but they cannot be in the mutex group;
    # they're handled via the action which exits immediately.
    _add_removed_flag(watch, "--bc-root")
    _add_removed_flag(watch, "--lead-root")
    watch.set_defaults(func=_cmd_watch, _cwd_resolves=True)

    # bc-status: presence classification from the bc_presence heartbeat table
    # (PDR-010 / ADR-014). The lead's session-start drain calls this to surface
    # offline BCs before accepting user work.
    bc_status = sub.add_parser(
        "bc-status",
        help=(
            "report BC presence (online/stale/offline) from the heartbeat "
            "table; the lead's session-start liveness check"
        ),
    )
    bc_status.add_argument(
        "--bc",
        default=None,
        help=(
            "canonical BC name to report just that BC's status (synthesises an "
            "offline row when never watched). Omit to report all BCs with a "
            "presence row."
        ),
    )
    bc_status.set_defaults(func=_cmd_bc_status)

    # Registry subcommand
    registry = sub.add_parser(
        "registry",
        help="manage the shop name registry (name-based addressing)",
    )
    registry_sub = registry.add_subparsers(dest="registry_cmd", required=True)

    registry_add_cmd = registry_sub.add_parser(
        "add",
        help="register a shop by canonical name",
    )
    registry_add_cmd.add_argument("name", help="canonical shop name")
    registry_add_cmd.add_argument(
        "shop_root",
        help="filesystem path to the shop root directory",
    )
    registry_add_cmd.add_argument(
        "--lead-shop",
        action="store_true",
        default=False,
        help="register as a lead shop (shop_type=lead) instead of a BC",
    )
    registry_add_cmd.set_defaults(func=_cmd_registry_add)

    registry_remove_cmd = registry_sub.add_parser(
        "remove",
        help="remove a shop from the registry by canonical name",
    )
    registry_remove_cmd.add_argument("name", help="canonical shop name to remove")
    registry_remove_cmd.set_defaults(func=_cmd_registry_remove)

    registry_list_cmd = registry_sub.add_parser(
        "list",
        help="list all registered shops",
    )
    registry_list_cmd.set_defaults(func=_cmd_registry_list)

    registry_sync_cmd = registry_sub.add_parser(
        "sync",
        help="synchronise the registry from a BC manifest file",
    )
    registry_sync_cmd.add_argument(
        "manifest",
        help="path to YAML/JSON manifest with 'bcs' mapping of name -> root path",
    )
    registry_sync_cmd.set_defaults(func=_cmd_registry_sync)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    # Subcommands that opt in to CWD-implicit shop resolution mark themselves
    # via set_defaults(_cwd_resolves=True). When neither --bc nor --lead was
    # given on such a subcommand, walk up from CWD to find the invoking
    # shop's .claude/shop/ marker and populate the appropriate flag
    # (PDR-008). Subcommands that do not opt in (registry, dump, send) are
    # untouched by this pass.
    if getattr(args, "_cwd_resolves", False):
        _apply_cwd_resolution(args)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
