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
        each scenario is computed in-process via
        scenarios.hash.compute_scenario_hash (the canonicalization rule
        lives in the scenarios package, not here).
    send request_bugfix --bc-root PATH --work-id ID --description TEXT
                        [--feature-title TEXT --bc-tag NAME
                         --scenario-file PATH ...]
        Writes a RequestBugfix message to the Postgres messages table.
        Scenarios are optional.
    read outbox --bc PATH --work-id ID [--message-type TYPE]
        Reads outbox rows for a work_id from Postgres, validates each against
        the BCResponse union, and dumps the canonical YAML to stdout. The
        outbox is keyed by (work_id, message_type), so multiple rows can
        coexist under one work_id (e.g. a work_done AND a later
        mechanism_observation). With no --message-type, EVERY coexisting row
        is surfaced (created_at order, oldest first); --message-type narrows
        to that single row. Exits non-zero (with a stderr message) when no
        outbox row matches the work_id (or the --message-type), or validation
        fails.
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
import os
import subprocess
import sys
import typing
from pathlib import Path

import yaml

from pydantic import TypeAdapter, ValidationError

from catalog.schemas import (
    AssignScenarios,
    BCResponse,
    Clarify,
    ClarifyResponse,
    LeadMessage,
    MechanismObservation,
    Nudge,
    RequestBugfix,
    RequestCompletionJournal,
    RequestCompletionJournalResponse,
    RequestMaintenance,
    RequestScenarioRegister,
    RegisterNarrowing,
    ScenarioPayload,
    WorkDone,
    _nudge_payload_rejects_scenario_state,
)
from scenarios.hash import compute_scenario_hash as _canonical_scenario_hash
from shop_msg import bd_facade
from shop_msg.storage import (
    CollisionError,
    OutboxDepositError,
    consume_lead_inbox_message,
    consume_outbox_message,
    delete_bc_messages,
    dispatch_inbox_row_exists,
    existing_lead_inbox_message_type,
    inbox_row_exists,
    insert_bc_response,
    insert_message,
    insert_nudge,
    insert_raw_payload,
    outbox_row_exists,
    presence_status,
    query_pending_inbox,
    query_pending_lead_inbox,
    query_pending_outbox,
    read_inbox_message,
    read_lead_inbox_message,
    read_outbox_messages,
    retract_inbox_message,
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

# The BC->lead OUTBOX-response message_type set, derived authoritatively from
# the catalog's BCResponse union so it tracks the schema and auto-includes any
# future BC->lead `*_response` type (lead-ay7j). A `nudge` is auxiliary
# both-directions signaling stored at direction='nudge' (OUTSIDE the outbox
# partial unique index) — it is never deposited as a direction='outbox' marker
# and so is never surfaced by `pending outbox` nor drainable by `consume
# outbox`; it is therefore excluded from the consume-outbox enum. The remaining
# members (clarify, work_done, mechanism_observation,
# request_completion_journal_response, and any sibling BC->lead `*_response`)
# are the consumable outbox responses.
_CONSUME_OUTBOX_MESSAGE_TYPES = [
    typing.get_args(member.model_fields["message_type"].annotation)[0]
    for member in typing.get_args(BCResponse)
    if member is not Nudge
]

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


def _refuse_lead_side_respond(verb: str) -> int | None:
    """Refuse `shop-msg respond <verb>` when the CALLER is a lead shop.

    Worldview A (ADR-018 / 05-inter-shop-protocol.md §5.3): `shop-msg
    respond` is a BC->lead vehicle ONLY. There is no lead->BC `respond`
    row. When the CALLER's shop (resolved by CWD walk-up to the nearest
    `.claude/shop/type.md`) is a `lead`, every respond sub-verb
    (clarify / work_done / mechanism_observation) is refused with a
    non-zero exit and a stderr message directing the lead to the
    lead->BC vehicles.

    Keys on the CALLER (CWD), never on the target `--bc`. Returns an
    exit code (1) when the caller is a confirmed lead — the sub-verb
    must return it immediately. Returns None otherwise (BC caller, or
    no/partial marker, or the resolver could not confirm a lead), in
    which case the sub-verb proceeds normally. The guard NEVER crashes
    the command on a missing/partial marker: it only REFUSES on a
    confirmed `lead` caller, so a BC-side respond (caller type `bc`)
    and the no-marker case both fall through untouched.
    """
    try:
        _name, shop_type = _walk_up_resolve_shop()
    except SystemExit:
        # No marker, partial marker, or an unrecognized shop type: the
        # resolver could not confirm a `lead` caller. Do not refuse —
        # and do not let the resolver's own non-zero exit escape, since
        # that would crash an otherwise-legitimate BC-side respond run
        # from a directory without a marker. Fall through.
        return None
    if shop_type == "lead":
        print(
            f"shop-msg respond {verb}: refused — 'shop-msg respond' is a "
            f"BC->lead vehicle only; a lead shop does not 'respond'. The "
            f"lead->BC vehicles are: 'shop-msg send' (assign_scenarios | "
            f"request_bugfix | request_maintenance | request_scenario_register "
            f"| request_shop_card), 'shop-msg nudge', and 'shop-msg consume'. "
            f"To answer a BC clarify, RE-DISPATCH on a fresh lead bead (e.g. "
            f"'shop-msg send request_bugfix ...'); the lead does not answer a "
            f"clarify with 'respond'.",
            file=sys.stderr,
        )
        return 1
    return None


def _apply_cwd_resolution(args: argparse.Namespace) -> None:
    """Populate args.bc or args.lead from CWD if neither was given.

    Used by the bare-invocation surface (``shop-msg prime / pending / read /
    respond / watch``): when neither addressing flag is supplied, walk up
    from CWD to find the invoking shop's .claude/shop/ marker, then
    populate the appropriate args attribute based on the shop type read
    from type.md (PDR-008).

    No-op when one of the flags is already set (explicit-flag precedence).

    Slug-form fallback (lead-t8v8 scenario 49): the literal name read from
    ``.claude/shop/name.md`` may differ from the registered canonical name
    only by literal spaces where the canonical form uses hyphens (e.g.
    ``"shopsystem product"`` on disk vs the registered ``"shopsystem-product"``).
    When the literal form does not match a registered canonical name but its
    slug form (literal spaces replaced with hyphens) does, the slug form is
    used to resolve identity and a one-line normalization advisory is written
    to stderr (composes with lead-yi0k canonical-name source-of-truth:
    surface the drift, do not silently accept it).
    """
    bc = getattr(args, "bc", None)
    lead = getattr(args, "lead", None)
    if bc is not None or lead is not None:
        return
    name, shop_type = _walk_up_resolve_shop()

    # Record that this invocation's addressing was derived from the CWD
    # walk-up (vs an explicit --bc/--lead flag). prime uses this to
    # warn-and-continue rather than hard-exit when a CWD-derived name does
    # not resolve against the registry (lead-t8v8 scenario 48).
    args._cwd_derived = True
    args._cwd_derived_literal_name = name

    resolved_name = name
    if resolve_shop_name(name) is None:
        slug = name.replace(" ", "-")
        if slug != name and resolve_shop_name(slug) is not None:
            print(
                f"shop-msg: CWD-derived shop name {name!r} was normalized to "
                f"slug {slug!r} to resolve against the registry.",
                file=sys.stderr,
            )
            resolved_name = slug

    if shop_type == "lead":
        args.lead = resolved_name
    else:
        args.bc = resolved_name


def _resolve_bc(args: argparse.Namespace) -> str:
    """Resolve --bc <name> to the BC's abstract address via the registry.

    ADR-020 / PDR-007 Option A: the registry stores no filesystem path, so
    resolution yields the BC's abstract address (``<system>/<name>``), which
    is the value threaded through the storage layer as the messages
    ``bc``/``to`` column key. Exits non-zero if the name is not registered.
    """
    name = args.bc
    address = resolve_shop_name(name)
    if address is None:
        print(
            f"shop-msg: shop name {name!r} is not registered in the registry. "
            f"Run 'shop-msg registry add' to register it first.",
            file=sys.stderr,
        )
        sys.exit(1)
    return address


def _resolve_lead(args: argparse.Namespace) -> str:
    """Resolve --lead <name> to the lead's abstract address via the registry.

    ADR-020: the lead collapses to the sentinel abstract address
    ``<system>/lead``. Exits non-zero if the name is not registered.
    """
    name = args.lead
    address = resolve_shop_name(name)
    if address is None:
        print(
            f"shop-msg: lead name {name!r} is not registered in the registry. "
            f"Run 'shop-msg registry add' to register it first.",
            file=sys.stderr,
        )
        sys.exit(1)
    return address


def _invoking_bd_context() -> Path:
    """Return the local invoking CWD as the bd working directory (ADR-020).

    The registry stores no filesystem path, so a name-addressed shop-msg
    operation resolves its bd context from the LOCAL invoking CWD (the
    ``.beads`` workspace is discovered by walk-up from here). Returning the
    CWD — never a registry-stored path — is what keeps name-addressed ops
    from raising FileNotFoundError / NotADirectoryError off a registry path
    that no longer exists (scenario f9910cf40291768c).

    Test/operator seam: ``SHOPMSG_BD_CONTEXT``, when set, overrides the CWD
    as the bd-context root. In production an agent invokes shop-msg from
    within its own shop directory, so the CWD is the bd workspace; the
    override lets a harness that invokes the CLI from a neutral directory
    still point bd at the correct shop workspace, exactly as the
    pre-ADR-020 registry-path resolution did.
    """
    override = os.environ.get("SHOPMSG_BD_CONTEXT")
    if override:
        return Path(override)
    return Path.cwd()


def _bc_clone_context() -> Path:
    """Return the directory to read the BC's origin/main HEAD from (ADR-020).

    The registry stores no BC path, so the BC's ``bc_origin_main_commit`` is
    read from the ``SHOPMSG_BC_CLONE`` override when set (the BC's local clone
    on whatever host actually holds it), falling back to the invoking CWD.
    Best-effort: when neither carries a git clone the commit is simply None.
    """
    override = os.environ.get("SHOPMSG_BC_CLONE")
    if override:
        return Path(override)
    return _invoking_bd_context()


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
    """Return the registered lead shop's abstract address, or None.

    ADR-020: yields the sentinel abstract address ``<system>/lead`` (no
    filesystem path). Used by ``respond`` commands to route responses to the
    lead's inbox namespace.
    """
    return resolve_lead_shop()


def _compute_scenario_hash(gherkin_body: str) -> str:
    """Canonicalize and hash a scenario body in-process.

    Delegates to ``scenarios.hash.compute_scenario_hash`` directly rather
    than shelling out to the ``scenarios`` binary. The canonicalization
    rule belongs to the scenarios package, which this distribution already
    Requires; importing it in-process makes the CLI's computed hash and the
    catalog ``ScenarioPayload`` validator (which delegates to the same
    function) agree by construction, and removes the PATH dependency that
    a ``subprocess.run(["scenarios", "hash"])`` shell-out carried (ADR-019,
    defect lead-pw41(b)).
    """
    return _canonical_scenario_hash(gherkin_body)


def _render_message_yaml(message) -> str:
    """Render a validated message model to YAML for the `read` commands.

    Uses an effectively-unbounded line width so multi-line string fields —
    notably a scenario's ``gherkin`` block — are not folded mid-line. Folding
    used to be harmless when the gherkin began with a short ``Feature:``
    header, but after lead-pw41 the gherkin block leads with its
    ``@scenario_hash: @bc:`` tag line (scenario-block-only canonical form),
    and default 80-column folding would split a ``Scenario:`` line across
    YAML continuation lines, breaking the readability the `read inbox`
    contract depends on. This affects display only; stored content, the wire
    format, and computed hashes are untouched.
    """
    return yaml.safe_dump(
        message.model_dump(exclude_none=True),
        sort_keys=False,
        width=float("inf"),
    )


def _cmd_respond_clarify(args: argparse.Namespace) -> int:
    refusal = _refuse_lead_side_respond("clarify")
    if refusal is not None:
        return refusal

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

    ADR-020: ``bc_root`` is now the BC's abstract address, not a path; the bd
    workspace is discovered by walk-up from the LOCAL invoking CWD instead.
    """
    bc_root_path = _invoking_bd_context()
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
    refusal = _refuse_lead_side_respond("work_done")
    if refusal is not None:
        return refusal

    bc_root = _resolve_bc(args)

    # Reject blank (empty-string / whitespace-only) --scenario-hash values
    # BEFORE any storage write. A fat-fingered `--scenario-hash ""` must NOT
    # land a malformed-but-schema-valid work_done carrying a blank list member
    # (lead-7w0w; mechanism_observation lead-37zx). Validate first so no
    # work_done response is stored on rejection.
    for raw_hash in (args.scenario_hash or []):
        if not raw_hash.strip():
            print(
                f"shop-msg respond work_done: refusing blank --scenario-hash "
                f"value {raw_hash!r} (empty or whitespace-only); a "
                f"scenario-hash must be a non-empty value. No work_done "
                f"response was stored.",
                file=sys.stderr,
            )
            return 1

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
    # Stdout confirmation on success: the operator gets explicit feedback that
    # the emit landed, naming the work-id and status (lead-7w0w). Without this
    # a successful respond printed nothing, so a fat-fingered emit was silent.
    print(
        f"work_done {args.status} recorded for work-id {args.work_id}"
    )
    return 0


def _cmd_respond_mechanism_observation(args: argparse.Namespace) -> int:
    refusal = _refuse_lead_side_respond("mechanism_observation")
    if refusal is not None:
        return refusal

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


def _cmd_respond_request_completion_journal(args: argparse.Namespace) -> int:
    """`shop-msg respond request_completion_journal` (lead-f1ui).

    The BC's response to a request_completion_journal: it carries the set of
    completed block-only canonical scenario hashes back to the requester. Like
    the other respond verbs it is a BC->requester vehicle, delivered into the
    requester (lead) inbox via insert_bc_response under the
    `request_completion_journal_response` message_type.
    """
    refusal = _refuse_lead_side_respond("request_completion_journal")
    if refusal is not None:
        return refusal

    bc_root = _resolve_bc(args)

    if "/" in args.work_id or ".." in args.work_id or not args.work_id:
        print(
            f"shop-msg respond request_completion_journal: refusing unsafe "
            f"work_id {args.work_id!r}",
            file=sys.stderr,
        )
        return 1

    lead_root = _resolve_registered_lead()
    if lead_root is None:
        print(
            "shop-msg respond request_completion_journal: no lead shop is "
            "registered in the registry. Run 'shop-msg registry add "
            "--lead-shop' to register the lead first.",
            file=sys.stderr,
        )
        return 1

    message = RequestCompletionJournalResponse(
        message_type="request_completion_journal_response",
        work_id=args.work_id,
        completed_entries=set(args.completed or []),
    )

    try:
        insert_bc_response(
            lead_root,
            bc_root,
            args.work_id,
            "request_completion_journal_response",
            message.model_dump(mode="json"),
            force=getattr(args, "force", False),
        )
    except CollisionError:
        existing = existing_lead_inbox_message_type(
            lead_root, args.work_id, "request_completion_journal_response"
        ) or "request_completion_journal_response"
        print(
            f"shop-msg respond request_completion_journal: refusing to "
            f"overwrite existing {existing} response for "
            f"work_id={args.work_id!r} (use --force to replace)",
            file=sys.stderr,
        )
        return 1

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
    # ADR-020: a registered lead resolves to the sentinel abstract address,
    # not a path; the lead's bd workspace is discovered by walk-up from the
    # LOCAL invoking CWD. The "is a lead registered?" gate is the non-None
    # abstract address; the bd cwd is the invoking CWD.
    lead_address = _resolve_registered_lead()
    lead_root = _invoking_bd_context() if lead_address is not None else None
    use_bd = lead_root is not None and bd_facade.bd_available(lead_root)

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
        "request_completion_journal": RequestCompletionJournal,
        "request_scenario_register": RequestScenarioRegister,
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
        bc_origin_main_commit=_bc_origin_main_commit(str(_bc_clone_context())),
        payload_ref=getattr(args, "payload", None),
        queue_on_dependency=getattr(args, "queue_on_dependency", False),
    )


def _cmd_send_request_completion_journal(args: argparse.Namespace) -> int:
    """`shop-msg send request_completion_journal` (lead-f1ui).

    Deposits a request_completion_journal inbox message naming the target BC
    whose completed scenarios are sought. Like the other lead->BC sends it runs
    through the bd-first send protocol, which gracefully degrades to a bare
    postgres deposit when no lead shop is registered.
    """
    bc_root = _resolve_bc(args)

    if getattr(args, "payload", None):
        payload = _load_payload_file(
            args.payload, "request_completion_journal", args.work_id
        )
    else:
        if args.target_bc is None:
            print(
                "shop-msg send request_completion_journal: --target-bc is "
                "required unless --payload is supplied",
                file=sys.stderr,
            )
            return 2
        message = RequestCompletionJournal(
            message_type="request_completion_journal",
            work_id=args.work_id,
            target_bc=args.target_bc,
            from_shop=_resolve_send_sender(),
        )
        payload = message.model_dump(exclude_none=True)

    return _bd_first_send(
        command="request_completion_journal",
        bc_root=bc_root,
        bc_name=args.bc,
        work_id=args.work_id,
        message_type="request_completion_journal",
        payload=payload,
        scenario_hashes_pinned=None,
        depends_on_dispatch=getattr(args, "depends_on", None),
        bc_origin_main_commit=_bc_origin_main_commit(str(_bc_clone_context())),
        payload_ref=getattr(args, "payload", None),
        queue_on_dependency=getattr(args, "queue_on_dependency", False),
    )


def _cmd_send_request_scenario_register(args: argparse.Namespace) -> int:
    """`shop-msg send request_scenario_register` (lead-i1we, scenario 38).

    Deposits exactly one well-formed request_scenario_register inbox message
    naming the target BC whose scenario register is sought. Modeled on the
    sibling `send request_completion_journal` send subcommand (both name a
    target_bc and carry no answer-side entry of their own), it runs through
    the same bd-first send protocol.

    The narrowing selector is OPTIONAL (RequestScenarioRegister.narrowing):

      - `--feature-area <surface>` confines the request to a named
        feature-area surface, OR
      - `--hash <h>` (repeatable) confines it to an explicit set of
        block-only canonical hashes.

    Omitting narrowing entirely (no --feature-area and no --hash) leaves
    ``narrowing`` None, which the schema documents as denoting the target
    BC's FULL register rather than any subset. The request carries NO
    register entry of its own — RequestScenarioRegister has no
    register_entries field at all.
    """
    bc_root = _resolve_bc(args)

    if getattr(args, "payload", None):
        payload = _load_payload_file(
            args.payload, "request_scenario_register", args.work_id
        )
    else:
        if args.target_bc is None:
            print(
                "shop-msg send request_scenario_register: --target-bc is "
                "required unless --payload is supplied",
                file=sys.stderr,
            )
            return 2
        narrowing = None
        feature_area = getattr(args, "feature_area", None)
        hashes = list(getattr(args, "hash", None) or [])
        if feature_area is not None or hashes:
            narrowing = RegisterNarrowing(
                feature_area=feature_area,
                # A JSON-serializable list preserving the supplied --hash order;
                # a set would crash insert_message's json.dumps (lead-jo9p).
                hashes=hashes or None,
            )
        message = RequestScenarioRegister(
            message_type="request_scenario_register",
            work_id=args.work_id,
            target_bc=args.target_bc,
            narrowing=narrowing,
            from_shop=_resolve_send_sender(),
        )
        payload = message.model_dump(exclude_none=True)

    return _bd_first_send(
        command="request_scenario_register",
        bc_root=bc_root,
        bc_name=args.bc,
        work_id=args.work_id,
        message_type="request_scenario_register",
        payload=payload,
        scenario_hashes_pinned=None,
        depends_on_dispatch=getattr(args, "depends_on", None),
        bc_origin_main_commit=_bc_origin_main_commit(str(_bc_clone_context())),
        payload_ref=getattr(args, "payload", None),
        queue_on_dependency=getattr(args, "queue_on_dependency", False),
    )


def _build_scenario_payload(
    path_str: str, feature_title: str, bc_tag: str
) -> ScenarioPayload:
    """Read a scenario body file and build a ScenarioPayload whose hash is
    the canonical scenario-block-only hash of that block. Shared between
    assign_scenarios and request_bugfix because both messages embed the
    same ScenarioPayload shape.

    Canonical hash text is scenario-block-only (ADR-019, scenario 117):
    the scenario block alone — its tags, the Scenario/Scenario Outline
    keyword line, steps, and any Examples — with NO `Feature:` header
    line. The earlier shape wrapped the body in `Feature: {title}\\n...`
    before hashing; because the canonicalization rule
    (`scenarios.hash.compute_scenario_hash`) drops blank lines and
    `@scenario_hash:` lines but does NOT drop the `Feature:` line, that
    line survived into the canonical hash text and made the dispatched
    `scenarios[].hash` diverge from the scenario-block-only
    `@scenario_hash:` tag written on disk for the same block (defect
    lead-pw41(a)). We therefore hash — and store in `gherkin` — the
    scenario block alone, with no Feature header.

    `feature_title` is retained in the signature (callers still supply
    it via `--feature-title`) but is intentionally NOT part of the hash
    text or the stored `gherkin`: only the scenario block participates in
    the canonical hash, per scenario 117.

    The chicken-and-egg of "the tag line includes the hash we're about
    to compute" resolves because the canonicalization rule strips
    `@scenario_hash:<anything>` lines unconditionally: we build the block
    with a sentinel `@scenario_hash:` tag, hash it (the sentinel line is
    dropped), then emit the same block with the now-known hash
    substituted in. The substitution does not perturb the canonical hash
    because both forms differ only in the stripped `@scenario_hash:` line.

    The hash this returns equals `scenarios hash` of the scenario-block-only
    body byte-for-byte, and equals the `@scenario_hash:` tag the BC writes
    on disk for that block — restoring scenario 117 wire/disk equality.
    """
    body = Path(path_str).read_text()
    sentinel = "0" * 16
    sentinel_tags = [f"@scenario_hash:{sentinel}", f"@bc:{bc_tag}"]
    sentinel_block = f"{' '.join(sentinel_tags)}\n{body}\n"
    scen_hash = _compute_scenario_hash(sentinel_block)
    tags = [f"@scenario_hash:{scen_hash}", f"@bc:{bc_tag}"]
    block = f"{' '.join(tags)}\n{body}\n"
    return ScenarioPayload(hash=scen_hash, tags=tags, gherkin=block)


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
            bc_origin_main_commit=_bc_origin_main_commit(str(_bc_clone_context())),
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
        bc_origin_main_commit=_bc_origin_main_commit(str(_bc_clone_context())),
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
            bc_origin_main_commit=_bc_origin_main_commit(str(_bc_clone_context())),
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
        bc_origin_main_commit=_bc_origin_main_commit(str(_bc_clone_context())),
        payload_ref=getattr(args, "payload", None),
        queue_on_dependency=getattr(args, "queue_on_dependency", False),
    )


def _cmd_send_clarify_response(args: argparse.Namespace) -> int:
    """`shop-msg send clarify_response` — lead -> BC in-band clarify answer (lead-ox8).

    Delivers the lead's answer to an outstanding BC clarify, RE-OPENING the
    original dispatch on the SAME work_id for the BC's gated loop to resume. The
    answer lands as a ``direction='inbox'`` row keyed
    ``(bc, work_id, message_type='clarify_response')`` deposited with
    ``allow_multi_type=True`` so it COEXISTS with the original dispatch inbox row
    rather than colliding against the one-row-per-(bc,work_id,message_type)
    invariant — the same coexistence mechanism work_done/mechanism_observation
    use (lead-0lml). No new work_id and no new bead are minted: clarify_response
    re-opens, it does not re-dispatch.

    Precondition (enforced at the CLI surface): a prior BC clarify must exist for
    that (bc, work_id). A clarify_response with nothing to answer is operator
    error and is refused with a non-zero exit. The BC's clarify is recorded as a
    BC-side outbox marker at ``(bc, work_id, direction='outbox',
    message_type='clarify')`` by ``shop-msg respond clarify``; that marker is the
    authoritative "the BC asked something here" check.

    clarify_response carries NO scenario state: the ClarifyResponse schema has no
    scenario_hashes field (and forbids extra keys), so a scope-changing answer
    cannot ride a clarify_response and must route to re-dispatch
    (assign_scenarios / request_bugfix) per ADR-009 layer (b) / ADR-027.
    """
    bc_root = _resolve_bc(args)

    # Precondition: refuse unless the BC has an outstanding clarify on this
    # (bc, work_id). Checked BEFORE any write so a refused send leaves no
    # clarify_response row and does not re-open the dispatch.
    if not outbox_row_exists(bc_root, args.work_id, "clarify"):
        print(
            f"shop-msg send clarify_response: refusing to send for "
            f"work_id={args.work_id!r}: a clarify_response requires a prior BC "
            f"clarify on that (bc, work_id), and none exists. clarify_response is "
            f"valid ONLY as the answer to an outstanding clarify.",
            file=sys.stderr,
        )
        return 1

    message = ClarifyResponse(
        message_type="clarify_response",
        work_id=args.work_id,
        resolution=args.resolution,
        from_shop=_resolve_send_sender(),
    )

    # Deposit as a coexisting inbox row (allow_multi_type=True): re-opens the
    # original dispatch on the SAME work_id without overwriting it. NOTIFY fires
    # so the BC's inbox watcher wakes for the re-opened work.
    try:
        insert_message(
            bc_root,
            args.work_id,
            "inbox",
            "clarify_response",
            message.model_dump(exclude_none=True),
            notify=True,
            allow_multi_type=True,
        )
    except CollisionError:
        print(
            f"shop-msg send clarify_response: a clarify_response already exists "
            f"for work_id={args.work_id!r}",
            file=sys.stderr,
        )
        return 1
    return 0


_NUDGE_REASONS = ("stuck-on-you", "status-check", "predecessor-landed", "general")


def _do_nudge(
    *,
    command: str,
    recipient_root: str,
    work_id: str | None,
    reason: str,
    note: str | None,
    payload_file: str | None,
    sender_name: str | None,
) -> int:
    """Shared body for `shop-msg nudge` (BC->lead) and `shop-msg send nudge`
    (lead->BC).

    Both surfaces share the same delivery semantics (ADR-015 / lead-xp5f):
      * validate the reason against the closed 4-value enum,
      * require --note iff reason=general,
      * reject any payload carrying scenario_hashes (transmission-layer only),
      * store a direction='nudge' row at the recipient (multi-delivery; never
        collides with the dispatch row or with a prior nudge),
      * append exactly one canonical bd note to the lead's bead for work_id,
        WITHOUT touching dispatch_state.

    The messaging BC's responsibility ends at delivery + bd note appending
    (lead-xp5f decision 3): no receiver-reply / one-reply-cap logic here.
    """
    # Closed reason enum enforcement at the CLI surface (ADR-015 decision 2).
    # argparse `choices` already rejects bad reasons, but we re-check so the
    # error message names the invalid value AND lists the four valid ones,
    # exactly as scenario 1ff42687 pins it (and so a --payload path that
    # smuggles a reason is caught too).
    if reason not in _NUDGE_REASONS:
        print(
            f"shop-msg {command}: invalid reason {reason!r}. Valid reasons are: "
            f"{', '.join(_NUDGE_REASONS)}.",
            file=sys.stderr,
        )
        return 2

    # Payload-file path: the file is the content source but MUST NOT carry
    # scenario state. We reject scenario_hashes BEFORE building the model, so
    # the error names the rejected field (scenario eab77aec).
    if payload_file:
        raw = yaml.safe_load(Path(payload_file).read_text()) or {}
        try:
            _nudge_payload_rejects_scenario_state(raw)
        except ValueError as exc:
            print(f"shop-msg {command}: {exc}", file=sys.stderr)
            return 2
        if isinstance(raw, dict):
            # The file may override note/reason; flags still win for keys the
            # operator passed explicitly.
            note = note if note is not None else raw.get("note")
            reason = raw.get("reason", reason) if reason is None else reason

    # Build & validate the Nudge model. The schema enforces --note-iff-general
    # and the path-safe work_id shape; surface its message verbatim.
    try:
        message = Nudge(
            message_type="nudge",
            reason=reason,
            work_id=work_id,
            note=note,
            from_shop=sender_name,
        )
    except ValidationError as exc:
        # Name --note explicitly for the general-without-note case so the
        # error matches scenario 4abbd813's expectation.
        if reason == "general" and not (note and note.strip()):
            print(
                f"shop-msg {command}: --note is required when --reason=general "
                f"(the 'general' reason carries no semantics of its own).",
                file=sys.stderr,
            )
        else:
            print(f"shop-msg {command}: {exc}", file=sys.stderr)
        return 2

    payload = message.model_dump(exclude_none=True)

    # Delivery: store the direction='nudge' row at the recipient. Never
    # collides (nudge is outside the inbox/outbox partial unique index), so a
    # second nudge against the same (recipient, work_id) is storable.
    insert_nudge(recipient_root, work_id, payload)

    # bd note: append exactly one canonical note to the LEAD's bead for
    # work_id, leaving dispatch_state untouched. The bead lives in the lead
    # shop's bd workspace. Best-effort: a missing lead workspace or missing
    # bead is a no-op (bd_available / bd note guards), never blocking delivery.
    if work_id:
        lead_address = _resolve_registered_lead()
        if lead_address is not None:
            at = (
                __import__("datetime")
                .datetime.now(__import__("datetime").timezone.utc)
                .isoformat()
            )
            note_text = bd_facade.nudge_note_text(reason, work_id, at)
            try:
                # ADR-020: the lead's bd workspace resolves from the LOCAL
                # invoking CWD (no registry-stored path).
                bd_facade.append_note(
                    work_id, note_text, root=_invoking_bd_context()
                )
            except bd_facade.BdFacadeError as exc:
                # Delivery already succeeded; the bd note is a secondary
                # artefact. Surface a warning but do not fail the nudge.
                print(
                    f"shop-msg {command}: nudge delivered but bd note append "
                    f"failed: {exc}",
                    file=sys.stderr,
                )

    recipient_desc = f" against work_id={work_id!r}" if work_id else ""
    print(f"shop-msg {command}: nudge ({reason}) delivered{recipient_desc}.")
    return 0


def _cmd_nudge(args: argparse.Namespace) -> int:
    """`shop-msg nudge` — BC -> lead operational-liveness signal.

    The recipient is the registered lead shop; the sender is the BC resolved
    from CWD (or --bc). The nudge row is stored at the LEAD's root.
    """
    # The --bc flag names the BC (sender); the recipient is the lead.
    sender_name = args.bc or _resolve_send_sender()
    lead_root_str = _resolve_registered_lead()
    if lead_root_str is None:
        print(
            "shop-msg nudge: no lead shop is registered; cannot address a "
            "BC->lead nudge. Register the lead with 'shop-msg registry add'.",
            file=sys.stderr,
        )
        return 1
    return _do_nudge(
        command="nudge",
        recipient_root=lead_root_str,
        work_id=getattr(args, "work_id", None),
        reason=args.reason,
        note=getattr(args, "note", None),
        payload_file=getattr(args, "payload", None),
        sender_name=sender_name,
    )


def _cmd_send_nudge(args: argparse.Namespace) -> int:
    """`shop-msg send nudge` — lead -> BC operational-liveness signal.

    The recipient is the BC named by --bc; the sender is the lead resolved
    from CWD. The nudge row is stored at the BC's root.
    """
    bc_root = _resolve_bc(args)
    return _do_nudge(
        command="send nudge",
        recipient_root=bc_root,
        work_id=getattr(args, "work_id", None),
        reason=args.reason,
        note=getattr(args, "note", None),
        payload_file=getattr(args, "payload", None),
        sender_name=_resolve_send_sender(),
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

    # The outbox is keyed by (work_id, message_type), so multiple rows can
    # legitimately coexist under one work_id (e.g. a work_done AND a later
    # mechanism_observation). A reader that surfaced only the most-recent row
    # masked the others — a router could then wrongly conclude an intact
    # earlier response was overwritten and dispatch a destructive "restore".
    #
    # Default (no --message-type): surface EVERY coexisting row in
    # created_at order (oldest first), so the full outbox state for the
    # work_id is observable. --message-type narrows to exactly that row.
    selector = getattr(args, "message_type", None)
    if selector is not None:
        selected = [r for r in rows if r.get("message_type") == selector]
        if not selected:
            present = sorted({str(r.get("message_type")) for r in rows})
            print(
                f"shop-msg read outbox: no {selector!r} outbox response found "
                f"for work_id={args.work_id!r} in bc={bc_root!r}; "
                f"present message_types: {present}",
                file=sys.stderr,
            )
            return 1
        rows = selected

    rendered: list[str] = []
    for raw in rows:
        try:
            message = _response_adapter.validate_python(raw)
        except ValidationError as e:
            print(
                f"shop-msg read outbox: validation failed for "
                f"work_id={args.work_id!r}:\n{e}",
                file=sys.stderr,
            )
            return 1
        rendered.append(
            f"valid {message.message_type} from {args.work_id}:\n"
            f"{_render_message_yaml(message)}"
        )

    print("\n".join(rendered))
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
    print(_render_message_yaml(message))
    return 0


def _cmd_pending_inbox(args: argparse.Namespace) -> int:
    """Enumerate inbox messages that have no matching outbox response.

    Queries Postgres using the pending-query SQL that replaces the old
    directory-glob walk.

    No shop_root existence check is performed (ADR-018, lead-mxxm). On the
    lead host the BC's registered shop_root column is load-bearing for
    nothing: BCs run as bc-launcher containers and the clone lives inside
    the container, so the lead never reads or runs that path. A name ->
    bc_id resolution is all the Postgres-backed pending query needs; the
    path's existence on the lead filesystem is irrelevant to whether the
    query returns the right rows. The former staleness guard refused
    name-addressed operations whenever the path was absent, which is
    doctrine-incoherent on the lead host. The bc_id used by the query is
    derived from the registered name, not from probing the filesystem, so
    dropping the check does not reintroduce the silent-zero-rows failure it
    was written to prevent.
    """
    bc_root = _resolve_bc(args)
    rows = query_pending_inbox(bc_root)
    # ADR-020: bc_root is the BC's abstract address (no path). The BC-side
    # bead side effect (ADR-017) runs `bd` with cwd scoped to the LOCAL
    # invoking CWD, whose `.beads` workspace is discovered by walk-up. A
    # bd-less invoking CWD simply lists the rows (best-effort, pre-lead-sn1e
    # behavior); resolving from the local CWD — never a registry-stored path
    # — is what keeps name-addressed pending from crashing (scenario
    # f9910cf40291768c).
    bc_root_path = _invoking_bd_context()
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
    print(_render_message_yaml(message))
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
    lead_address = _resolve_registered_lead()
    if lead_address is not None:
        # ADR-020: the lead's bd workspace resolves from the LOCAL invoking
        # CWD, not a registry-stored path.
        lead_root = _invoking_bd_context()
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


def _cmd_consume_inbox(args: argparse.Namespace) -> int:
    """Mark a specific lead-inbox row as consumed (lead side).

    Lead-side counterpart to ``consume outbox`` for the inbox direction
    (lead-rcjf, scenario c4dbfe1cd31d0aea): a lead drains a consumed or
    superseded message from its OWN inbox so it no longer appears in
    'pending inbox --lead' output. Exits non-zero if no matching unconsumed
    inbox row exists.
    """
    lead_root = _resolve_lead(args)
    found = consume_lead_inbox_message(lead_root, args.work_id)
    if not found:
        print(
            f"shop-msg consume inbox: no unconsumed inbox row found for "
            f"work_id={args.work_id!r} in lead={args.lead!r}",
            file=sys.stderr,
        )
        return 1
    return 0


def _cmd_retract_inbox(args: argparse.Namespace) -> int:
    """Retract a still-pending inbox dispatch the BC has not yet consumed.

    lead-9xrd. The lead retracts a dispatch it deposited in a BC's inbox so
    the BC will NOT process it — but only while it is STILL PENDING:

    * Still pending -> the inbox row is REMOVED. After retraction the
      dispatch is absent from ``pending inbox`` and ``read inbox`` reports
      not-found. The retraction is recorded in the messaging audit trail.
      Exits zero.
    * Absent / already-retracted -> idempotent no-op SUCCESS (exits zero),
      leaving the dispatch absent. (Distinguished from the consumed case:
      there is no row to refuse.)
    * Already CONSUMED -> REFUSED. The BC has already taken the work on, so
      the deposit is left intact and the command exits NON-zero naming the
      already-consumed condition. The refused attempt is recorded in the
      audit trail.
    """
    bc_root = _resolve_bc(args)
    outcome = retract_inbox_message(bc_root, args.work_id, args.message_type)
    if outcome == "refused":
        print(
            f"shop-msg retract inbox: work_id={args.work_id!r} was already "
            f"consumed and cannot be retracted (the BC has taken the work on). "
            f"The consumed deposit is left intact.",
            file=sys.stderr,
        )
        return 1
    # "retracted" (removed) and "absent" (idempotent no-op) are both success.
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
    if resolve_shop_name(args.shop) is None:
        print(
            f"shop-msg sweep: shop name {args.shop!r} is not registered in the "
            f"registry.",
            file=sys.stderr,
        )
        return 1
    # ADR-020: the swept shop's bd workspace resolves from the LOCAL invoking
    # CWD (no registry-stored path).
    shop_root = _invoking_bd_context()
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
            # Authoritative ground truth is bd-native issue status (lead-fnj5
            # cure (a)): a predecessor closed via `bd close` reads
            # status=closed even though its dispatch_state metadata was never
            # advanced past consumed. predecessor_satisfied() ORs the
            # bd-native status with the historical dispatch_state=closed
            # projection, so this skip remains at least as strict as before.
            if not bd_facade.predecessor_satisfied(shop_root, pending_dependency):
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
        # ADR-020: bc_root_str is the BC's abstract address; thread it
        # straight through as the storage key (no path resolution).
        bc_root = bc_root_str

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
    # ADR-020: bc_root_str is the BC's abstract address (storage key).
    bc_root = bc_root_str
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
    if resolve_shop_name(args.shop) is None:
        print(
            f"shop-msg promote: shop name {args.shop!r} is not registered in "
            f"the registry.",
            file=sys.stderr,
        )
        return 1
    # ADR-020: the promoted shop's bd workspace resolves from the LOCAL
    # invoking CWD (no registry-stored path).
    shop_root = _invoking_bd_context()
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
    from shop_msg.storage import _get_dsn, probe_db_reachable, query_pending_inbox

    bc_name = args.bc

    # CWD-derived name that does not resolve against the registry: prime must
    # still orient (DSN, DB-health, CLI catalog) so the agent is not left
    # blind. The name-resolution miss becomes a stderr warning, not a hard
    # exit (lead-t8v8 scenario 48). Explicit --bc <name> retains the hard
    # exit via _resolve_bc below. The DB-unreachable hard exit is preserved
    # in both paths.
    cwd_derived = getattr(args, "_cwd_derived", False)
    unresolved = cwd_derived and resolve_shop_name(bc_name) is None

    if unresolved:
        dsn = _get_dsn()
        print(f"DSN: {dsn}")
        try:
            probe_db_reachable()
        except Exception as exc:
            print(f"DB reachable: no — {exc}")
            print()
            _print_prime_reminder(bc_name)
            return 1
        print("DB reachable: yes")
        print()
        print(
            f"shop-msg: warning: CWD-derived shop name {bc_name!r} did not "
            f"resolve against the registry; orientation is shown but inbox "
            f"counts are unavailable until the shop is registered "
            f"(run 'shop-msg registry add').",
            file=sys.stderr,
        )
        _print_prime_reminder(bc_name)
        return 0

    bc_root = _resolve_bc(args)
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
        "  shop-msg respond clarify | work_done | mechanism_observation ...\n"
        "  shop-msg send ...        # lead-side dispatch into a BC inbox\n"
        f"  shop-msg watch --bc {bc_name}    # LISTEN/NOTIFY inbox watcher\n"
        "  shop-msg registry ...    # add | remove | list shop registrations"
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
    from shop_msg.storage import (
        _get_dsn,
        probe_db_reachable,
        query_pending_lead_inbox,
    )

    lead_name = args.lead

    # CWD-derived lead name that does not resolve against the registry:
    # orient (DSN, DB-health, CLI catalog) and warn on stderr rather than
    # hard-exit (lead-t8v8 scenario 48 — type.md content is "lead", so the
    # bare-prime invocation dispatches through the lead branch). Explicit
    # --lead <name> retains the hard exit via _resolve_lead below. The
    # DB-unreachable hard exit is preserved in both paths.
    cwd_derived = getattr(args, "_cwd_derived", False)
    unresolved = cwd_derived and resolve_shop_name(lead_name) is None

    if unresolved:
        literal = getattr(args, "_cwd_derived_literal_name", lead_name)
        dsn = _get_dsn()
        print(f"DSN: {dsn}")
        try:
            probe_db_reachable()
        except Exception as exc:
            print(f"DB reachable: no — {exc}")
            print()
            _print_prime_lead_reminder(lead_name)
            return 1
        print("DB reachable: yes")
        print()
        print(
            f"shop-msg: warning: CWD-derived shop name {literal!r} did not "
            f"resolve against the registry; orientation is shown but inbox "
            f"counts are unavailable until the shop is registered "
            f"(run 'shop-msg registry add').",
            file=sys.stderr,
        )
        _print_prime_lead_reminder(lead_name)
        return 0

    lead_root = _resolve_lead(args)
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
        "  shop-msg send assign_scenarios | request_bugfix | request_maintenance"
        " | request_scenario_register | request_shop_card  # dispatch into a BC inbox\n"
        "  shop-msg nudge ...       # nudge a BC about a pending dispatch\n"
        "  shop-msg consume ...     # consume a BC outbox response\n"
        f"  shop-msg watch --lead {lead_name}    # LISTEN/NOTIFY outbox watcher\n"
        "  shop-msg registry ...    # add | remove | list shop registrations\n"
        "Note: 'shop-msg respond' is a BC->lead vehicle only; the lead does NOT respond.\n"
        "The lead answers a BC clarify by RE-DISPATCH on a fresh lead bead (a new\n"
        "'shop-msg send ...'), not by 'shop-msg respond'."
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
        # ADR-020: resolve_shop_name yields the abstract address (storage key).
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
    """Register a shop by canonical name (ADR-020: no filesystem path).

    A filesystem-path positional is no longer accepted. When one is supplied
    the command exits non-zero with a migration message naming the new
    no-path form, and adds NO entry (scenario cf22ce33ba3edeea).
    """
    if getattr(args, "shop_root", None) is not None:
        print(
            "shop-msg registry add: a shop_root path is no longer accepted "
            "(ADR-020: the registry stores no filesystem path). "
            "Use: registry add [--lead-shop] <name>",
            file=sys.stderr,
        )
        return 1
    shop_type = "lead" if getattr(args, "lead_shop", False) else "bc"
    registry_add(args.name, shop_type=shop_type)
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
    """List all registered shops.

    ADR-020: each line is ``<name> <abstract_address> <shop_type>``; there
    is no filesystem-path column (scenarios 3d9da19b3174fcf6 /
    b324c650784c2378).
    """
    entries = registry_list()
    for name, abstract_address, shop_type in entries:
        print(f"{name} {abstract_address} {shop_type}")
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

    rcj_resp = respond_sub.add_parser(
        "request_completion_journal",
        help=(
            "write a request_completion_journal response carrying the BC's "
            "completed block-only canonical scenario hashes (lead-f1ui)"
        ),
    )
    _rcj_resp_mode = rcj_resp.add_mutually_exclusive_group(required=False)
    _rcj_resp_mode.add_argument(
        "--bc", default=None,
        help=(
            "canonical BC name. Optional: when omitted the shop is resolved "
            "from CWD via .claude/shop/ marker walk-up (PDR-008)."
        ),
    )
    _add_removed_flag(rcj_resp, "--bc-root")
    rcj_resp.add_argument(
        "--work-id", required=True,
        help="work_id of the request_completion_journal being responded to",
    )
    rcj_resp.add_argument(
        "--completed", action="append", default=None,
        help=(
            "a completed block-only canonical scenario hash (repeatable). The "
            "collected hashes form the response's bare completed-entries set."
        ),
    )
    rcj_resp.add_argument(
        "--force",
        action="store_true",
        help=(
            "replace an existing same-message_type lead-inbox response for this "
            "work_id (recovery path; lead-2id). Without --force the command "
            "refuses on collision."
        ),
    )
    rcj_resp.set_defaults(
        func=_cmd_respond_request_completion_journal, _cwd_resolves=True
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

    # clarify_response (lead-ox8): the lead's in-band answer to an outstanding
    # BC clarify. Re-opens the original dispatch on the SAME work_id. Carries
    # ONLY --bc, --work-id, and --resolution — NO --scenario-file or any flag
    # that carries scenario state, mirroring the no-scenario-state constraint
    # the schema enforces (ADR-009 layer (b) / ADR-027).
    clarify_response = send_sub.add_parser(
        "clarify_response",
        help=(
            "answer an outstanding BC clarify in-band, re-opening the original "
            "dispatch on the same work_id"
        ),
    )
    clarify_response.add_argument(
        "--bc", required=True, help="canonical BC name (resolved via registry)"
    )
    clarify_response.add_argument(
        "--work-id",
        required=True,
        help="work_id of the dispatch whose clarify is being answered",
    )
    clarify_response.add_argument(
        "--resolution",
        required=True,
        help="the lead's answer text delivered in-band to the BC",
    )
    clarify_response.set_defaults(func=_cmd_send_clarify_response)

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

    request_completion_journal = send_sub.add_parser(
        "request_completion_journal",
        help="write a request_completion_journal message (lead-f1ui)",
    )
    request_completion_journal.add_argument(
        "--bc", required=True, help="canonical BC name (resolved via registry)"
    )
    _add_removed_flag(request_completion_journal, "--bc-root")
    request_completion_journal.add_argument(
        "--work-id", required=True, help="work_id identifying this request"
    )
    request_completion_journal.add_argument(
        "--target-bc",
        default=None,
        help=(
            "the bounded context whose completed scenarios are sought "
            "(required unless --payload)"
        ),
    )
    request_completion_journal.add_argument(
        "--payload",
        default=None,
        help=(
            "path to a YAML/JSON file pinning the complete message body; when "
            "supplied it is the authoritative content source and --target-bc "
            "is not required"
        ),
    )
    request_completion_journal.add_argument(
        "--depends-on",
        default=None,
        help=(
            "work_id of a prior dispatch this one depends on; recorded on the "
            "lead bd entry as depends_on_dispatch metadata"
        ),
    )
    request_completion_journal.add_argument(
        "--queue-on-dependency",
        action="store_true",
        help=(
            "when a bd depends-on predecessor is not yet at "
            "dispatch_state=closed, defer the postgres deposit (PDR-010 / "
            "ADR-013 decision 4)"
        ),
    )
    request_completion_journal.set_defaults(
        func=_cmd_send_request_completion_journal
    )

    request_scenario_register = send_sub.add_parser(
        "request_scenario_register",
        help="write a request_scenario_register message (lead-i1we)",
    )
    request_scenario_register.add_argument(
        "--bc", required=True, help="canonical BC name (resolved via registry)"
    )
    _add_removed_flag(request_scenario_register, "--bc-root")
    request_scenario_register.add_argument(
        "--work-id", required=True, help="work_id identifying this request"
    )
    request_scenario_register.add_argument(
        "--target-bc",
        default=None,
        help=(
            "the bounded context whose scenario register is sought "
            "(required unless --payload)"
        ),
    )
    request_scenario_register.add_argument(
        "--feature-area",
        default=None,
        help=(
            "OPTIONAL narrowing selector: confine the request to a named "
            "feature-area surface. Omit all narrowing flags to request the "
            "target BC's full register"
        ),
    )
    request_scenario_register.add_argument(
        "--hash",
        action="append",
        default=None,
        metavar="HASH",
        help=(
            "OPTIONAL narrowing selector: confine the request to an explicit "
            "block-only canonical hash (repeatable). Omit all narrowing flags "
            "to request the target BC's full register"
        ),
    )
    request_scenario_register.add_argument(
        "--payload",
        default=None,
        help=(
            "path to a YAML/JSON file pinning the complete message body; when "
            "supplied it is the authoritative content source and --target-bc "
            "is not required"
        ),
    )
    request_scenario_register.add_argument(
        "--depends-on",
        default=None,
        help=(
            "work_id of a prior dispatch this one depends on; recorded on the "
            "lead bd entry as depends_on_dispatch metadata"
        ),
    )
    request_scenario_register.add_argument(
        "--queue-on-dependency",
        action="store_true",
        help=(
            "when a bd depends-on predecessor is not yet at "
            "dispatch_state=closed, defer the postgres deposit (PDR-010 / "
            "ADR-013 decision 4)"
        ),
    )
    request_scenario_register.set_defaults(
        func=_cmd_send_request_scenario_register
    )

    send_nudge = send_sub.add_parser(
        "nudge",
        help="send a lead->BC operational-liveness nudge (ADR-015)",
    )
    send_nudge.add_argument(
        "--bc", required=True, help="canonical BC name (recipient; resolved via registry)"
    )
    _add_removed_flag(send_nudge, "--bc-root")
    send_nudge.add_argument(
        "--reason",
        required=True,
        choices=list(_NUDGE_REASONS),
        help=(
            "closed reason enum (ADR-015 decision 2): stuck-on-you, "
            "status-check, predecessor-landed, general"
        ),
    )
    send_nudge.add_argument(
        "--work-id",
        default=None,
        help="lead dispatch work_id this nudge references (optional)",
    )
    send_nudge.add_argument(
        "--note",
        default=None,
        help="free-form note; REQUIRED when --reason=general, optional otherwise",
    )
    send_nudge.add_argument(
        "--payload",
        default=None,
        help=(
            "path to a YAML/JSON file pinning the nudge body; rejected if it "
            "carries a scenario_hashes field (nudge is transmission-layer only)"
        ),
    )
    send_nudge.set_defaults(func=_cmd_send_nudge)

    nudge = sub.add_parser(
        "nudge",
        help="send a BC->lead operational-liveness nudge (ADR-015)",
    )
    nudge.add_argument(
        "--bc",
        default=None,
        help=(
            "canonical BC name of the SENDER (resolved via registry). "
            "Optional: when omitted, the BC name is resolved from CWD via "
            ".claude/shop/ marker walk-up (PDR-008). The recipient is the "
            "registered lead shop."
        ),
    )
    _add_removed_flag(nudge, "--bc-root")
    nudge.add_argument(
        "--reason",
        required=True,
        choices=list(_NUDGE_REASONS),
        help=(
            "closed reason enum (ADR-015 decision 2): stuck-on-you, "
            "status-check, predecessor-landed, general"
        ),
    )
    nudge.add_argument(
        "--work-id",
        default=None,
        help="lead dispatch work_id this nudge references (optional)",
    )
    nudge.add_argument(
        "--note",
        default=None,
        help="free-form note; REQUIRED when --reason=general, optional otherwise",
    )
    nudge.add_argument(
        "--payload",
        default=None,
        help=(
            "path to a YAML/JSON file pinning the nudge body; rejected if it "
            "carries a scenario_hashes field (nudge is transmission-layer only)"
        ),
    )
    nudge.set_defaults(func=_cmd_nudge, _cwd_resolves=True)

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
    read_outbox.add_argument(
        "--message-type",
        default=None,
        help=(
            "optional selector: narrow to the single outbox row of this "
            "message_type. The outbox is keyed by (work_id, message_type), so "
            "multiple rows can coexist under one work_id (e.g. work_done + a "
            "later mechanism_observation). When omitted, ALL coexisting rows "
            "under the work_id are surfaced in created_at order."
        ),
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
        choices=_CONSUME_OUTBOX_MESSAGE_TYPES,
        help="message_type of the outbox row to consume",
    )
    consume_outbox.set_defaults(func=_cmd_consume_outbox)

    consume_inbox = consume_sub.add_parser(
        "inbox",
        help=(
            "mark a specific row in the lead's own inbox as consumed so it no "
            "longer appears in 'pending inbox --lead' output"
        ),
    )
    consume_inbox.add_argument(
        "--lead",
        required=True,
        help="canonical lead shop name (resolved via registry)",
    )
    consume_inbox.add_argument(
        "--work-id",
        required=True,
        help="work_id of the lead-inbox row to consume",
    )
    consume_inbox.set_defaults(func=_cmd_consume_inbox)

    # lead-9xrd: retract a still-pending inbox dispatch (lead side).
    retract = sub.add_parser(
        "retract",
        help="retract a still-pending inbox dispatch the BC has not yet consumed",
    )
    retract_sub = retract.add_subparsers(dest="retract_target", required=True)
    retract_inbox = retract_sub.add_parser(
        "inbox",
        help=(
            "remove a still-pending inbox dispatch so the BC will not process "
            "it; refuses if the BC has already consumed it"
        ),
    )
    retract_inbox.add_argument(
        "--bc",
        required=True,
        help="canonical BC name (resolved via registry)",
    )
    retract_inbox.add_argument(
        "--work-id",
        required=True,
        help="work_id of the inbox dispatch to retract",
    )
    retract_inbox.add_argument(
        "--message-type",
        required=True,
        help="message_type of the inbox dispatch to retract",
    )
    retract_inbox.set_defaults(func=_cmd_retract_inbox)

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
        help="register a shop by canonical name (no filesystem path; ADR-020)",
    )
    registry_add_cmd.add_argument("name", help="canonical shop name")
    # ADR-020: the registry stores no path. A filesystem-path positional is
    # accepted at parse time only so the command can emit a clear migration
    # message and exit non-zero (cf22ce33ba3edeea), rather than argparse
    # reporting an opaque "unrecognized arguments".
    registry_add_cmd.add_argument(
        "shop_root",
        nargs="?",
        default=None,
        help=argparse.SUPPRESS,
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
