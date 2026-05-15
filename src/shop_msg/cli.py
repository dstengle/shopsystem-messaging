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
from shop_msg.storage import (
    CollisionError,
    delete_bc_messages,
    inbox_row_exists,
    insert_message,
    insert_raw_payload,
    outbox_row_exists,
    query_pending_inbox,
    query_pending_outbox,
    read_inbox_message,
    read_outbox_messages,
)

_response_adapter = TypeAdapter(BCResponse)
_lead_adapter = TypeAdapter(LeadMessage)


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
    bc_root = str(Path(args.bc_root).resolve())

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

    message = Clarify(
        message_type="clarify",
        work_id=args.work_id,
        question=args.question,
    )

    try:
        insert_message(
            bc_root,
            args.work_id,
            "outbox",
            "clarify",
            message.model_dump(),
        )
    except CollisionError:
        print(
            f"shop-msg respond clarify: refusing to overwrite existing outbox "
            f"entry for work_id={args.work_id!r}",
            file=sys.stderr,
        )
        return 1
    return 0


def _cmd_respond_work_done(args: argparse.Namespace) -> int:
    bc_root = str(Path(args.bc_root).resolve())

    message = WorkDone(
        message_type="work_done",
        work_id=args.work_id,
        status=args.status,
        summary=args.summary,
        scenario_hashes=list(args.scenario_hash or []),
    )

    try:
        insert_message(
            bc_root,
            args.work_id,
            "outbox",
            "work_done",
            message.model_dump(),
        )
    except CollisionError:
        print(
            f"shop-msg respond work_done: refusing to overwrite existing outbox "
            f"entry for work_id={args.work_id!r}",
            file=sys.stderr,
        )
        return 1
    return 0


def _cmd_respond_mechanism_observation(args: argparse.Namespace) -> int:
    bc_root = str(Path(args.bc_root).resolve())

    # Path-safety: refuse work_ids that would escape the outbox dir.
    if "/" in args.work_id or ".." in args.work_id or not args.work_id:
        print(
            f"shop-msg respond mechanism_observation: refusing unsafe "
            f"work_id {args.work_id!r}",
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
        insert_message(
            bc_root,
            args.work_id,
            "outbox",
            "mechanism_observation",
            message.model_dump(exclude_none=True),
        )
    except CollisionError:
        print(
            f"shop-msg respond mechanism_observation: refusing to overwrite "
            f"existing outbox entry for work_id={args.work_id!r}",
            file=sys.stderr,
        )
        return 1
    return 0


def _cmd_send_request_maintenance(args: argparse.Namespace) -> int:
    bc_root = str(Path(args.bc_root).resolve())

    acceptance_criteria = list(args.acceptance_criterion or []) or None
    file_hints = list(args.file_hint or []) or None

    message = RequestMaintenance(
        message_type="request_maintenance",
        work_id=args.work_id,
        description=args.description,
        acceptance_criteria=acceptance_criteria,
        file_hints=file_hints,
    )

    try:
        insert_message(
            bc_root,
            args.work_id,
            "inbox",
            "request_maintenance",
            message.model_dump(exclude_none=True),
            notify=True,
        )
    except CollisionError:
        print(
            f"shop-msg send request_maintenance: refusing to overwrite existing "
            f"inbox entry for work_id={args.work_id!r}",
            file=sys.stderr,
        )
        return 1
    return 0


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
    bc_root = str(Path(args.bc_root).resolve())

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
    )

    try:
        insert_message(
            bc_root,
            args.work_id,
            "inbox",
            "request_bugfix",
            message.model_dump(),
            notify=True,
        )
    except CollisionError:
        print(
            f"shop-msg send request_bugfix: refusing to overwrite existing "
            f"inbox entry for work_id={args.work_id!r}",
            file=sys.stderr,
        )
        return 1
    return 0


def _cmd_send_assign_scenarios(args: argparse.Namespace) -> int:
    bc_root = str(Path(args.bc_root).resolve())

    scenario_files = list(args.scenario_file or [])
    scenarios_payload: list[ScenarioPayload] = [
        _build_scenario_payload(path_str, args.feature_title, args.bc_tag)
        for path_str in scenario_files
    ]

    message = AssignScenarios(
        message_type="assign_scenarios",
        work_id=args.work_id,
        scenarios=scenarios_payload,
    )

    try:
        insert_message(
            bc_root,
            args.work_id,
            "inbox",
            "assign_scenarios",
            message.model_dump(),
            notify=True,
        )
    except CollisionError:
        print(
            f"shop-msg send assign_scenarios: refusing to overwrite existing "
            f"inbox entry for work_id={args.work_id!r}",
            file=sys.stderr,
        )
        return 1
    return 0


def _cmd_read_outbox(args: argparse.Namespace) -> int:
    bc_root = str(Path(args.bc_root).resolve())
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
    bc_root = str(Path(args.bc_root).resolve())
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
    directory-glob walk. Missing bc_root directories are no longer
    relevant — the database is the store.
    """
    bc_root = str(Path(args.bc_root).resolve())
    rows = query_pending_inbox(bc_root)
    for work_id, message_type in rows:
        print(f"{work_id} {message_type}")
    return 0


def _cmd_pending_outbox(args: argparse.Namespace) -> int:
    """Enumerate pending outbox responses across sibling BC clones.

    Lead-side counterpart to `pending inbox`. Queries Postgres for
    outbox rows whose bc path sits under <lead-root>/repos/.
    """
    lead_root = str(Path(args.lead_root).resolve())
    rows = query_pending_outbox(lead_root, bc_filter=args.bc)
    for work_id, message_type, bc_name in rows:
        print(f"{work_id} {message_type} {bc_name}")
    return 0


def _cmd_dump(args: argparse.Namespace) -> int:
    """Operator debugging: dump rows from the messages table."""
    import json as _json
    from shop_msg.storage import _connect, _bc_id

    bc_root = str(Path(args.bc_root).resolve()) if args.bc_root else None
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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="shop-msg")
    sub = parser.add_subparsers(dest="command", required=True)

    respond = sub.add_parser("respond", help="write a BC response message")
    respond_sub = respond.add_subparsers(dest="response_type", required=True)

    clarify = respond_sub.add_parser("clarify", help="write a clarify response")
    clarify.add_argument("--bc-root", required=True, help="BC root directory")
    clarify.add_argument("--work-id", required=True, help="work_id from the lead message")
    clarify.add_argument("--question", required=True, help="clarifying question text")
    clarify.set_defaults(func=_cmd_respond_clarify)

    work_done = respond_sub.add_parser("work_done", help="write a work_done response")
    work_done.add_argument("--bc-root", required=True, help="BC root directory")
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
    work_done.set_defaults(func=_cmd_respond_work_done)

    mech_obs = respond_sub.add_parser(
        "mechanism_observation",
        help="surface a BC observation about the shop-system mechanism",
    )
    mech_obs.add_argument("--bc-root", required=True, help="BC root directory")
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
    mech_obs.set_defaults(func=_cmd_respond_mechanism_observation)

    send = sub.add_parser("send", help="write a lead-to-BC message into a BC's inbox")
    send_sub = send.add_subparsers(dest="message_type", required=True)

    request_maintenance = send_sub.add_parser(
        "request_maintenance", help="write a request_maintenance message"
    )
    request_maintenance.add_argument("--bc-root", required=True, help="BC root directory")
    request_maintenance.add_argument(
        "--work-id", required=True, help="work_id identifying this assignment"
    )
    request_maintenance.add_argument(
        "--description", required=True, help="description of the work being requested"
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
    request_maintenance.set_defaults(func=_cmd_send_request_maintenance)

    assign_scenarios = send_sub.add_parser(
        "assign_scenarios", help="write an assign_scenarios message"
    )
    assign_scenarios.add_argument("--bc-root", required=True, help="BC root directory")
    assign_scenarios.add_argument(
        "--work-id", required=True, help="work_id identifying this assignment"
    )
    assign_scenarios.add_argument(
        "--feature-title",
        required=True,
        help="title used in the wrapping `Feature:` line for each scenario",
    )
    assign_scenarios.add_argument(
        "--bc-tag",
        required=True,
        help="BC name used in the @bc:<name> scenario tag",
    )
    assign_scenarios.add_argument(
        "--scenario-file",
        action="append",
        default=None,
        required=True,
        help="path to a file containing one scenario body (repeatable)",
    )
    assign_scenarios.set_defaults(func=_cmd_send_assign_scenarios)

    request_bugfix = send_sub.add_parser(
        "request_bugfix", help="write a request_bugfix message"
    )
    request_bugfix.add_argument("--bc-root", required=True, help="BC root directory")
    request_bugfix.add_argument(
        "--work-id", required=True, help="work_id identifying this assignment"
    )
    request_bugfix.add_argument(
        "--description",
        required=True,
        help="plain-language description of the fix",
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
    request_bugfix.set_defaults(func=_cmd_send_request_bugfix)

    read = sub.add_parser("read", help="read a message from a BC's mailboxes")
    read_sub = read.add_subparsers(dest="read_target", required=True)

    read_outbox = read_sub.add_parser(
        "outbox", help="read and validate a BC response from its outbox"
    )
    read_outbox.add_argument("--bc-root", required=True, help="BC root directory")
    read_outbox.add_argument(
        "--work-id", required=True, help="work_id whose response to read"
    )
    read_outbox.set_defaults(func=_cmd_read_outbox)

    read_inbox = read_sub.add_parser(
        "inbox", help="read and validate a lead message from a BC's inbox"
    )
    read_inbox.add_argument("--bc-root", required=True, help="BC root directory")
    read_inbox.add_argument(
        "--work-id", required=True, help="work_id whose inbox message to read"
    )
    read_inbox.set_defaults(func=_cmd_read_inbox)

    pending = sub.add_parser(
        "pending", help="enumerate pending mailbox entries (queries, not gates)"
    )
    pending_sub = pending.add_subparsers(dest="pending_target", required=True)

    pending_inbox = pending_sub.add_parser(
        "inbox",
        help="list inbox messages with no matching outbox response (BC side)",
    )
    pending_inbox.add_argument("--bc-root", required=True, help="BC root directory")
    pending_inbox.set_defaults(func=_cmd_pending_inbox)

    pending_outbox = pending_sub.add_parser(
        "outbox",
        help="list pending outbox responses across sibling BC clones (lead side)",
    )
    pending_outbox.add_argument(
        "--lead-root",
        required=True,
        help="lead-shop root containing a repos/ directory of sibling BC clones",
    )
    pending_outbox.add_argument(
        "--bc",
        default=None,
        help="restrict to a single BC name (must match the directory under repos/)",
    )
    pending_outbox.set_defaults(func=_cmd_pending_outbox)

    dump = sub.add_parser(
        "dump",
        help="operator debugging: dump messages table rows to stdout as YAML",
    )
    dump.add_argument("--bc-root", default=None, help="restrict to this BC root path")
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

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
