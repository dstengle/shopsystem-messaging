"""shop-msg CLI entry point.

Subcommands:
    respond clarify --bc-root PATH --work-id ID --question TEXT
        Writes <bc-root>/outbox/<work_id>-clarify.yaml as a valid
        Clarify message (schema from the prototype's shared schemas
        module).
    respond work_done --bc-root PATH --work-id ID --status STATUS
                      [--scenario-hash HASH ...] [--summary TEXT]
        Writes <bc-root>/outbox/<work_id>-work_done.yaml as a valid
        WorkDone message.
    respond mechanism_observation --bc-root PATH --work-id ID --subject TEXT
                                  --body TEXT [--observed-during ID]
                                  [--evidence TEXT ...] [--proposed-action TEXT]
                                  [--provenance-ref REF]
        Writes <bc-root>/outbox/<work_id>-mechanism_observation.yaml as
        a valid MechanismObservation message. The optional
        --provenance-ref names a BC-side record (issue, document,
        commit) where long-form analysis lives; the wire schema does
        not constrain that reference to any particular tracker so the
        catalog stays decoupled from the BC's work-registry choice
        (lead-231 item C).
    send request_maintenance --bc-root PATH --work-id ID --description TEXT
                             [--acceptance-criterion TEXT ...]
                             [--file-hint TEXT ...]
        Writes <bc-root>/inbox/<work_id>.yaml as a valid
        RequestMaintenance message.
    send assign_scenarios --bc-root PATH --work-id ID --feature-title TEXT
                          --bc-tag NAME --scenario-file PATH ...
        Writes <bc-root>/inbox/<work_id>.yaml as a valid
        AssignScenarios message. Each --scenario-file becomes one
        ScenarioPayload. The hash for each scenario is computed by
        shelling out to the `scenarios hash` CLI (the canonicalization
        rule lives in the scenarios package, not here).
    send request_bugfix --bc-root PATH --work-id ID --description TEXT
                        [--feature-title TEXT --bc-tag NAME
                         --scenario-file PATH ...]
        Writes <bc-root>/inbox/<work_id>.yaml as a valid
        RequestBugfix message. Scenarios are optional: with none the
        message carries description-only fix instructions. If any
        --scenario-file is supplied, --feature-title and --bc-tag
        become required (they wrap each scenario body the same way
        assign_scenarios does).
    read outbox --bc-root PATH --work-id ID
        Reads the latest <bc-root>/outbox/<work_id>-*.yaml, validates it
        against the BCResponse union, and dumps the canonical YAML to
        stdout. Exits non-zero (with a stderr message) when no outbox
        file matches the work_id or validation fails.
    read inbox --bc-root PATH --work-id ID
        Reads <bc-root>/inbox/<work_id>.yaml, validates it against the
        LeadMessage union, and dumps the canonical YAML to stdout.
        Exits non-zero (with a stderr message) when no inbox file
        matches the work_id or validation fails.
    pending inbox --bc-root PATH
        Enumerates inbox messages that have no matching outbox response
        (a message is "pending" iff no <work_id>-*.yaml exists in the
        outbox for its work_id). Stdout is one line per pending message
        of the form "<work_id> <message_type>". Exit zero in both the
        empty and non-empty cases; this command is a query, not a gate.
        Lets routers and dispatch wrappers decide when to invoke the
        implementer/reviewer without inspecting the mailboxes directly.
    pending outbox --lead-root PATH [--bc NAME]
        Lead-side counterpart: walks sibling BC clones under
        <lead-root>/repos/ (one directory per BC) and enumerates outbox
        responses across them. With --bc NAME, scopes to a single BC
        directory; without it, every sibling under repos/ is included.
        A response is "pending" iff an outbox file exists for it (the
        lead has not yet drained / acted on it). Stdout is one line per
        pending response of the form "<work_id> <message_type> <bc>".
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
    bc_root = Path(args.bc_root)
    outbox = bc_root / "outbox"
    outbox.mkdir(parents=True, exist_ok=True)

    out_path = outbox / f"{args.work_id}-clarify.yaml"
    if out_path.exists():
        # Refuse to overwrite an existing outbox file for this work_id.
        # The §4.4 loop (BC clarify -> lead request_bugfix -> BC may
        # clarify again on the same work_id) recurs at exactly this
        # boundary; silent overwrite would destroy the prior clarify.
        print(
            f"shop-msg respond clarify: refusing to overwrite existing outbox file: {out_path}",
            file=sys.stderr,
        )
        return 1

    message = Clarify(
        message_type="clarify",
        work_id=args.work_id,
        question=args.question,
    )

    with out_path.open("w") as f:
        yaml.safe_dump(message.model_dump(), f, sort_keys=False)
    return 0


def _cmd_respond_work_done(args: argparse.Namespace) -> int:
    bc_root = Path(args.bc_root)
    outbox = bc_root / "outbox"
    outbox.mkdir(parents=True, exist_ok=True)

    out_path = outbox / f"{args.work_id}-work_done.yaml"
    if out_path.exists():
        # Refuse to overwrite an existing work_done file for this work_id.
        # Same reasoning as the clarify collision check: silently
        # clobbering a prior reply destroys the lead's reconciliation
        # record for that work_id.
        print(
            f"shop-msg respond work_done: refusing to overwrite existing outbox file: {out_path}",
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

    with out_path.open("w") as f:
        yaml.safe_dump(message.model_dump(), f, sort_keys=False)
    return 0


def _cmd_respond_mechanism_observation(args: argparse.Namespace) -> int:
    bc_root = Path(args.bc_root)
    outbox = bc_root / "outbox"
    outbox.mkdir(parents=True, exist_ok=True)

    # Path-safety: refuse work_ids that would escape the outbox dir.
    # The catalog schemas do not pin a pattern on WorkDone.work_id
    # (only Clarify.work_id is pattern-constrained) so historically the
    # CLI relied on the bd_ref pattern to keep this command's filename
    # safe. After lead-231 decoupling, bd_ref is gone; the equivalent
    # input-safety check has to live here. Use the same alphanumeric-
    # plus-safe-punctuation shape Clarify.work_id and provenance_ref
    # accept.
    if "/" in args.work_id or ".." in args.work_id or not args.work_id:
        print(
            f"shop-msg respond mechanism_observation: refusing unsafe "
            f"work_id {args.work_id!r}",
            file=sys.stderr,
        )
        return 1

    out_path = outbox / f"{args.work_id}-mechanism_observation.yaml"
    if out_path.exists():
        # Refuse to overwrite. Same reasoning as the other respond
        # collision checks: the work_id identifies one response
        # uniquely, and silently clobbering destroys the prior record.
        print(
            f"shop-msg respond mechanism_observation: refusing to overwrite "
            f"existing outbox file: {out_path}",
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

    # exclude_none=True: MechanismObservation has optional fields
    # (observed_during, evidence, proposed_action); omitting them keeps
    # the YAML compact and round-trip-safe. model_validate treats absent
    # keys as None on the receiving side. The respond clarify and respond
    # work_done handlers predate this convention.
    with out_path.open("w") as f:
        yaml.safe_dump(message.model_dump(exclude_none=True), f, sort_keys=False)
    return 0


def _cmd_send_request_maintenance(args: argparse.Namespace) -> int:
    bc_root = Path(args.bc_root)
    inbox = bc_root / "inbox"
    inbox.mkdir(parents=True, exist_ok=True)

    out_path = inbox / f"{args.work_id}.yaml"
    if out_path.exists():
        # Refuse to overwrite an existing inbox file for this work_id.
        # Same reasoning as the outbox collision checks: the lead sends one
        # message per work_id, and silently clobbering a prior message
        # destroys the BC's record of what was asked.
        print(
            f"shop-msg send request_maintenance: refusing to overwrite existing inbox file: {out_path}",
            file=sys.stderr,
        )
        return 1

    acceptance_criteria = list(args.acceptance_criterion or []) or None
    file_hints = list(args.file_hint or []) or None

    message = RequestMaintenance(
        message_type="request_maintenance",
        work_id=args.work_id,
        description=args.description,
        acceptance_criteria=acceptance_criteria,
        file_hints=file_hints,
    )

    with out_path.open("w") as f:
        yaml.safe_dump(message.model_dump(exclude_none=True), f, sort_keys=False)
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
    bc_root = Path(args.bc_root)
    inbox = bc_root / "inbox"
    inbox.mkdir(parents=True, exist_ok=True)

    out_path = inbox / f"{args.work_id}.yaml"
    if out_path.exists():
        # Refuse to overwrite an existing inbox file for this work_id.
        # Same reasoning as the other send-collision checks: silent
        # clobber would destroy the BC's record of what was asked.
        print(
            f"shop-msg send request_bugfix: refusing to overwrite existing inbox file: {out_path}",
            file=sys.stderr,
        )
        return 1

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

    with out_path.open("w") as f:
        yaml.safe_dump(message.model_dump(), f, sort_keys=False)
    return 0


def _cmd_send_assign_scenarios(args: argparse.Namespace) -> int:
    bc_root = Path(args.bc_root)
    inbox = bc_root / "inbox"
    inbox.mkdir(parents=True, exist_ok=True)

    out_path = inbox / f"{args.work_id}.yaml"
    if out_path.exists():
        # Refuse to overwrite an existing inbox file for this work_id.
        # Same reasoning as the request_maintenance collision check: the
        # lead sends one message per work_id; silent clobber would
        # destroy the BC's record of what was asked.
        print(
            f"shop-msg send assign_scenarios: refusing to overwrite existing inbox file: {out_path}",
            file=sys.stderr,
        )
        return 1

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

    with out_path.open("w") as f:
        yaml.safe_dump(message.model_dump(), f, sort_keys=False)
    return 0


def _cmd_read_outbox(args: argparse.Namespace) -> int:
    bc_root = Path(args.bc_root)
    outbox = bc_root / "outbox"
    candidates = sorted(outbox.glob(f"{args.work_id}-*.yaml"))
    if not candidates:
        print(
            f"shop-msg read outbox: no outbox response found for "
            f"work_id={args.work_id!r} in {outbox}",
            file=sys.stderr,
        )
        return 1
    path = candidates[-1]
    raw = yaml.safe_load(path.read_text())
    try:
        message = _response_adapter.validate_python(raw)
    except ValidationError as e:
        print(
            f"shop-msg read outbox: validation failed for {path.name}:\n{e}",
            file=sys.stderr,
        )
        return 1
    print(f"valid {message.message_type} from {path.name}:")
    print(yaml.safe_dump(message.model_dump(exclude_none=True), sort_keys=False))
    return 0


def _cmd_read_inbox(args: argparse.Namespace) -> int:
    bc_root = Path(args.bc_root)
    inbox = bc_root / "inbox"
    path = inbox / f"{args.work_id}.yaml"
    if not path.exists():
        # Same pattern as `read outbox`: a missing file is the
        # caller's mistake, not a schema problem; surface a phrase the
        # step definitions can substring-check ("no inbox message").
        print(
            f"shop-msg read inbox: no inbox message found for "
            f"work_id={args.work_id!r} in {inbox}",
            file=sys.stderr,
        )
        return 1
    raw = yaml.safe_load(path.read_text())
    try:
        message = _lead_adapter.validate_python(raw)
    except ValidationError as e:
        # The "validation failed" phrase is load-bearing — the step
        # definition for the schema-validation scenario substring-checks
        # it. Keep the wording aligned with `read outbox`'s sibling
        # branch so a future consolidation doesn't drift one without
        # the other.
        print(
            f"shop-msg read inbox: validation failed for {path.name}:\n{e}",
            file=sys.stderr,
        )
        return 1
    print(f"valid {message.message_type} from {path.name}:")
    print(yaml.safe_dump(message.model_dump(exclude_none=True), sort_keys=False))
    return 0


def _peek_message_type(path: Path) -> str:
    """Best-effort `message_type` extraction from a mailbox YAML file.

    Used by the pending-listing subcommands to label each entry without
    requiring full schema validation — a malformed file should still
    show up in the listing as pending (the caller will then read it,
    surface the schema error, and decide what to do). Falls back to
    "unknown" if the file does not parse as a mapping or omits the
    discriminator.
    """
    try:
        raw = yaml.safe_load(path.read_text())
    except yaml.YAMLError:
        return "unknown"
    if not isinstance(raw, dict):
        return "unknown"
    mt = raw.get("message_type")
    return mt if isinstance(mt, str) else "unknown"


def _cmd_pending_inbox(args: argparse.Namespace) -> int:
    """Enumerate inbox messages that have no matching outbox response.

    "Pending" iff no <work_id>-*.yaml exists in the outbox for the
    inbox file's work_id. This is the BC router's discriminator for
    "is there work to dispatch the implementer onto?". Missing
    inbox / outbox directories are treated as empty so a freshly-
    bootstrapped BC produces the empty-result case cleanly rather
    than an FS error.
    """
    bc_root = Path(args.bc_root)
    inbox = bc_root / "inbox"
    outbox = bc_root / "outbox"
    inbox_files = sorted(inbox.glob("*.yaml")) if inbox.exists() else []
    for path in inbox_files:
        work_id = path.stem
        # An outbox file matching <work_id>-*.yaml means the BC has
        # already responded (clarify, work_done, or other) — so this
        # inbox is no longer pending. Globbing keeps the rule
        # response-type-agnostic.
        if outbox.exists() and any(outbox.glob(f"{work_id}-*.yaml")):
            continue
        mt = _peek_message_type(path)
        print(f"{work_id} {mt}")
    return 0


def _cmd_pending_outbox(args: argparse.Namespace) -> int:
    """Enumerate pending outbox responses across sibling BC clones.

    Lead-side counterpart to `pending inbox`. Walks <lead-root>/repos/
    looking for sibling BC directories (each with an outbox/ child) and
    enumerates the outbox files. With --bc NAME, scopes to one sibling.
    A response is "pending" iff its file exists — there is no lead-side
    "consumed" marker yet, so existence is the only signal available.
    Returns one line per response of the form
    "<work_id> <message_type> <bc>".
    """
    lead_root = Path(args.lead_root)
    repos_root = lead_root / "repos"
    if not repos_root.exists():
        # No sibling BCs visible; cleanly produce the empty case.
        return 0
    if args.bc is not None:
        bc_dirs = [repos_root / args.bc]
    else:
        bc_dirs = sorted(p for p in repos_root.iterdir() if p.is_dir())
    for bc_dir in bc_dirs:
        outbox = bc_dir / "outbox"
        if not outbox.exists():
            continue
        for path in sorted(outbox.glob("*.yaml")):
            # Outbox filenames are "<work_id>-<response_type>.yaml" per
            # the BC respond commands. Split on the last hyphen-before-
            # ".yaml" to recover work_id; if the filename does not match
            # that shape (e.g. an old hand-rolled file), fall back to
            # the stem so the entry still surfaces.
            stem = path.stem
            # Response type is the trailing chunk; pre-known set keeps
            # the parse robust against work_ids that themselves contain
            # hyphens (e.g. "lead-301").
            for response_type in ("clarify", "work_done", "mechanism_observation"):
                suffix = f"-{response_type}"
                if stem.endswith(suffix):
                    work_id = stem[: -len(suffix)]
                    mt = response_type
                    break
            else:
                work_id = stem
                mt = _peek_message_type(path)
            print(f"{work_id} {mt} {bc_dir.name}")
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
            "respond clarify / respond work_done flag of the same name; "
            "drives the outbox filename <work_id>-mechanism_observation.yaml."
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

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
