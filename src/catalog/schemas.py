import re
from typing import Annotated, Literal, Union
from pydantic import BaseModel, ConfigDict, Field, model_validator

# Delegate the canonical scenario-hash to the scenarios package. The
# canonical rule is SCENARIO-BLOCK-ONLY (ADR-019 D1/D2, scenario 117):
# exactly one canonical hash text per scenario block — the Scenario /
# Scenario Outline keyword line through its steps / Examples, with NO
# surrounding @-tag lines and NO `Feature:` header line. Its true home is
# `scenarios.outstanding.parse_then_block_only_hash` (the SAME in-process
# entry point the `scenarios hash` CLI delegates to since it was reconciled
# to parse-then-block-only, ADR-056 D5). We re-export it under the local
# name `_canonical_scenario_hash` because (a) ScenarioPayload's validator
# below still calls it under that name and (b) the cross-package agreement
# test in tests/integration/test_catalog_scenarios_agreement.py imports it
# from `catalog.schemas` and asserts it matches the `scenarios hash` CLI
# output. Keeping the export name stable lets that test continue to pin the
# catalog-side contract — what changes is that the implementation is now
# imported rather than duplicated.
#
# History: this re-export previously bound `scenarios.hash.compute_scenario_hash`,
# which canonicalizes WHOLE-TEXT — it strips blank and `@scenario_hash:`
# lines but RETAINS a standalone `@bc:`/`@origin:` tag line and any
# `Feature:` line. That violated the block-only rule (ADR-019 D2) and
# diverged from the block-only on-disk `@scenario_hash:` pins for any body
# whose @bc/@origin/Feature sat on a separate line; it agreed with the old
# whole-text CLI only by coincidence, masked because the agreement test's
# sample bodies were pure scenario blocks (block-only == whole-text). Per
# ADR-019 D2 / ADR-060 messaging must DELEGATE to the scenarios block-only
# entry point, never re-enact canonicalization; this re-export is that
# delegation.
from scenarios.outstanding import (
    parse_then_block_only_hash as _canonical_scenario_hash,
)


# Canonical work_id grammar, shared by EVERY message-type schema (lead-4wy).
#
# History / the bug this fixes: work_id constraints had drifted apart across
# the catalog. Clarify.work_id and Nudge.work_id carried `^[a-zA-Z0-9-]+$`
# (hyphens only, dots REJECTED), while AssignScenarios / RequestBugfix /
# RequestMaintenance / WorkDone / RequestCompletionJournal[Response] left
# work_id as a bare `str` that accepted anything — including dotted lead
# child-bead ids like `lead-231.1` AND path separators like `../escape`.
# The asymmetry was latent: a dispatch sent with a dotted work_id (accepted
# by assign_scenarios/request_bugfix) could not be answered, because
# `shop-msg respond clarify --work-id lead-231.1` failed Clarify validation
# on the dot. Cross-message-type symmetry broke at clarify-time.
#
# The fix is ONE canonical pattern applied via ONE shared type alias so the
# same work_id is valid/invalid on every vehicle:
#
#   ^[A-Za-z0-9][A-Za-z0-9_.-]*$
#
#   - First char must be alphanumeric, so leading dots (`.hidden`) and the
#     `..` path-escape are rejected.
#   - Subsequent chars may be alphanumerics, `_`, `.`, or `-`, so dotted
#     child-bead ids (`lead-231.1`) round-trip through every vehicle,
#     including Clarify.
#   - Slashes and whitespace are absent from the charset, so path separators
#     (`../escape`, `foo/bar`) and whitespace stay rejected — preserving the
#     Clarify input-safety hardening (lead-008) that the path-separator and
#     empty-work_id scenarios pin.
#
# This is the same shape as MechanismObservation.provenance_ref's input-safety
# pattern; reusing it keeps the catalog's identifier-safety story uniform.
#
# Symmetry is regression-pinned two ways (lead-qbbk): per-vehicle in
# tests/test_work_id_pattern_symmetry.py, and as a direct cross-type
# accept/reject AGREEMENT battery in tests/test_work_id_symmetry_battery.py
# (the same work_id yields one verdict across all 8 vehicles). Changing this
# single constant — or reintroducing a per-schema work_id constraint — breaks
# the agreement battery, which is the point: the pattern lives here ONCE.
_WORK_ID_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9_.-]*$"

# Required work_id field, shared by every message type that carries one.
WorkId = Annotated[str, Field(min_length=1, pattern=_WORK_ID_PATTERN)]

# Optional work_id field (only Nudge: a bare liveness ping need not reference
# a dispatch). Same grammar when present.
OptionalWorkId = Annotated[
    str | None, Field(default=None, min_length=1, pattern=_WORK_ID_PATTERN)
]


# Matches an "@bc:<name>" token where <name> is one or more non-space
# characters. Used by ScenarioPayload to enforce that every scenario's
# gherkin body declares which BC owns the scenario, regardless of which
# tool constructed the payload (lead CLI, hand-rolled tests, future
# automation).
#
# The token form is anchored (^...$) because we apply it to whitespace-
# split tokens of a single line, not to the gherkin body as a whole.
# An earlier version used `re.compile(r"@bc:\S+").search(gherkin)` over
# the entire string, which accepted gherkin whose only @bc: occurrence
# was inside a step's quoted content (e.g. `Given the file mentions
# "@bc:fake" in passing`). The intent is "the gherkin has a tag-line
# containing @bc:<name>", so we walk lines, split on whitespace, and
# require at least one token to match this anchored pattern. That
# matches how pytest-bdd tag lines are actually shaped — a sequence of
# whitespace-separated `@tag` tokens — without permitting substring
# matches inside step bodies.
_BC_TAG_TOKEN_RE = re.compile(r"^@bc:\S+$")


def _gherkin_has_bc_tag_line(gherkin: str) -> bool:
    """True if some line in `gherkin` contains an @bc:<name> token.

    A "token" here is what you get from `str.split()` on the line — a
    whitespace-bounded run of non-space characters. This rejects @bc:
    appearing inside a quoted step phrase like
    `Given the body contains "@bc:fake"` because the surrounding quote
    characters bind to the token, leaving `"@bc:fake"` rather than the
    bare `@bc:fake` we require.
    """
    for line in gherkin.splitlines():
        for token in line.split():
            if _BC_TAG_TOKEN_RE.match(token):
                return True
    return False


class RequestMaintenance(BaseModel):
    message_type: Literal["request_maintenance"]
    work_id: WorkId
    description: str
    acceptance_criteria: list[str] | None = None
    file_hints: list[str] | None = None
    # from_shop names the sender (the shop that ran `shop-msg send`).
    # When the sender is resolved implicitly from CWD (PDR-008), the CLI
    # populates this field with the canonical name read from the sender
    # shop's .claude/shop/name.md. None when not resolved.
    from_shop: str | None = None


class ScenarioPayload(BaseModel):
    hash: str
    tags: list[str] = Field(default_factory=list)
    gherkin: str

    @model_validator(mode="after")
    def _gherkin_must_carry_bc_tag(self) -> "ScenarioPayload":
        # The @bc:<name> tag identifies which Bounded Context owns the
        # scenario. Previously enforced only by the lead-side CLI's
        # --bc-tag flag, which left a hand-constructed ScenarioPayload
        # free to skip it. Promoting the check to schema level means
        # every construction site (CLI, tests, future tools) gets the
        # same guarantee. The token must appear as a whitespace-bounded
        # tag on some line, not merely as a substring — see
        # `_gherkin_has_bc_tag_line` for why.
        if not _gherkin_has_bc_tag_line(self.gherkin):
            raise ValueError(
                "ScenarioPayload.gherkin must contain a line with a "
                "@bc:<name> tag (e.g. '@bc:shop-msg'); none was found."
            )
        return self

    @model_validator(mode="after")
    def _hash_must_match_canonical_body_hash(self) -> "ScenarioPayload":
        # The hash field is load-bearing: the lead emits it in
        # `work_done.scenario_hashes`, the BC echoes it back, and the
        # lead reconciles. Previously the only guarantee that
        # `hash == canonical_hash(gherkin)` came from the lead-side
        # `shop-msg send` CLI's hash-computation step, which left a
        # hand-constructed or test-constructed ScenarioPayload free to
        # carry mismatched values. Promoting the check to schema level
        # means every construction site (CLI, hand-rolled tests, future
        # automation reading YAML) gets the same guarantee.
        #
        # The canonicalization rule is owned by the scenarios package;
        # `_canonical_scenario_hash` above is a re-export of
        # `scenarios.hash.compute_scenario_hash`. See the import-site
        # docstring at the top of this module for why the local name
        # is kept stable, and the agreement test under
        # tests/integration/test_catalog_scenarios_agreement.py for
        # the cross-package contract pin.
        expected = _canonical_scenario_hash(self.gherkin)
        if self.hash != expected:
            raise ValueError(
                "ScenarioPayload.hash does not match the canonical "
                f"scenario-hash of the gherkin body: hash={self.hash!r} "
                f"but canonical(gherkin)={expected!r}."
            )
        return self


class AssignScenarios(BaseModel):
    message_type: Literal["assign_scenarios"]
    work_id: WorkId
    scenarios: list[ScenarioPayload]
    # See RequestMaintenance.from_shop. Populated by `shop-msg send` when
    # the sender is resolved implicitly from CWD (PDR-008).
    from_shop: str | None = None


class RequestBugfix(BaseModel):
    message_type: Literal["request_bugfix"]
    work_id: WorkId
    description: str
    scenarios: list[ScenarioPayload] = Field(default_factory=list)
    # See RequestMaintenance.from_shop. Populated by `shop-msg send` when
    # the sender is resolved implicitly from CWD (PDR-008).
    from_shop: str | None = None


class Nudge(BaseModel):
    """Operational-liveness signal between lead and BC (ADR-015 / lead-1w7r).

    A nudge is an auxiliary signal — "are you stuck?", "I'm stuck", "the
    predecessor landed", or a general heads-up — that is NOT subject to the
    dispatch lifecycle (ADR-015 decision 6). It carries no scenario state
    (ADR-015 decision 7): a nudge that references a work_id references the
    dispatch by ID only and makes no claim about scenario coverage.

    Closed reason enum (ADR-015 decision 2):
      stuck-on-you       -- the sender is blocked waiting on the recipient
      status-check       -- the sender is asking for a liveness/progress ping
      predecessor-landed -- a dependency the recipient was waiting on has landed
      general            -- catch-all; the reason itself carries no semantics,
                            so --note is REQUIRED for this value only.

    The `--note` requirement is asymmetric (lead-1w7r decision, scenario
    4abbd813c588af06): mandatory for ``general`` (where the reason alone
    communicates nothing), opportunistic for the three semantic reasons.

    Transmission-layer purity (ADR-015 decision 7): the schema has no
    ``scenario_hashes`` field, and a payload carrying one is rejected at
    construction by ``_reject_scenario_state``.
    """
    message_type: Literal["nudge"]
    reason: Literal[
        "stuck-on-you", "status-check", "predecessor-landed", "general"
    ]
    # work_id is optional: a nudge MAY reference an in-flight dispatch by id,
    # but a bare liveness ping need not. When present it is path-safe-shaped
    # and uses the SHARED canonical work_id grammar (lead-4wy), so a dotted
    # child-bead id round-trips through a nudge exactly as it does through
    # every other vehicle.
    work_id: OptionalWorkId = None
    note: str | None = None
    # See RequestMaintenance.from_shop. Populated by `shop-msg` when the
    # sender is resolved implicitly from CWD (PDR-008).
    from_shop: str | None = None

    @model_validator(mode="after")
    def _note_required_for_general(self) -> "Nudge":
        # ADR-015 decision 2 / lead-1w7r: --note is mandatory only for the
        # catch-all reason "general", where the reason itself communicates no
        # semantics. The three semantic reasons accept but do not require it.
        if self.reason == "general" and not (self.note and self.note.strip()):
            raise ValueError(
                "Nudge with reason='general' requires a non-empty note: the "
                "'general' reason carries no semantics of its own, so the note "
                "is the only signal. Supply --note."
            )
        return self


def _nudge_payload_rejects_scenario_state(data: object) -> None:
    """Reject a nudge payload that carries scenario state (ADR-015 decision 7).

    Nudges are purely transmission-layer: they MUST NOT carry a
    ``scenario_hashes`` field. The Nudge model itself has no such field, so a
    plain ``Nudge(**data)`` would silently *drop* an extra key rather than
    reject it. This helper is the explicit guard the CLI invokes on the raw
    payload dict BEFORE constructing the model, so an adversarial payload file
    that smuggles ``scenario_hashes`` is named and rejected at the surface.
    """
    if isinstance(data, dict) and "scenario_hashes" in data:
        raise ValueError(
            "nudge payload carries a 'scenario_hashes' field, but a nudge MUST "
            "NOT carry scenario state per ADR-015 decision 7. A nudge that "
            "references a work_id references the dispatch lifecycle by ID only "
            "and makes no claim about scenario coverage. Remove 'scenario_hashes'."
        )


class Clarify(BaseModel):
    message_type: Literal["clarify"]
    # work_id uses the SHARED canonical work_id grammar (lead-4wy): a safe
    # identifier shape that still rejects path separators ("/", ".."),
    # whitespace, and empty strings — so callers (CLI and any future tools)
    # cannot weaponize the value via crafted arguments — while ADMITTING
    # dotted lead child-bead ids (`lead-231.1`). Previously this field
    # carried `^[a-zA-Z0-9-]+$`, which rejected the dot and made a dotted
    # dispatch unanswerable at clarify-time; the shared alias closes that
    # asymmetry so the same work_id is valid on every message type.
    work_id: WorkId
    question: str = Field(min_length=1)


class WorkDone(BaseModel):
    message_type: Literal["work_done"]
    work_id: WorkId
    status: Literal["complete", "partial", "blocked"]
    summary: str | None = None
    scenario_hashes: list[str] = Field(default_factory=list)


class MechanismObservation(BaseModel):
    """BC -> lead observation about the shop-system mechanism itself.

    Surfaced alongside `work_done` (or, less commonly, ambient outside any
    directed work) when the BC notices something load-bearing about
    templates, schemas, role discipline, package boundaries, or the
    spec — anything that is mechanism-of-the-system rather than a
    property of the work item itself.

    Carve-outs per design (see `prototypes/mechanism-observation-v1/design.md`):
    - Property of the scenario / work item -> `clarify`
    - Implementation block -> `work_done(blocked)`
    - Mechanism-of-the-system -> `mechanism_observation`

    Three-artifact pattern: when the BC keeps long-form analysis in its
    own work-tracker, it can reference that record via the optional
    `provenance_ref` field; the lead's drain may then create a
    corresponding lead-side record that references back. The wire
    message itself does not name any particular tracker — beads,
    GitHub Issues, or any other — so the catalog stays decoupled from
    the lead-side and BC-side choices of work registry (per brief 001
    item C / lead-231). Long-form analysis lives in the referenced
    record, not in this message.
    """
    message_type: Literal["mechanism_observation"]
    subject: str = Field(min_length=5, max_length=120)
    observed_during: str | None = None
    body: str = Field(min_length=50)
    evidence: list[Annotated[str, Field(min_length=1)]] | None = Field(default=None, min_length=1)
    proposed_action: str | None = None
    # provenance_ref is an optional, tracker-neutral pointer to a BC-side
    # record where long-form analysis lives. The catalog deliberately does
    # not constrain its shape to a beads issue-id pattern (lead-231 item
    # C: schemas must not assume beads participation). The constraint is
    # only path-safety — no slashes, no path separators — preserving the
    # same hardening Clarify.work_id (lead-008) and the prior bd_ref
    # field had, but without the beads-specific shape.
    provenance_ref: str | None = Field(
        default=None, min_length=1, pattern=r"^[A-Za-z0-9][A-Za-z0-9_.-]*$"
    )


class ClarifyResponse(BaseModel):
    """Lead -> BC in-band answer to an outstanding BC clarify (lead-ox8).

    A clarify_response delivers the lead's answer to a clarify the BC raised,
    RE-OPENING the original dispatch on the SAME work_id for the BC's gated
    loop to resume — no new work_id and no new bead are minted. It coexists
    with the original dispatch inbox row (it opts into allow_multi_type at the
    storage layer, the same coexistence mechanism work_done/mechanism_observation
    use), distinguished by its message_type discriminator.

    Transmission-layer purity (mirrors Nudge / ADR-015 decision 7): the schema
    has NO ``scenario_hashes`` field and forbids extra keys, so a payload that
    smuggles scenario state is REJECTED at construction rather than silently
    dropped. This is the bounding constraint pinned by ADR-009 layer (b) /
    ADR-027: because clarify_response cannot carry scenarios, an answer that
    would change the contract or add/tighten scenarios CANNOT be sent as a
    clarify_response — it MUST route to re-dispatch (assign_scenarios /
    request_bugfix).
    """
    model_config = ConfigDict(extra="forbid")

    message_type: Literal["clarify_response"]
    # work_id uses the SHARED canonical work_id grammar (lead-qbbk): a dotted
    # child-bead id round-trips, while path separators and whitespace stay
    # rejected. clarify_response answers an EXISTING dispatch, so it reuses
    # the same work_id grammar every other vehicle carries — no asymmetric
    # per-schema pattern is reintroduced here.
    work_id: WorkId
    # The lead's answer text. Required and non-empty: a clarify_response with
    # no resolution answers nothing.
    resolution: str = Field(min_length=1)
    # See RequestMaintenance.from_shop. Populated by `shop-msg send` when the
    # sender is resolved implicitly from CWD (PDR-008).
    from_shop: str | None = None


class RequestCompletionJournal(BaseModel):
    """Lead -> BC request for a target BC's completion journal (lead-f1ui).

    A request_completion_journal asks the named target bounded context for the
    set of block-only canonical scenario hashes it has completed. The request
    is purely a *request*: it names the target BC whose completed scenarios are
    sought and carries NO scenario-completion entry of its own — the completed
    set travels back on the paired ``RequestCompletionJournalResponse``, not on
    this request. (Deliberately no ``completed_entries`` field here; a request
    that carried completion state would conflate the ask with the answer.)
    """
    message_type: Literal["request_completion_journal"]
    work_id: WorkId
    # The bounded context whose completed scenarios are sought. Required: a
    # completion-journal request is meaningless without naming its target.
    target_bc: str = Field(min_length=1)
    # See RequestMaintenance.from_shop. Populated by `shop-msg send` when the
    # sender is resolved implicitly from CWD (PDR-008).
    from_shop: str | None = None


class RequestCompletionJournalResponse(BaseModel):
    """BC -> requester response carrying a completion journal (lead-f1ui).

    The paired response to ``RequestCompletionJournal``. It carries the set of
    block-only canonical scenario hashes the target BC has completed, back to
    the requester. ``completed_entries`` is a BARE SET of hash strings — not a
    list of per-entry records: the journal is identified by hash alone, with no
    additional per-entry metadata. Set semantics mean duplicate hashes collapse
    and order is not significant; the wire form (JSON) serializes the set as an
    array, but the in-model contract is a ``set[str]``.
    """
    message_type: Literal["request_completion_journal_response"]
    work_id: WorkId
    # The completed block-only canonical scenario hashes, as a bare set. A set
    # (not a list) so the schema itself enforces "no per-entry record beyond
    # the hash" and de-duplicates. Defaults to the empty set: a target BC that
    # has completed nothing yet returns an empty journal.
    completed_entries: set[str] = Field(default_factory=set)


class RegisterNarrowing(BaseModel):
    """Optional narrowing selector on a ``RequestScenarioRegister`` (lead-cl1u).

    A scenario-register request defaults to the target BC's FULL register: when
    the request omits its ``narrowing`` field entirely (it is ``None``), the
    request denotes the whole register, not a subset. This model is the OPTIONAL
    selector that confines the request to a narrower surface — either:

      - a named feature-area surface (``feature_area``), or
      - an explicit set of block-only canonical hashes (``hashes``).

    Both fields are individually optional so a caller may narrow by area, by an
    explicit list of hashes, or (degenerately) by neither; the meaningful
    confinement is supplied by whichever field the caller populates.

    ``hashes`` is an ORDERED LIST, not a set: the wire form of this message is
    JSON (deposited via ``insert_message``'s ``json.dumps(payload)``), and a
    Python ``set`` is not JSON-serializable — a set field crashes the send path
    with ``TypeError: Object of type set is not JSON serializable`` (lead-jo9p).
    A list serializes cleanly and preserves the caller's supplied order.
    """
    model_config = ConfigDict(extra="forbid")

    # A named feature-area surface to confine the request to. Optional.
    feature_area: str | None = Field(default=None, min_length=1)
    # An explicit list of block-only canonical hashes to confine the request to.
    # A list (not a set): JSON-serializable on the wire and order-preserving.
    hashes: list[str] | None = Field(default=None)


class RequestScenarioRegister(BaseModel):
    """Lead -> BC request for a target BC's scenario register (lead-cl1u).

    A request_scenario_register asks the named target bounded context for its
    scenario register — the set of pinned scenarios, each described by its
    block-only canonical hash, title and step text, features/ file location, and
    live-or-retired status. The register itself travels back on the paired
    ``RequestScenarioRegisterResponse``; THIS message is purely the *request*.

    It names the target BC whose register is sought and carries NO register
    entry of its own (deliberately no ``register_entries`` field here — a request
    that carried register state would conflate the ask with the answer, exactly
    as RequestCompletionJournal avoids carrying ``completed_entries``).

    The ``narrowing`` selector is OPTIONAL. Omitting it (``None``) denotes the
    target BC's FULL register rather than any subset; supplying a
    ``RegisterNarrowing`` confines the request to a named feature-area surface or
    to an explicit set of block-only canonical hashes.
    """
    model_config = ConfigDict(extra="forbid")

    message_type: Literal["request_scenario_register"]
    work_id: WorkId
    # The bounded context whose scenario register is sought. Required: a
    # scenario-register request is meaningless without naming its target.
    target_bc: str = Field(min_length=1)
    # The OPTIONAL narrowing selector. None (the default) denotes the target
    # BC's FULL register; a RegisterNarrowing confines it to a subset.
    narrowing: RegisterNarrowing | None = None
    # See RequestMaintenance.from_shop. Populated by `shop-msg send` when the
    # sender is resolved implicitly from CWD (PDR-008).
    from_shop: str | None = None


class ScenarioRegisterEntry(BaseModel):
    """One per-scenario entry in a scenario register (lead-cl1u).

    Unlike the bare-hash completion journal — where a completed scenario is
    identified by hash ALONE — a scenario-register entry carries the full
    per-entry record the requester needs to LOCATE, IMPORT, or SUPERSEDE the
    pinned scenario from the response alone, with no out-of-band lookup:

      - ``hash``          the scenario's block-only canonical hash
      - ``title``         the scenario's title
      - ``text``          the scenario's step text
      - ``file_location`` the scenario's features/ file location within the
                          target BC's tree
      - ``status``        whether the scenario is ``live`` or ``retired``
                          (superseded)

    Every one of these fields is REQUIRED — that is the teeth. An entry supplying
    ONLY a bare hash, with no title, step text, file location, or status, is
    rejected as schema-invalid. ``extra="forbid"`` additionally rejects unknown
    keys so a malformed entry cannot smuggle metadata past the contract.
    """
    model_config = ConfigDict(extra="forbid")

    hash: str = Field(min_length=1)
    title: str = Field(min_length=1)
    text: str = Field(min_length=1)
    file_location: str = Field(min_length=1)
    status: Literal["live", "retired"]


class RequestScenarioRegisterResponse(BaseModel):
    """BC -> requester response carrying a scenario register (lead-cl1u).

    The paired response to ``RequestScenarioRegister``. It carries the target
    BC's scenario register back to the requester as a LIST of per-entry records
    (``register_entries``) — NOT a bare set of hashes. Each entry is a
    ``ScenarioRegisterEntry`` exposing the scenario's hash, title, step text,
    features/ file location, and live-or-retired status, so the requester can
    locate / import / supersede each pinned scenario from the response alone.

    This is the per-entry richness that distinguishes the scenario register from
    the bare-hash completion journal: a register entry supplying ONLY a bare hash
    is rejected at construction because every per-entry field beyond the hash is
    required (see ``ScenarioRegisterEntry``).
    """
    model_config = ConfigDict(extra="forbid")

    message_type: Literal["request_scenario_register_response"]
    work_id: WorkId
    # The register entries, as a list of per-entry records. A list (not a bare
    # set of hashes): the register is a sequence of full ScenarioRegisterEntry
    # records, each carrying the metadata required to act on the pinned scenario
    # from the response alone. Defaults to empty: a target BC whose register is
    # empty returns an empty register.
    register_entries: list[ScenarioRegisterEntry] = Field(default_factory=list)


# A nudge is auxiliary signaling that flows BOTH directions: lead -> BC
# (`shop-msg send nudge`) and BC -> lead (`shop-msg nudge`). It is therefore
# a member of both message unions. It is NOT a dispatch (no lifecycle) and
# not a work-response (no scenario state) — ADR-015 decisions 6 & 7.
LeadMessage = Union[
    RequestMaintenance,
    AssignScenarios,
    RequestBugfix,
    RequestCompletionJournal,
    RequestScenarioRegister,
    ClarifyResponse,
    Nudge,
]
BCResponse = Union[
    Clarify,
    WorkDone,
    MechanismObservation,
    RequestCompletionJournalResponse,
    RequestScenarioRegisterResponse,
    Nudge,
]
