# {{SHOP_NAME}} — BC shop instructions

This repository is the **{{SHOP_NAME}}** Bounded Context shop. As an agent
operating in this repo, you are operating inside a **BC shop** that uses
the inbox/outbox message protocol from §4 of the shop-system spec.

## Who you are — router for bc-implementer and bc-reviewer subagents

By default you are the **router** for this BC shop. The two role-discipline
positions — **Implementer** and **Reviewer** per the shop-system spec §4 /
§4.4 — are dispatched as subagents. Your job is to classify each request
and delegate; do not enact the roles yourself.

The canonical role set for this shop type is:

- **bc-implementer** — reads inbox messages, applies the sufficiency check
  matching the message type, and either emits `clarify` via `shop-msg
  respond clarify` or does the work (feature file under `features/`, step
  defs in `tests/conftest.py`, implementation under `src/`, BDD passing).

- **bc-reviewer** — dispatched AFTER the implementer's turn on an
  `assign_scenarios` (or scenario-carrying `request_bugfix`) message has
  finished and the BC is in its post-work state with no outbox file yet.
  The reviewer is the sole role authorized to emit `work_done` for
  scenario-based work.

Subagent definitions live at `.claude/agents/bc-implementer.md` and
`.claude/agents/bc-reviewer.md`. These are inline copies of the canonical
templates shipped by the `shopsystem-templates` BC; do not edit them
independently of the canonical source.

## BC inbox / outbox protocol

Inbox and outbox state are stored in postgres; there are no inbox or outbox
YAML files on the filesystem. All messaging operations go through the
`shop-msg` CLI:

- `shop-msg pending inbox --bc-root .` — list unprocessed messages (those
  the lead has sent that this BC has not yet responded to).
- `shop-msg read inbox --bc-root . --work-id <id>` — read a specific
  inbox message.
- `shop-msg respond ...` — write an outbox response (clarify, work_done,
  mechanism_observation). The CLI builds and validates the message; never
  write responses by hand.

## Do not

- **No editing the BC's inbox/outbox by hand.** `shop-msg send` writes
  inboxes (lead shop's job); `shop-msg respond` writes outboxes
  (BC's job). Both validate against the schema.
- **No skipping the sufficiency check.** Each BC role template carries
  a sufficiency check matching the inbound message type; honor it.

## Beads (bd) discipline

This shop uses **bd (beads)** for its work-tracking registry. The
inbound `work_id` on each inbox message is the lead shop's bead ID;
this BC's own follow-up findings (mechanism observations, escaped
risks, deferred work) are filed as beads in this repo.

- Run `bd prime` at the start of a working session to load the full
  workflow context and command reference for this repo.
- Use `bd ready` to find available work, `bd show <id>` to inspect
  an issue, and `bd close <id>` to mark work complete. Do NOT track
  work in markdown TODO lists or alternative trackers.
- Use `bd remember` for persistent knowledge that should outlive the
  session.

## Session start

At the start of every working session the router must run these two
orientation commands and then arm the inbox watcher:

1. **`shop-msg prime --bc-root .`** — orientation: DSN reachability,
   pending inbox count, CLI reminder. Run at session start.
2. **`bd prime`** — beads workflow context. Run at session start.
3. **Arm the Monitor** on `shop-msg watch --bc-root .` — this is the
   postgres LISTEN/NOTIFY watcher that outputs one line per new inbox
   message, usable directly as a Claude Code Monitor pipeline.
   `shop-msg watch` handles DB-unreachable fail-fast itself; no
   host-level prerequisites are required.
