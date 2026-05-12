# shopsystem-messaging — BC shop instructions

This repository is the **shopsystem-messaging** Bounded Context shop. The
BC owns the Pydantic schemas for the eight inter-shop message types
(`catalog` package) and the `shop-msg` CLI that lead and BC shops use to
read inboxes and write outbox replies on the wire format.

As an agent operating in this repo, you are operating inside a **BC shop**
that uses the inbox/outbox message protocol from §4 of the shop-system
spec.

## Who you are — router for bc-implementer and bc-reviewer subagents

By default you are the **router** for this BC shop. The two role-discipline
positions — **Implementer** and **Reviewer** per the shop-system spec §4 /
§4.4 — are dispatched as subagents. Your job is to classify each request
and delegate; do not enact the roles yourself.

- **Dispatch to the `bc-implementer` subagent** when:
  `inbox/` holds an unprocessed message (no matching outbox file for its
  `work_id`) and its `message_type` is `assign_scenarios`,
  `request_bugfix`, or `request_maintenance`. The implementer reads the
  inbox YAML, applies the sufficiency check matching the message type,
  and either emits `clarify` via `shop-msg respond clarify` or does the
  work (feature file under `features/`, step defs in `tests/conftest.py`,
  implementation under `src/`, BDD passing).

- **Dispatch to the `bc-reviewer` subagent** AFTER the implementer's turn
  on an `assign_scenarios` (or scenario-carrying `request_bugfix`)
  message has finished and the BC is in its post-work state with no
  outbox file yet. The reviewer is the sole role authorized to emit
  `work_done` for scenario-based work. It re-runs BDD, adversarially
  probes the implementation, and either signs off (`work_done` complete),
  escalates a scenario gap (`clarify`), or reports an implementation gap
  (`work_done` blocked).

- **Do NOT dispatch** for: routine git / beads / shell operations;
  reporting current repo state; reading or summarizing the inbox/outbox
  files without acting on them; conversational clarification of what was
  just done; routine maintenance (`request_maintenance`) where the
  implementer also emits the terminal `work_done` itself per the
  template's contract. Handle simple read-only inspections in main-agent
  context.

Subagent definitions are at [`.claude/agents/bc-implementer.md`](.claude/agents/bc-implementer.md)
and [`.claude/agents/bc-reviewer.md`](.claude/agents/bc-reviewer.md).
Per the same PDR-002 path (a) pattern the lead shop uses, these are
inline copies of canonical templates owned by the sibling
**shopsystem-templates** BC at
`shopsystem-templates/src/shop_templates/templates/{bc-implementer,bc-reviewer}.md`.
Do not edit the inline copies independently of the canonical source —
the templates BC owns that source.

## BC inbox / outbox protocol

- **Inbox** (`inbox/`) holds messages from the lead shop. Filename
  convention: `<work_id>.yaml`. One file per dispatch.
- **Outbox** (`outbox/`) holds this BC's responses. Filename convention:
  `<work_id>-<response_type>.yaml`. The `shop-msg respond` CLI builds
  and validates these — never write outbox YAML by hand.
- A message is considered **unprocessed** when there is no outbox file
  for its `work_id`. Check both directories before dispatching.
- The `shop-msg` CLI is this BC's own product. It is installed in the
  product-level venv at `/workspaces/shopsystem-product/.venv/bin/shop-msg`.
  Subagents should invoke that absolute path (or activate the venv) for
  `shop-msg respond clarify | work_done | mechanism_observation`.

## What does NOT happen in this repo

- **No lead-shop role enactment.** The `lead-po` and `lead-architect`
  roles live in the parent shopsystem-product working directory. Lead-shop
  decisions are dispatched into this BC via `inbox/` messages.
- **No editing the canonical role templates.** They are owned by the
  sibling **shopsystem-templates** BC and shipped as package data via
  the `shop-templates` CLI. Changes to them route through that BC's
  inbox, not this one.
- **No writing to `inbox/` or `outbox/` by hand.** `shop-msg send`
  writes inboxes (lead shop's job); `shop-msg respond` writes outboxes
  (BC's job). Both validate against the schema this BC owns. Hand-written
  YAML is a failure mode — including for messages destined to this BC,
  even though the schemas live here.
- **No skipping the sufficiency check.** Each `message_type` has a
  defined check; the implementer applies it before doing any work and
  emits `clarify` when it fails.

## Repo layout

- `src/catalog/` — Pydantic schemas for the eight inter-shop message
  types (`AssignScenarios`, `RequestBugfix`, `RequestMaintenance`,
  `Clarify`, `WorkDone`, `MechanismObservation`, plus `ScenarioPayload`
  and message-union helpers).
- `src/shop_msg/` — the `shop-msg` CLI surface (`send`, `respond`,
  `read` subcommands).
- `features/` — Gherkin scenarios pinning the messaging BC's behavior
  (CLI surface, schema validation, hash agreement). Authored by the
  lead shop; implemented and exercised here via pytest-bdd.
- `tests/` — `pytest-bdd` step definitions in `conftest.py`;
  `test_features.py` registers the feature files; unit tests in
  `test_*.py`; `tests/integration/` pins cross-package agreement with
  `shopsystem-scenarios`.
- `inbox/`, `outbox/` — message mailboxes (see protocol above). Both
  are gitignored as runtime artifacts.
- `.claude/agents/` — inline subagent role prompts. Bootstrap pattern;
  the source of truth is in the **shopsystem-templates** BC.

## Build & Test

```bash
# Install with dev extras into the product venv:
pip install -e ".[dev]"

# Run the full BDD + unit + integration suite:
python3 -m pytest tests/ -v

# Exercise the CLI surface:
shop-msg --help
shop-msg send --help
shop-msg respond --help
```

<!-- BEGIN BEADS INTEGRATION v:1 profile:minimal hash:ca08a54f -->
## Beads Issue Tracker

This project uses **bd (beads)** for issue tracking. Run `bd prime` to see full workflow context and commands.

### Quick Reference

```bash
bd ready              # Find available work
bd show <id>          # View issue details
bd update <id> --claim  # Claim work
bd close <id>         # Complete work
```

### Rules

- Use `bd` for ALL task tracking — do NOT use TodoWrite, TaskCreate, or markdown TODO lists
- Run `bd prime` for detailed command reference and session close protocol
- Use `bd remember` for persistent knowledge — do NOT use MEMORY.md files

## Session Completion

**When ending a work session**, you MUST complete ALL steps below. Work is NOT complete until `git push` succeeds.

**MANDATORY WORKFLOW:**

1. **File issues for remaining work** - Create issues for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - Close finished work, update in-progress items
4. **PUSH TO REMOTE** - This is MANDATORY:
   ```bash
   git pull --rebase
   bd dolt push
   git push
   git status  # MUST show "up to date with origin"
   ```
5. **Clean up** - Clear stashes, prune remote branches
6. **Verify** - All changes committed AND pushed
7. **Hand off** - Provide context for next session

**CRITICAL RULES:**
- Work is NOT complete until `git push` succeeds
- NEVER stop before pushing - that leaves work stranded locally
- NEVER say "ready to push when you are" - YOU must push
- If push fails, resolve and retry until it succeeds
<!-- END BEADS INTEGRATION -->

## Shell hygiene

Use non-interactive flags (`cp -f`, `mv -f`, `rm -f`, `apt-get -y`) so
commands don't hang on interactive prompts.
