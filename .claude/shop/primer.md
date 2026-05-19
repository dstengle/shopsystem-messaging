# shopsystem-messaging — project-specific context

This BC owns the Pydantic schemas for the eight inter-shop message types
(`catalog` package) and the `shop-msg` CLI that lead and BC shops use to
read inboxes and write outbox responses on the wire format.

## Repo layout

- `src/catalog/` — Pydantic schemas for the eight inter-shop message
  types (`AssignScenarios`, `RequestBugfix`, `RequestMaintenance`,
  `Clarify`, `WorkDone`, `MechanismObservation`, plus `ScenarioPayload`
  and message-union helpers).
- `src/shop_msg/` — the `shop-msg` CLI surface (`send`, `respond`,
  `read`, `prime`, `watch` subcommands).
- `features/` — Gherkin scenarios pinning the messaging BC's behavior
  (CLI surface, schema validation, hash agreement). Authored by the
  lead shop; implemented and exercised here via pytest-bdd.
- `tests/` — `pytest-bdd` step definitions in `conftest.py`;
  `test_features.py` registers the feature files; unit tests in
  `test_*.py`; `tests/integration/` pins cross-package agreement with
  `shopsystem-scenarios`.
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

## What does NOT happen in this repo

- **No lead-shop role enactment.** The `lead-po` and `lead-architect`
  roles live in the parent shopsystem-product working directory.
- **No editing the canonical role templates.** They are owned by the
  sibling **shopsystem-templates** BC. Changes route through that BC's
  inbox, not this one.
- **No skipping the sufficiency check.** Each `message_type` has a
  defined check; the implementer applies it before doing any work and
  emits `clarify` when it fails.

## Shell hygiene

Use non-interactive flags (`cp -f`, `mv -f`, `rm -f`, `apt-get -y`) so
commands don't hang on interactive prompts.
