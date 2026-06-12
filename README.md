# shopsystem-messaging

Messaging bounded context of the shopsystem product. Owns:

- **Pydantic schemas** for the inter-shop message types under the
  importable package `catalog` (`AssignScenarios`, `RequestBugfix`,
  `RequestMaintenance`, `Clarify`, `WorkDone`, `MechanismObservation`,
  `Nudge`, plus the `ScenarioPayload` and message-union helpers).
- **`shop-msg` CLI**, the tool that lead and BC shops use to read their
  inbox and write outbox replies on the inter-shop wire format.
- **`features/`** — the Gherkin scenarios that pin the messaging BC's
  behavior. Authored by the product (lead) shop; implemented and
  exercised here via pytest-bdd.

Speaks one ubiquitous language: *message_type, work_id, schema,
validation, inbox, outbox, send, respond, read, wire format*. See
[ADR-001](https://github.com/dstengle/ddd-product-system/blob/main/docs/shop-system/adr-001-framework-packaging.md)
for the BC-of-the-shopsystem framing that drove this packaging.

## Message-type catalogue

The authoritative catalogue is `05-inter-shop-protocol.md` §5.3 (as
amended 2026-06-12); this list mirrors it for count and contents. The
catalogue enumerates **eight** message types: six lead → BC dispatch
verbs plus the BC → lead responses.

**Lead → BC dispatch verbs:**

- `assign_scenarios` — dispatch a feature's Gherkin scenarios to a BC.
- `request_bugfix` — dispatch a bug repair (may carry scenarios).
- `request_maintenance` — dispatch a non-behavioral chore (no scenarios).
- `request_scenario_register` — **DEFERRED / UNIMPLEMENTED.** A pinned
  named reservation per §5.3 with no schema class; not a selectable
  dispatch vehicle.
- `request_shop_card` — **DEFERRED / UNIMPLEMENTED.** A pinned named
  reservation per §5.3 with no schema class; not a selectable dispatch
  vehicle.
- `nudge` — symmetric operational-liveness ping (ADR-015), flowing both
  lead → BC and BC → lead; carries a reason enum and no `scenario_hashes`.

**BC → lead responses:**

- `work_done` — report a dispatch complete or blocked.
- `clarify` — ask a clarifying question on an ambiguous work item.
- `mechanism_observation` — surface a load-bearing, out-of-scope property
  of the mechanism itself.

## Install

Phase-1 distribution is via git URL (no PyPI yet — ADR-001 §Phase-1
wiring):

```bash
pip install "git+https://github.com/dstengle/shopsystem-messaging@v0.1.0"
```

This pulls in [`shopsystem-scenarios`](https://github.com/dstengle/shopsystem-scenarios)
at tag `v0.1.0` as a transitive dependency. The scenarios package owns
the canonical Gherkin-hashing rule that `ScenarioPayload.hash` is
validated against; messaging delegates to it rather than duplicating.

## CLI

The console script is `shop-msg`. Subcommands are split into `respond`
(BC -> lead replies, written to `<bc-root>/outbox/`), `send` (lead ->
BC dispatches, written to `<bc-root>/inbox/`), and `read` (read the
latest outbox file for a work_id):

```bash
shop-msg --help

# BC responds to a lead-assigned work item, completed
shop-msg respond work_done \
    --bc-root /path/to/bc --work-id lead-042 \
    --status complete --summary "All three scenarios green"

# BC asks a clarifying question on an ambiguous work item
shop-msg respond clarify \
    --bc-root /path/to/bc --work-id lead-042 \
    --question "Does 'expired' include items past 24h?"

# Lead dispatches scenarios to a BC
shop-msg send assign_scenarios \
    --bc-root /path/to/bc --work-id lead-043 \
    --feature-title "Coupon redemption" --bc-tag billing \
    --scenario-file scenarios/coupon-applied.feature
```

`send assign_scenarios` and `send request_bugfix` shell out to the
`scenarios hash` CLI from the scenarios package to compute each
scenario's canonical hash — the same rule the schema validator
enforces, reached from two directions (Python import on the schema
side, subprocess on the CLI side).

## Tests

```bash
pip install -e ".[dev]"
pytest tests/ -v
```

Two pytest suites: catalog schema tests (`tests/test_*.py`) and BDD
features (`tests/test_features.py` discovers `features/*.feature`).
The integration test under `tests/integration/` pins the cross-package
agreement with shopsystem-scenarios.

## License

MIT. See [LICENSE](./LICENSE).
