Feature: Test fixture hygiene for production canonical registry entries

  Background: the messaging BC's pytest session uses a session-scoped
    autouse fixture to snapshot production canonical registry entries before
    they are mutated by tests, and a function-scoped autouse fixture to
    restore per-test mutations between tests. Both layers cover every
    canonical name that the test suite's _register_shop helper may touch,
    not a hand-maintained pair of lead aliases.

  @scenario_hash:2460440854300728 @bc:shopsystem-messaging
  Scenario: The test session restores every production canonical registry entry it observed pre-session, not only the lead aliases
  Given the messaging BC's pytest session is about to start
  And the shop_registry contains a production entry for canonical name "shopsystem-messaging" with shop_root "/workspaces/shopsystem-product/repos/shopsystem-messaging"
  And the shop_registry contains a production entry for canonical name "shopsystem-docs" with shop_root "/workspaces/shopsystem-product/repos/shopsystem-docs"
  When the pytest session runs and at least one test invokes the fixture helper that registers "shopsystem-messaging" or "shopsystem-docs" at a tmp_path-rooted shop_root
  And the pytest session completes (teardown of the session-scoped registry fixture runs)
  Then the shop_registry entry for "shopsystem-messaging" has shop_root "/workspaces/shopsystem-product/repos/shopsystem-messaging" and the same shop_type it had pre-session
  And the shop_registry entry for "shopsystem-docs" has shop_root "/workspaces/shopsystem-product/repos/shopsystem-docs" and the same shop_type it had pre-session
  And the restoration applies to every production canonical name the session observed pre-test, not only "shopsystem-product" and "shopsystem product"

  @scenario_hash:acd9e1c74ea1744a @bc:shopsystem-messaging
  Scenario: A test that mutates a production canonical registry entry sees the registry restored to its pre-test state before the next test runs
  Given the shop_registry contains a production entry for canonical name "shopsystem-docs" with shop_root "/workspaces/shopsystem-product/repos/shopsystem-docs"
  And the messaging BC's pytest session is running
  When a test calls the fixture helper to register "shopsystem-docs" at a tmp_path-rooted shop_root distinct from the production shop_root
  And that test completes (passes)
  And the next test in the same pytest session begins
  Then at the start of the next test, the shop_registry entry for "shopsystem-docs" has the production shop_root "/workspaces/shopsystem-product/repos/shopsystem-docs" — not the prior test's tmp_path value
  And the restoration is performed by a per-test (function-scoped) teardown, not deferred to session teardown

  @scenario_hash:6dcbb68f89d527ec @bc:shopsystem-messaging
  Scenario: A test that mutates a production canonical registry entry and then fails (raises) sees the registry restored before the next test runs
  Given the shop_registry contains a production entry for canonical name "shopsystem-messaging" with shop_root "/workspaces/shopsystem-product/repos/shopsystem-messaging"
  And the messaging BC's pytest session is running
  When a test calls the fixture helper to register "shopsystem-messaging" at a tmp_path-rooted shop_root distinct from the production shop_root
  And that test raises an unhandled exception (fails) after the mutation
  And the next test in the same pytest session begins
  Then at the start of the next test, the shop_registry entry for "shopsystem-messaging" has the production shop_root "/workspaces/shopsystem-product/repos/shopsystem-messaging" — not the failed test's tmp_path value
  And the per-test restoration runs regardless of test outcome (pass, fail, or error)

  @scenario_hash:27c7804d2392736c @bc:shopsystem-messaging
  Scenario: After the pytest session completes, no shop_registry row for any production canonical name has a tmp_path-prefixed shop_root
  Given the messaging BC's pytest session has run to completion (all session-scoped teardowns have executed)
  And the production canonical names observed pre-session include at least "shopsystem-messaging", "shopsystem-docs", "shopsystem-product", and "shopsystem product"
  When I run "shop-msg registry list" against the same shop_registry the test session used
  Then the command exits zero
  And no entry whose canonical name was observed as a production entry pre-session has a shop_root that begins with "/tmp/" or matches the pytest tmp_path pattern (e.g., contains "/pytest-of-")
  And every such production canonical entry resolves to the same shop_root it had pre-session

  @scenario_hash:420caad777af2152 @bc:shopsystem-messaging
  Scenario: The session-scoped save/restore mechanism covers every canonical name that a step definition may pass to the registry-mutating helper, not a hand-maintained subset
  Given the messaging BC's test suite contains a fixture helper that registers a (canonical-name, shop_root, shop_type) triple in the shop_registry on behalf of a step definition
  And the test suite contains a session-scoped fixture responsible for restoring production registry state at teardown
  When the test session starts and the session-scoped fixture initializes
  Then for every canonical name the fixture helper may register during the session, the session-scoped fixture has captured that name's pre-session registry state (or absence) before any test mutates it
  And the captured set is not limited to "shopsystem-product" and "shopsystem product"
  And at session teardown, each captured entry is restored to its pre-session value (or removed if it was absent pre-session)
  And the test suite contains no canonical name that a step definition can register but the session-scoped fixture does not cover

  @scenario_hash:e4263ccdca3b7a17 @bc:shopsystem-messaging
  Scenario: When a pytest session starts with the shop_registry already corrupted by a prior un-cleaned session, the session-scoped fixture treats tmp_path-prefixed entries as absent and does not re-persist them at teardown
  Given the shop_registry contains an entry for canonical name "shopsystem-docs" whose shop_root begins with "/tmp/" (a leaked tmp_path from a prior corrupted pytest session)
  And the production shop_root for "shopsystem-docs" is "/workspaces/shopsystem-product/repos/shopsystem-docs" but that production row is currently missing or pointing at the stale tmp_path
  When the messaging BC's pytest session starts and the session-scoped fixture initializes
  Then the session-scoped fixture captures the pre-session state for "shopsystem-docs" as absent (it does not preserve the tmp_path value)
  And at session teardown, the shop_registry contains no entry for "shopsystem-docs" with a tmp_path-prefixed shop_root
  And the self-healing behavior applies uniformly to every production canonical name the session-scoped fixture covers, not only to "shopsystem-product" and "shopsystem product"
