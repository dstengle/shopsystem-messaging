Feature: shopsystem-messaging release workflow declares NO repository_dispatch emit to bc-launcher (ADR-022)

  Register parity with shopsystem-scenarios (lead-ignt): the no-emit
  guarantee that lead-k6xq made true (release.yml deleted, pytest guard
  tests/test_release_dispatch_retired.py added) is pinned here as a tagged
  scenario_hash BDD scenario. The hash is computed scenario-block-only and
  canonical (ADR-019); the leading comment block below is load-bearing
  context reproduced verbatim from the dispatched gherkin.

  @scenario_hash:974ee23c53cbb09a @bc:shopsystem-messaging
  # FORMALIZATION (ADR-022, work_id lead-n8pf) — NOT a delete instruction:
  # Your work_done under lead-k6xq (origin/main 7baf439) ALREADY deleted release.yml
  # and added the pytest guard tests/test_release_dispatch_retired.py enforcing the
  # no-emit behavior. This message does NOT ask you to delete anything again. It asks
  # you to REGISTER that already-true no-emit guarantee as a tagged @scenario_hash BDD
  # scenario (this block), for register parity with shopsystem-scenarios (lead-ignt).
  # - Pin THIS block at its @scenario_hash; your existing pytest guard already satisfies
  #   the behavior, so this should be a near-trivial register addition (optionally wire it
  #   to the same comment-stripped workflows-tree parse the guard uses).
  # - Hash-identity note: the retired pin was on-disk @scenario_hash:b891abf0d7ce801f
  #   (alias a83760dcc40c57e6 in legacy comments), retired with NO successor per your
  #   work_done. THIS block is the NEW no-emit guarantee, not a revival of the old emit.
  # - If release.yml stays absent (no workflows), satisfy this against the empty/whatever
  #   workflows tree — absence of the emit is the guarantee, not presence of a file.
  Scenario: shopsystem-messaging release workflow declares NO repository_dispatch emit to shopsystem-bc-launcher and references NO BC_LAUNCHER_DISPATCH_TOKEN
    Given the shopsystem-messaging release workflow at ".github/workflows/release.yml"
    And bc-base rebuilds are driven by shopsystem-bc-launcher's own centralized scheduled poll per ADR-022, not by a per-repo repository_dispatch emit
    When the release workflow's executable body, with YAML comment lines excluded, is inspected on a version-tag release
    Then the executable body declares no "dispatch-bc-launcher-build" job and no step performing a repository_dispatch targeting "dstengle/shopsystem-bc-launcher"
    And the executable body declares no repository_dispatch with event_type "framework-utility-release"
    And the executable body references no secret named "BC_LAUNCHER_DISPATCH_TOKEN"
    And a repository_dispatch target or BC_LAUNCHER_DISPATCH_TOKEN reference present only in a descriptive YAML comment, absent from the executable body, does not fail this guarantee
