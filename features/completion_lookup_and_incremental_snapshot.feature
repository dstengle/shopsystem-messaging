Feature: completion lookup keyed on block-only canonical hash and incremental snapshot application (lead-if3j)

  A completion lookup answers, keyed purely on a scenario's block-only
  canonical hash, whether that scenario is recorded as completed. The lead
  snapshot can also reflect a newly-completed scenario carried by a single
  work_done incrementally, without sweeping every BC journal.

  @scenario_hash:1b21dbb923413455 @bc:shopsystem-messaging
  Scenario: the completion lookup answers a definite yes for a scenario whose block-only canonical hash is recorded as completed
    Given a scenario block whose block-only canonical hash is "h1"
    And the completion state records "h1" as a completed scenario
    When the completion lookup is queried for the block-only canonical hash "h1"
    Then the lookup returns a definite "yes" answer for "h1"
    And the answer is keyed on the block-only canonical hash "h1", not on any bead id, scenario title, or dispatch record

  @scenario_hash:528c08b5a0a6d024 @bc:shopsystem-messaging
  Scenario: the completion lookup answers a definite no for a scenario whose block-only canonical hash is not recorded as completed
    Given a scenario block whose block-only canonical hash is "h2"
    And the completion state records no completed scenario with hash "h2"
    When the completion lookup is queried for the block-only canonical hash "h2"
    Then the lookup returns a definite "no" answer for "h2"
    And the answer is keyed on the block-only canonical hash "h2", not on any bead id, scenario title, or dispatch record

  @scenario_hash:307967ddfb53fc45 @bc:shopsystem-messaging
  Scenario: the lead snapshot incrementally reflects a newly-completed scenario carried by a work_done
    Given a lead snapshot in which the block-only canonical hash "h4" is not recorded as completed
    And a work_done arrives carrying a scenario whose block-only canonical hash "h4" equals its on-disk @scenario_hash tag
    When the lead applies that work_done to its snapshot incrementally
    Then the lead snapshot records the block-only canonical hash "h4" as completed
    And the lead recorded "h4" without performing a full reconciliation sweep of every BC journal
