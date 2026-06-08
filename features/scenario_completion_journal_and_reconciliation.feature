Feature: BC scenario-completion journal and lead snapshot reconciliation (lead-9b3w)

  A Bounded Context records the completion of a scenario by appending the
  scenario's block-only canonical hash to an authoritative, append-only
  journal. The lead does not write to that journal; it pulls the journal
  on demand and reconciles its own per-BC snapshot against it.

  @scenario_hash:d01313bf5090bee6 @bc:shopsystem-messaging
  Scenario: a BC appends a completed scenario's hash to its authoritative append-only journal on completion
    Given a BC whose authoritative journal does not yet contain the block-only canonical hash "h3"
    And a scenario for that BC becomes PINNED & DEMONSTRATED: a work_done landed for it and its block-only canonical hash "h3" equals its on-disk @scenario_hash tag
    When the BC records the completion of that scenario
    Then the BC's authoritative journal contains the block-only canonical hash "h3" as an appended entry
    And no previously-journaled hash is removed or overwritten by the append

  @scenario_hash:d0a74c6e8ecb8eb3 @bc:shopsystem-messaging
  Scenario: the lead reconciles its snapshot against a BC journal pulled on demand
    Given a BC whose authoritative journal contains the block-only canonical hash "h5"
    And a lead snapshot that does not record "h5" as completed for that BC
    When the lead pulls that BC's journal on demand and reconciles its snapshot against it
    Then the lead snapshot records the block-only canonical hash "h5" as completed for that BC
    And the reconciled snapshot matches the BC's authoritative journal for that BC entry by entry
