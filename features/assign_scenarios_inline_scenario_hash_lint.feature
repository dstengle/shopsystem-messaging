@bc:shopsystem-messaging @origin:lead-xp2nc
Feature: assign_scenarios pre-send inline @scenario_hash lint over ScenarioPayload

  Tightens the assign_scenarios pre-send lint (lead-xp2nc). The existing
  hash-matches-body invariant checks the envelope "hash" against
  scenario-block-only canonicalization, which strips the inline
  scenario-hash tag line, so the strip-then-hash check alone cannot see a
  missing or disagreeing inline tag. This lint additively requires each
  dispatched ScenarioPayload's gherkin to carry an inline scenario-hash
  tag whose hex equals the envelope hash.

  @scenario_hash:99a9d14a8d859642 @bc:shopsystem-messaging
  Scenario: assign_scenarios ScenarioPayload validation accepts a scenario whose gherkin carries an inline @scenario_hash tag matching the envelope hash
    Given a scenario gherkin block whose text carries an inline "@scenario_hash:<hex>" tag line directly above its "Scenario:" line
    And a ScenarioPayload envelope "hash" field whose value equals that inline "<hex>"
    When the assign_scenarios ScenarioPayload for that scenario is validated before send
    Then validation accepts the payload
    And the scenario is carried into the dispatched AssignScenarios message unchanged

  @scenario_hash:7d10bea41b0a0291 @bc:shopsystem-messaging
  Scenario: assign_scenarios ScenarioPayload validation rejects a scenario whose gherkin carries no inline @scenario_hash tag, naming that scenario
    Given a scenario gherkin block whose text contains no "@scenario_hash:" tag line
    And a ScenarioPayload envelope "hash" field equal to the scenario-block-only canonical hash of that gherkin, so the strip-then-hash invariant alone would accept the block
    When the assign_scenarios ScenarioPayload for that scenario is validated before send
    Then validation rejects the payload and no AssignScenarios message is written to the BC inbox
    And the rejection error names the scenario whose gherkin block is missing its inline "@scenario_hash" tag
