Feature: shop-msg read outbox — lead-side response reading

  @scenario_hash:81e8af96807f33f4 @bc:shop-msg
  Scenario: Read a work_done from a BC's outbox
    Given an empty BC at a temporary path
    And shop-msg respond work_done was previously used to write "lead-001-work_done.yaml" with status "complete" and scenario-hash "abc123"
    When I run shop-msg read outbox with work-id "lead-001"
    Then the command exits zero
    And stdout includes message_type "work_done" and work_id "lead-001" and status "complete"

  @scenario_hash:d3e94f098d60143f @bc:shop-msg
  Scenario: Read a clarify from a BC's outbox
    Given an empty BC at a temporary path
    And shop-msg respond clarify was previously used to write "lead-002-clarify.yaml" with question "what about edge cases?"
    When I run shop-msg read outbox with work-id "lead-002"
    Then the command exits zero
    And stdout includes message_type "clarify" and work_id "lead-002"

  @scenario_hash:2cac6d6dba471090 @bc:shop-msg
  Scenario: Read fails when no outbox file matches the work_id
    Given an empty BC at a temporary path
    When I run shop-msg read outbox with work-id "nonexistent"
    Then the command exits non-zero
    And stderr explains no outbox response was found

  @scenario_hash:c039ab184dd1bbb8 @bc:shop-msg
  Scenario: Read fails with a stderr message when the outbox file fails schema validation
    Given an empty BC at a temporary path
    And the BC's outbox already contains a file named "lead-099-clarify.yaml" with content that is valid YAML but does not match the BCResponse schema
    When I run shop-msg read outbox with work-id "lead-099"
    Then the command exits non-zero
    And stderr explains schema validation failed
