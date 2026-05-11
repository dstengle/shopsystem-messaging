Feature: shop-msg respond — write a work_done outbox YAML

  @scenario_hash:35fece8e1f96e074 @bc:shop-msg
  Scenario: Refuse to overwrite an existing work_done for the same work_id
    Given an empty BC at a temporary path
    And the BC's outbox already contains a file named "lead-001-work_done.yaml"
    When I run shop-msg respond work_done with work-id "lead-001" and status "complete"
    Then the command exits non-zero
    And the BC's outbox file "lead-001-work_done.yaml" is unchanged
