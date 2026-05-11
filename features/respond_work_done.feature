Feature: shop-msg respond — write a work_done outbox YAML

  @scenario_hash:650e6761d5479ce3 @bc:shop-msg
  Scenario: Reply to lead with a work_done message
    Given an empty BC at a temporary path
    When I run shop-msg respond work_done with work-id "lead-001" and status "complete" and scenario-hash "abc123"
    Then the BC's outbox contains a file named "lead-001-work_done.yaml"
    And the file parses as a valid WorkDone with work_id "lead-001" and status "complete"
