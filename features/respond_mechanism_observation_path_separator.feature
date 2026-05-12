Feature: shop-msg respond mechanism_observation — work_id input safety

  @scenario_hash:00f34c55c7ea9b93 @bc:shop-msg
  Scenario: Reject work_id containing a path separator
    Given an empty BC at a temporary path
    When I run shop-msg respond mechanism_observation with work-id "lead/../etc-passwd" and subject "anything" and body "Body content of at least fifty characters to satisfy the schema's minimum length constraint."
    Then the command exits non-zero
    And the BC's outbox is empty
