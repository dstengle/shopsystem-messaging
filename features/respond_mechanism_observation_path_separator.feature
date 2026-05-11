Feature: shop-msg respond mechanism_observation — bd_ref input safety

  @scenario_hash:84fff111077d4035 @bc:shop-msg
  Scenario: Reject bd_ref containing a path separator
    Given an empty BC at a temporary path
    When I run shop-msg respond mechanism_observation with bd-ref "ddd/../etc-passwd" and subject "anything" and body "Body content of at least fifty characters to satisfy the schema's minimum length constraint."
    Then the command exits non-zero
    And the BC's outbox is empty
