Feature: shop-msg respond mechanism_observation — body min-length

  @scenario_hash:af080ca711b8a127 @bc:shop-msg
  Scenario: Reject body shorter than the schema's minimum length
    Given an empty BC at a temporary path
    When I run shop-msg respond mechanism_observation with work-id "lead-022" and subject "anything" and body "too short"
    Then the command exits non-zero
    And the BC's outbox is empty
