Feature: shop-msg respond mechanism_observation — body min-length

  @scenario_hash:6f9e0083f7dc6b73 @bc:shop-msg
  Scenario: Reject body shorter than the schema's minimum length
    Given an empty BC at a temporary path
    When I run shop-msg respond mechanism_observation with bd-ref "ddd-product-system-abc" and subject "anything" and body "too short"
    Then the command exits non-zero
    And the BC's outbox is empty
