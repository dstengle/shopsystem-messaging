Feature: shop-msg respond — refuse on outbox collision for mechanism_observation

  @scenario_hash:67668aaac41d49f0 @bc:shop-msg
  Scenario: Refuse to overwrite an existing mechanism_observation for the same bd_ref
    Given an empty BC at a temporary path
    And the BC's outbox already contains a file named "ddd-product-system-abc-mechanism_observation.yaml"
    When I run shop-msg respond mechanism_observation with bd-ref "ddd-product-system-abc" and subject "second subject" and body "second body"
    Then the command exits non-zero
    And the BC's outbox file "ddd-product-system-abc-mechanism_observation.yaml" is unchanged
