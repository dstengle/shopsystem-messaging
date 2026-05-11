Feature: shop-msg respond — write a mechanism_observation outbox YAML

  @scenario_hash:5d706a6f3a55f2eb @bc:shop-msg
  Scenario: Reply with a mechanism_observation message
    Given an empty BC at a temporary path
    When I run shop-msg respond mechanism_observation with bd-ref "ddd-product-system-abc" and subject "template lacks discriminator" and body "While doing lead-022 the bc-implementer template did not give me a clear discriminator between two adjacent cases; I fell back on heuristic guessing that the next BC will likely interpret differently."
    Then the BC's outbox contains a file named "ddd-product-system-abc-mechanism_observation.yaml"
    And the file parses as a valid MechanismObservation with bd_ref "ddd-product-system-abc" and subject "template lacks discriminator"
