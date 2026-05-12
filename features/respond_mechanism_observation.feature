Feature: shop-msg respond — write a mechanism_observation outbox YAML

  @scenario_hash:950f7c76478e81c9 @bc:shop-msg
  Scenario: Reply with a mechanism_observation message
    Given an empty BC at a temporary path
    When I run shop-msg respond mechanism_observation with work-id "lead-022" and subject "template lacks discriminator" and body "While doing lead-022 the bc-implementer template did not give me a clear discriminator between two adjacent cases; I fell back on heuristic guessing that the next BC will likely interpret differently."
    Then the BC's outbox contains a file named "lead-022-mechanism_observation.yaml"
    And the file parses as a valid MechanismObservation with work_id "lead-022" and subject "template lacks discriminator"
