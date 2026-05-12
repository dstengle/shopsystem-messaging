Feature: shop-msg respond — refuse on outbox collision for mechanism_observation

  @scenario_hash:7c7a6d58fa71a60c @bc:shop-msg
  Scenario: Refuse to overwrite an existing mechanism_observation for the same work_id
    Given an empty BC at a temporary path
    And the BC's outbox already contains a file named "lead-022-mechanism_observation.yaml"
    When I run shop-msg respond mechanism_observation with work-id "lead-022" and subject "second subject" and body "Body content of at least fifty characters to satisfy the schema's minimum length constraint."
    Then the command exits non-zero
    And the BC's outbox file "lead-022-mechanism_observation.yaml" is unchanged
