Feature: shop-msg respond clarify — refuse on lead-inbox collision

  @scenario_hash:b6973413b7bfdd12 @bc:shop-msg
  Scenario: Refuse to overwrite an existing clarify for the same work_id
    Given an empty BC at a temporary path
    And the lead's inbox already contains a response named "lead-col2-clarify.yaml"
    When I run shop-msg respond clarify with work-id "lead-col2" and question "second"
    Then the command exits non-zero
    And the lead's inbox response "lead-col2-clarify.yaml" is unchanged
