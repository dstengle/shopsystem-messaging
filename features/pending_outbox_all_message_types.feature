Feature: shop-msg CLI surface — pending outbox for all message types

  @scenario_hash:b98f9d7c3f61435f @bc:shopsystem-messaging
  Scenario: pending outbox surfaces work_done for a work_id dispatched as request_bugfix
    Given a lead shop at a temporary path with BC clones "bc-alpha" and "bc-beta" present as sibling directories
    And shop-msg send request_bugfix was previously used inside "bc-alpha" to write an inbox message with work-id "lead-401"
    And shop-msg respond work_done was previously used inside "bc-alpha" to write an outbox response with work-id "lead-401" and status "complete"
    When I run the shop-msg subcommand that enumerates pending unprocessed outbox responses, filtered to BC "bc-alpha"
    Then the command exits zero
    And stdout includes an entry for work_id "lead-401" with message_type "work_done" originating from BC "bc-alpha"

  @scenario_hash:e6be1372adadc5e3 @bc:shopsystem-messaging
  Scenario: pending outbox surfaces work_done for a work_id dispatched as request_maintenance
    Given a lead shop at a temporary path with BC clones "bc-alpha" and "bc-beta" present as sibling directories
    And shop-msg send request_maintenance was previously used inside "bc-alpha" to write an inbox message with work-id "lead-402"
    And shop-msg respond work_done was previously used inside "bc-alpha" to write an outbox response with work-id "lead-402" and status "complete"
    When I run the shop-msg subcommand that enumerates pending unprocessed outbox responses, filtered to BC "bc-alpha"
    Then the command exits zero
    And stdout includes an entry for work_id "lead-402" with message_type "work_done" originating from BC "bc-alpha"
