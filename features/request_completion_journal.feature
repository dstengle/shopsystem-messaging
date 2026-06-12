Feature: request_completion_journal — request and response message types

  A request_completion_journal asks a target bounded context for the set of
  block-only canonical scenario hashes it has completed. The request names
  the target BC and carries no completion entries of its own; the response
  carries the completed entries back as a bare set of hashes.

  @scenario_hash:65c85ffce8f88507 @bc:shopsystem-messaging
  Scenario: A minimal valid request_completion_journal request message can be constructed carrying only the target bounded context
    Given the RequestCompletionJournal request schema from the shop-msg catalog
    When I construct a RequestCompletionJournal request instance supplying only the fields the schema marks as required, naming the target bounded context whose completed scenarios are sought
    Then construction succeeds
    And no schema validation error is raised
    And the constructed request carries the named target bounded context and no scenario-completion entry of its own
