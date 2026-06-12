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

  @scenario_hash:7afa72ede6099ee1 @bc:shopsystem-messaging
  Scenario: A request_completion_journal response message validates with a bare set of completed block-only canonical hashes
    Given the RequestCompletionJournal response schema from the shop-msg catalog
    When I construct a RequestCompletionJournal response instance whose completed-entries field is a set of block-only canonical hashes "h1" and "h2"
    Then construction succeeds
    And no schema validation error is raised
    And the constructed response carries exactly the completed block-only canonical hashes "h1" and "h2" as a bare set, with no per-entry record beyond the hash
