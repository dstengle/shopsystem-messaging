Feature: Brief 006 scope A+B: name registry and name-based addressing

  @scenario_hash:1803bfa0abaf3487 @bc:shopsystem-messaging
  Scenario: Using the removed --bc-root flag exits non-zero with a migration error message
    Given the shop-msg CLI has shipped name-based addressing
    When I run any shop-msg subcommand with a --bc-root flag
    Then the command exits non-zero
    And stderr contains a message indicating --bc-root is no longer supported and instructs the caller to use --bc <name>

  @scenario_hash:0d04698f4a53a7cd @bc:shopsystem-messaging
  Scenario: Using the removed --lead-root flag exits non-zero with a migration error message
    Given the shop-msg CLI has shipped name-based addressing
    When I run any shop-msg subcommand with a --lead-root flag
    Then the command exits non-zero
    And stderr contains a message indicating --lead-root is no longer supported and instructs the caller to use --lead <name>
