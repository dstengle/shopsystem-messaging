Feature: messaging registry: name-addressed BC ops do not require shop_root to exist on lead host (ADR-018)

  @scenario_hash:82e88384ccd417d8 @bc:shopsystem-messaging
  Scenario: shop-msg send by --bc <name> succeeds when the registered BC's shop_root path does not exist on the lead host
    Given a BC named "shopsystem-fictional-mxxm-101" is registered with a shop_root path that does not exist on the lead host
    When I run shop-msg send assign_scenarios for that BC with work-id "reg-mxxm-101"
    Then the command exits zero
    And stderr contains no "points to path ... which does not exist on disk" warning
    And stderr contains no "registry may be stale" warning
    And shop-msg pending inbox for that BC includes work-id "reg-mxxm-101"

  @scenario_hash:dac49bb9814f7ddc @bc:shopsystem-messaging
  Scenario: shop-msg read and pending against --bc <name> succeed when the registered BC's shop_root path does not exist on the lead host
    Given a BC named "shopsystem-fictional-mxxm-201" is registered with a shop_root path that does not exist on the lead host
    And an inbox message with work-id "reg-mxxm-201" is present for that BC
    And an outbox response with work-id "reg-mxxm-202" is present for that BC
    When I run shop-msg read inbox for that BC with work-id "reg-mxxm-201"
    And I run shop-msg read outbox for that BC with work-id "reg-mxxm-202"
    And I run shop-msg pending inbox for that BC
    And I run shop-msg pending outbox for that BC
    Then every one of those commands exits zero
    And none of those commands wrote a "points to path ... which does not exist on disk" warning to stderr
    And none of those commands wrote a "registry may be stale" warning to stderr

  @scenario_hash:33221aa5431226f3 @bc:shopsystem-messaging
  Scenario: shop-msg registry add and registry list accept a BC entry whose shop_root path does not exist on the lead host
    Given no shop named "shopsystem-fictional-mxxm-301" is registered in the messaging registry
    And the filesystem path "/workspaces/no-such-bc-clone-on-lead-host" does not exist on the lead host
    When I run shop-msg registry add with canonical name "shopsystem-fictional-mxxm-301" and shop_root "/workspaces/no-such-bc-clone-on-lead-host"
    Then the command exits zero
    And stderr contains no "points to path ... which does not exist on disk" warning
    And stderr contains no "registry may be stale" warning
    When I run shop-msg registry list
    Then the command exits zero
    And stdout includes an entry for "shopsystem-fictional-mxxm-301" with shop_root "/workspaces/no-such-bc-clone-on-lead-host"
    And stderr contains no "points to path ... which does not exist on disk" warning
    And stderr contains no "registry may be stale" warning
