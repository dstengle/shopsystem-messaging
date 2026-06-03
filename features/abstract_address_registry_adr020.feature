Feature: ADR-020 abstract-address registry and routing (no stored shop_root)

  @bc:shopsystem-messaging
  @scenario_hash:5374f77354187b06
  Scenario: registry add accepts a canonical name with no filesystem path and assigns the abstract address
    Given no shop named "shopsystem-messaging" is registered in the messaging registry
    And the deployment's system slug is "shopsystem"
    When I run shop-msg registry add with canonical name "shopsystem-messaging" and no filesystem-path argument
    Then the command exits zero
    And shop-msg registry list includes an entry for "shopsystem-messaging" whose abstract address is "shopsystem/messaging"

  @bc:shopsystem-messaging
  @scenario_hash:cf22ce33ba3edeea
  Scenario: Passing a filesystem-path positional to registry add exits non-zero with a migration message
    Given the shop-msg CLI has shipped abstract-address routing identity
    When I run shop-msg registry add with canonical name "shopsystem-messaging" and a filesystem-path positional argument
    Then the command exits non-zero
    And stderr contains a message indicating registry add no longer accepts a shop_root path and instructs the caller to use registry add [--lead-shop] <name>
    And no entry for "shopsystem-messaging" is added to the registry

  @bc:shopsystem-messaging
  @scenario_hash:3d9da19b3174fcf6
  Scenario: registry list emits abstract addresses and projects no path column
    Given "shopsystem-messaging" is registered in the messaging registry
    And "shopsystem-product" is registered as the lead shop
    When I run shop-msg registry list
    Then the command exits zero
    And stdout contains an entry whose abstract address is "shopsystem/messaging"
    And stdout contains an entry whose abstract address is "shopsystem/lead"
    And no entry in stdout contains a filesystem path field

  @bc:shopsystem-messaging
  @scenario_hash:166aac0254d757a2
  Scenario: A message sent to a BC by name routes to that BC's abstract address and resolves there for read pending and watch
    Given "shopsystem-messaging" is registered in the messaging registry with abstract address "shopsystem/messaging"
    And "shopsystem-product" is registered as the lead shop with abstract address "shopsystem/lead"
    When I run shop-msg send assign_scenarios --bc shopsystem-messaging with a valid payload and work-id "reg-501"
    Then the command exits zero
    And the stored message's "to" field is the abstract address "shopsystem/messaging"
    And shop-msg pending inbox --bc shopsystem-messaging includes work-id "reg-501"
    And shop-msg read inbox --bc shopsystem-messaging with work-id "reg-501" returns that message

  @bc:shopsystem-messaging
  @scenario_hash:370b97bb12d82ead
  Scenario: A message to the lead routes to the lead sentinel address and does not land in any BC inbox
    Given "shopsystem-product" is registered as the lead shop with abstract address "shopsystem/lead"
    And "shopsystem-messaging" is registered in the messaging registry with abstract address "shopsystem/messaging"
    When a BC sends a response addressed to the lead with work-id "reg-541"
    Then the command exits zero
    And the stored message's "to" field is the abstract address "shopsystem/lead"
    And shop-msg pending inbox --lead shopsystem-product includes work-id "reg-541"
    And shop-msg pending inbox --bc shopsystem-messaging does not include work-id "reg-541"

  @bc:shopsystem-messaging
  @scenario_hash:b324c650784c2378
  Scenario: A registered entry exposes no stored filesystem path on any observable surface
    Given "shopsystem-messaging" is registered in the messaging registry with abstract address "shopsystem/messaging"
    When I inspect the registered entry for "shopsystem-messaging" via shop-msg registry list
    Then the command exits zero
    And the entry exposes the fields abstract address and shop_type only
    And the entry exposes no shop_root field and no filesystem path value

  @bc:shopsystem-messaging
  @scenario_hash:f9910cf40291768c
  Scenario: A name-addressed shop-msg operation resolves its bd working directory from the local invoking CWD and does not crash when no registry path exists
    Given "shopsystem-messaging" is registered in the messaging registry with abstract address "shopsystem/messaging"
    And the registry stores no filesystem path for any entry
    And the invoking CWD contains a .beads directory discoverable by walk-up
    When I run a name-addressed shop-msg operation against --bc shopsystem-messaging that needs a bd context
    Then the command exits zero
    And the bd context used is the .beads directory discovered from the local invoking CWD
    And the command emits no FileNotFoundError or NotADirectoryError arising from a registry-stored path

  @bc:shopsystem-messaging
  @scenario_hash:e9a31b6fba19104d
  Scenario: Migration backfills abstract addresses from canonical names maps the lead to the sentinel and drops unmappable orphan rows
    Given a pre-migration shop_registry contains a path-keyed entry for canonical name "shopsystem-messaging"
    And it contains a path-keyed lead entry for canonical name "shopsystem-product"
    And it contains an orphan row whose key cannot be mapped to any known canonical name, such as "/workspace" or a tmp_path-prefixed key
    When the addressing migration runs to completion
    Then the entry for "shopsystem-messaging" has abstract address "shopsystem/messaging"
    And the lead entry has abstract address "shopsystem/lead"
    And the orphan row that maps to no known canonical name is absent from the migrated registry
    And no migrated entry retains a shop_root filesystem path
