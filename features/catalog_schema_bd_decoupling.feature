Feature: Catalog schema bd-decoupling — minimal valid messages need no bd field

  # The shop-system spec separates lead-side work-registry concerns (beads
  # is the lead's choice of tracker, per §6) from the inter-shop wire
  # message catalog. A BC consuming a shop-msg catalog schema must not
  # need to participate in beads to construct a valid message. These six
  # scenarios pin that invariant across the six currently-implemented
  # LeadMessage/BCResponse schemas: each must be constructible from its
  # required fields alone without supplying any beads identifier.

  @scenario_hash:23b9f1cf61ac9d42 @bc:shopsystem-messaging
  Scenario: A minimal valid assign_scenarios message can be constructed without supplying any bd-related field
  Given the AssignScenarios schema from the shop-msg catalog
  When I construct an AssignScenarios instance supplying only the fields the schema marks as required, with no field whose name begins with "bd_" or otherwise references a beads issue identifier
  Then construction succeeds
  And no schema validation error is raised
  And no required field of the schema names a beads identifier in its name, type, or validation pattern

  @scenario_hash:6fa7c60a9518aff4 @bc:shopsystem-messaging
  Scenario: A minimal valid request_bugfix message can be constructed without supplying any bd-related field
  Given the RequestBugfix schema from the shop-msg catalog
  When I construct a RequestBugfix instance supplying only the fields the schema marks as required, with no field whose name begins with "bd_" or otherwise references a beads issue identifier
  Then construction succeeds
  And no schema validation error is raised
  And no required field of the schema names a beads identifier in its name, type, or validation pattern

  @scenario_hash:c9d7a3dcd7fbbfca @bc:shopsystem-messaging
  Scenario: A minimal valid request_maintenance message can be constructed without supplying any bd-related field
  Given the RequestMaintenance schema from the shop-msg catalog
  When I construct a RequestMaintenance instance supplying only the fields the schema marks as required, with no field whose name begins with "bd_" or otherwise references a beads issue identifier
  Then construction succeeds
  And no schema validation error is raised
  And no required field of the schema names a beads identifier in its name, type, or validation pattern

  @scenario_hash:2e2aa2be87cc0498 @bc:shopsystem-messaging
  Scenario: A minimal valid clarify message can be constructed without supplying any bd-related field
  Given the Clarify schema from the shop-msg catalog
  When I construct a Clarify instance supplying only the fields the schema marks as required, with no field whose name begins with "bd_" or otherwise references a beads issue identifier
  Then construction succeeds
  And no schema validation error is raised
  And no required field of the schema names a beads identifier in its name, type, or validation pattern

  @scenario_hash:3230d5c4f056c9c8 @bc:shopsystem-messaging
  Scenario: A minimal valid work_done message can be constructed without supplying any bd-related field
  Given the WorkDone schema from the shop-msg catalog
  When I construct a WorkDone instance supplying only the fields the schema marks as required, with no field whose name begins with "bd_" or otherwise references a beads issue identifier
  Then construction succeeds
  And no schema validation error is raised
  And no required field of the schema names a beads identifier in its name, type, or validation pattern

  @scenario_hash:9c93d87a3a003f21 @bc:shopsystem-messaging
  Scenario: A minimal valid mechanism_observation message can be constructed without supplying any bd-related field
  Given the MechanismObservation schema from the shop-msg catalog
  When I construct a MechanismObservation instance supplying only the fields the schema marks as required, with no field whose name begins with "bd_" or otherwise references a beads issue identifier
  Then construction succeeds
  And no schema validation error is raised
  And no required field of the schema names a beads identifier in its name, type, or validation pattern
