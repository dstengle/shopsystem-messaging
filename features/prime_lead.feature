Feature: shop-msg prime --lead — lead shop context priming

  @scenario_hash:d61a143f647d632f @bc:shopsystem-messaging
  Scenario: shop-msg prime --lead exits zero and reports DB reachability when the database is reachable
  Given a registered lead shop at a temporary path
  And the environment variable SHOPMSG_DSN is set to a reachable Postgres instance
  When I run shop-msg prime --lead <name> for the registered lead shop
  Then the command exits zero
  And stdout contains "DB reachable: yes"

  @scenario_hash:b20b25a05f147ead @bc:shopsystem-messaging
  Scenario: shop-msg prime --lead output includes pending outbox count
  Given a registered lead shop at a temporary path
  And two BC outbox rows are present in Postgres for that lead shop, both unconsumed
  When I run shop-msg prime --lead <name> for the registered lead shop
  Then the command exits zero
  And stdout contains "Pending outbox responses: 2"

  @scenario_hash:998dc8df4b103a22 @bc:shopsystem-messaging
  Scenario: shop-msg prime --lead output includes shop-msg respond clarify in the key commands section
  Given a registered lead shop at a temporary path
  And the environment variable SHOPMSG_DSN is set to a reachable Postgres instance
  When I run shop-msg prime --lead <name> for the registered lead shop
  Then the command exits zero
  And stdout contains "shop-msg respond clarify"

  @scenario_hash:d16569f25194d6bc @bc:shopsystem-messaging
  Scenario: shop-msg prime --lead annotates respond clarify to show the lead is the caller
  Given a registered lead shop at a temporary path
  And the environment variable SHOPMSG_DSN is set to a reachable Postgres instance
  When I run shop-msg prime --lead <name> for the registered lead shop
  Then the command exits zero
  And stdout contains "shop-msg respond clarify" on a line that also contains text indicating the lead answers BC questions

  @scenario_hash:af462b8837827091 @bc:shopsystem-messaging
  Scenario: shop-msg prime --lead exits non-zero with an error message when the lead name is not registered
  Given no lead shop named "ghost-lead" is registered in the shop registry
  When I run shop-msg prime --lead ghost-lead
  Then the command exits non-zero
  And stderr contains "ghost-lead"
