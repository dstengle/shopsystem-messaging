Feature: Outbox NOTIFY and shop-msg watch --lead mode

  @scenario_hash:2c21b60536c8f493 @bc:shopsystem-messaging
  @bc:shopsystem-messaging
Scenario: A Postgres NOTIFY is fired on the outbox channel after a BC writes a respond work_done message
  Given an empty BC at a temporary path
  And a shop-msg watch --lead session is LISTEN-ing on the outbox channel for that BC
  And an inbox message with work-id "lead-300" has been sent to that BC
  When shop-msg respond work_done is called for work-id "lead-300" at that BC root
  Then the LISTEN session receives a NOTIFY with payload "lead-300" on the outbox channel
  And the NOTIFY arrives within 3 seconds of the respond call

  @scenario_hash:72c3cf64c7463208 @bc:shopsystem-messaging
  @bc:shopsystem-messaging
Scenario: shop-msg watch --lead outputs one line to stdout when any BC writes a respond message
  Given a lead root directory containing two empty BCs at temporary paths
  And shop-msg watch --lead is running in the background and has completed its startup drain
  When a shop-msg respond work_done message with work-id "lead-301" is inserted into the first BC's outbox
  Then shop-msg watch --lead outputs exactly one line to stdout for work_id "lead-301"
  And no additional output line arrives within 2 seconds

  @scenario_hash:d13a5258dd3971d1 @bc:shopsystem-messaging
  @bc:shopsystem-messaging
Scenario: shop-msg watch --bc behavior is unchanged after outbox NOTIFY is added
  Given an empty BC at a temporary path with no unprocessed inbox messages
  And shop-msg watch --bc is running in the background and has completed its startup drain
  When a new assign_scenarios message with work-id "lead-302" is inserted into the inbox
  Then shop-msg watch --bc outputs exactly one line to stdout for work_id "lead-302"
  And no additional output line arrives within 2 seconds
