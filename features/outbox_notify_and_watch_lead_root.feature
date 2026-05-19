Feature: Outbox NOTIFY and shop-msg watch --lead-root mode

  @scenario_hash:b4d0e28257f26985 @bc:shopsystem-messaging
  @bc:shopsystem-messaging
Scenario: A Postgres NOTIFY is fired on the outbox channel after a BC writes a respond work_done message
  Given an empty BC at a temporary path
  And a shop-msg watch --lead-root session is LISTEN-ing on the outbox channel for that BC
  And an inbox message with work-id "lead-300" has been sent to that BC
  When shop-msg respond work_done is called for work-id "lead-300" at that BC root
  Then the LISTEN session receives a NOTIFY with payload "lead-300" on the outbox channel
  And the NOTIFY arrives within 3 seconds of the respond call

  @scenario_hash:3acbf477af0c3f0e @bc:shopsystem-messaging
  @bc:shopsystem-messaging
Scenario: shop-msg watch --lead-root outputs one line to stdout when any BC writes a respond message
  Given a lead root directory containing two empty BCs at temporary paths
  And shop-msg watch --lead-root is running in the background and has completed its startup drain
  When a shop-msg respond work_done message with work-id "lead-301" is inserted into the first BC's outbox
  Then shop-msg watch --lead-root outputs exactly one line to stdout for work_id "lead-301"
  And no additional output line arrives within 2 seconds

  @scenario_hash:b4083b5ff38638f7 @bc:shopsystem-messaging
  @bc:shopsystem-messaging
Scenario: shop-msg watch --bc-root behavior is unchanged after outbox NOTIFY is added
  Given an empty BC at a temporary path with no unprocessed inbox messages
  And shop-msg watch --bc-root is running in the background and has completed its startup drain
  When a new assign_scenarios message with work-id "lead-302" is inserted into the inbox
  Then shop-msg watch --bc-root outputs exactly one line to stdout for work_id "lead-302"
  And no additional output line arrives within 2 seconds
