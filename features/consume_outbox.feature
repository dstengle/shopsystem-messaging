Feature: shop-msg consume outbox

  @scenario_hash:2893e2b810f7f061 @bc:shopsystem-messaging
  @bc:shopsystem-messaging
Scenario: Consuming a specific outbox row removes it from pending outbox output
  Given a lead shop at a temporary path with BC clone "bc-alpha" present as a sibling directory
  And shop-msg respond work_done was previously used inside "bc-alpha" to write an outbox response with work-id "lead-500" and status "complete"
  When I run shop-msg consume outbox with --bc-root pointing at "bc-alpha", --work-id "lead-500", and --message-type "work_done"
  Then the command exits zero
  And running shop-msg pending outbox --lead-root at the lead path contains no entry for work_id "lead-500"

  @scenario_hash:8489585a7dce78e6 @bc:shopsystem-messaging
  @bc:shopsystem-messaging
Scenario: Consuming one message_type on a work_id that has multiple outbox rows leaves the other message_types visible in pending outbox
  Given a lead shop at a temporary path with BC clone "bc-alpha" present as a sibling directory
  And shop-msg respond clarify was previously used inside "bc-alpha" to write an outbox response with work-id "lead-501" and question "which acceptance criterion applies?"
  And shop-msg respond work_done was previously used inside "bc-alpha" to write an outbox response with work-id "lead-501" and status "complete"
  When I run shop-msg consume outbox with --bc-root pointing at "bc-alpha", --work-id "lead-501", and --message-type "work_done"
  Then the command exits zero
  And running shop-msg pending outbox --lead-root at the lead path includes an entry for work_id "lead-501" with message_type "clarify" originating from BC "bc-alpha"
  And running shop-msg pending outbox --lead-root at the lead path contains no entry for work_id "lead-501" with message_type "work_done"

  @scenario_hash:bce99ef2b183daba @bc:shopsystem-messaging
  @bc:shopsystem-messaging
Scenario: pending outbox returns empty output when all outbox rows for all BCs have been consumed
  Given a lead shop at a temporary path with BC clone "bc-alpha" present as a sibling directory
  And shop-msg respond work_done was previously used inside "bc-alpha" to write an outbox response with work-id "lead-502" and status "complete"
  And shop-msg consume outbox has been run with --bc-root pointing at "bc-alpha", --work-id "lead-502", and --message-type "work_done"
  When I run the shop-msg subcommand that enumerates pending unprocessed outbox responses, with no filter
  Then the command exits zero
  And stdout contains no work_id entries

  @scenario_hash:2bf977865d61d825 @bc:shopsystem-messaging
  @bc:shopsystem-messaging
Scenario: Attempting to consume an outbox row that does not exist produces a clear error and exits non-zero
  Given a lead shop at a temporary path with BC clone "bc-alpha" present as a sibling directory
  And no outbox message exists for work-id "lead-503" in "bc-alpha"
  When I run shop-msg consume outbox with --bc-root pointing at "bc-alpha", --work-id "lead-503", and --message-type "work_done"
  Then the command exits non-zero
  And stderr includes work_id "lead-503" and message_type "work_done"
