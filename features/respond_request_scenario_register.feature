@bc:shopsystem-messaging @origin:brief-014
Feature: shop-msg respond request_scenario_register — carry the register back over the wire

  The respond-vehicle family already carries a BC's answer back into the
  requester's inbox; this pins the request_scenario_register respond
  round-trip. Unlike the bare-hash completion journal, the register response
  carries a LIST of per-entry records — each entry exposing its block-only
  canonical hash, the scenario's title and step text, its features/ file
  location, and a live-or-retired status — so the requester can locate,
  import, or supersede each pinned scenario from the response alone.

  @scenario_hash:2c8501835cf1f5f8 @bc:shopsystem-messaging
  Scenario: responding to a request_scenario_register carries each register entry back over the wire to the requester
    Given an inbox holding an unprocessed request_scenario_register request for work_id "lead-402" naming target bounded context "shopsystem-templates"
    When shop-msg responds to request_scenario_register for work_id "lead-402" with two register entries, each carrying a block-only canonical scenario hash, the scenario's title and step text, the scenario's features/ file location, and a status of either live or retired
    Then the requester can read a request_scenario_register response for work_id "lead-402" whose register-entries field reproduces those two entries, each carrying its block-only canonical hash together with its scenario title and step text, its features/ file location, and its live-or-retired status
    And that response validates against the RequestScenarioRegister response schema
