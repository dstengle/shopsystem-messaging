Feature: on a version-tag release, each framework-utility repo emits a repository_dispatch to the bc-launcher build workflow

  @scenario_hash:b891abf0d7ce801f @bc:shopsystem-messaging
  Scenario: on a version-tag release of shopsystem-messaging, its release workflow emits a repository_dispatch to the bc-launcher repository
    Given the shopsystem-messaging source repository
    And a tag named "v0.2.0" is pushed to its "main" branch
    When the shopsystem-messaging release workflow associated with that tag push runs to successful completion
    Then the workflow performs a "repository_dispatch" API call targeting the "shopsystem-bc-launcher" repository
    And that dispatch call carries a credential authorized to dispatch to the bc-launcher repository
