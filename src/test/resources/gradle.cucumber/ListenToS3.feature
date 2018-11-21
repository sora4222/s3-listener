Feature: Listen to a file system and store changes in KafkaProducer

  Scenario: S3 has no files on it
    Given a FileSystem with a list ability
    And a Storable
    And a KafkaProducer
    And a FileSystemListen
    When the FileSystem is empty
    Then the KafkaProducer should have 0 messages sent to it.

  Scenario Outline: S3 has just gotten objects put in it
    Given a FileSystem with a list ability
    And a FileSystemListen
    And a Storable
    And a KafkaProducer
    When the FileSystem has just had <number_of_objects> objects put in it
    Then the KafkaProducer should have <number_of_objects> messages sent to it.
    Examples:
      | number_of_objects |
      | 1                 |
      | 2                 |
      | 20                |
      | 10000             |
      | 99999999999       |
