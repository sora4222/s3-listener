Feature: Listen to a file system and store changes in KafkaProducer

  Scenario: S3 has no files on it
    Given a FileSystem with a list ability
    And a SQLite Storable
    And a Mock KafkaProducer
    And a FileSystemListen
    When the FileSystem is empty
    And the FileSystemListen listens to the bucket
    Then the KafkaProducer should have had 0 messages sent to it.

  Scenario Outline: S3 has just gotten objects put in it
    Given a FileSystem with a list ability
    And a SQLite Storable
    And a Mock KafkaProducer
    And a FileSystemListen
    When the FileSystem has just had <number_of_objects> objects put in it
    And the FileSystemListen listens to the bucket
    And the KafkaProducer <successfully_or_not> sent <all_or_not> of the messages
    Then the KafkaProducer should have had <number_of_objects> messages sent to it.
    But the Storable should have had <output> messages written to it

    Examples:
      | number_of_objects | output | successfully_or_not | all_or_not |
      | 1                 | 1      | successfully        | all        |
      | 2                 | 2      | successfully        | all        |
      | 20                | 14     | successfully        | 70 percent |
      | 1000              | 0      | unsuccessfully      | all        |
      | 100000            | 30000  | unsuccessfully      | 70 percent |
      | 100000            | 100000 | successfully        | all        |
