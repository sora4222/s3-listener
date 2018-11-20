Feature: Listen to S3File and store changes in KafkaProducer

  Scenario: S3 has no files on it
    Given an S3 bucket
    And a S3Listener
    And a Storable
    And a KafkaProducer
    When the S3 bucket is empty
    Then no messages should be sent to the KafkaProducer

  Scenario Outline: S3 has just gotten objects put in it
    Given an S3 bucket
    And a S3Listener
    And a Storable
    And a KafkaProducer
    When the S3 has just had <number_of_objects> objects put in it
    Then the KafkaProducer should have <number_of_objects> messages sent to it.
    Examples:
      | number_of_objects |
      | 1                 |
      | 2                 |
      | 20                |
      | 10000             |
      | 99999999999       |
