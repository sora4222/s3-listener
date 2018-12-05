Feature: The S3FileSystem can manage the connection to an S3 bucket

  This requires a bucket to check out.

  Scenario: The S3FileSystem connects to a large public S3 bucket
    Given a S3FileSystem connected to "listen-test-bucket"
    When the S3FileSystem does a list
    Then the returned result is equal to 2

  Scenario: The S3FileSystem connects to a large public S3 bucket
    Given a S3FileSystem connected to "listen-test-bucket" which only gets 1 file key at a time
    When the S3FileSystem does a list
    Then the returned result is equal to 2
