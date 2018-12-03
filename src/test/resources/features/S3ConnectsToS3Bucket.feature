Feature: The S3FileSystem can manage the connection to an S3 bucket

  Note that this requires credentials in us-east-1

  Scenario: The S3FileSystem connects to a large public S3 bucket
    Given a S3FileSystem connected to "commoncrawl"
    When the S3FileSystem does a list
    Then the returned result is over 25000000000