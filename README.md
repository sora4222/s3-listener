# Aims
This repository is for the production of an S3 Listener.
This will:
    * Listen to an S3 bucket, listing the bucket every 30 seconds
    * Take a listing and compare it to a cache
    * Everything that hasn't been listed before will be sent to a kafka topic.
This will allow for a different form of event listening.