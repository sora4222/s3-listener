package com.Jesse;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.*;

/**
 * S3Listen will listen to a provided S3 bucket and return information
 * on the events occuring on it using a polling method. This is
 * intended to be a temporary replacement for those that are not
 * allowed to use the standard SNS or Lambda methods.
 */
public class S3Listen {
    final AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
    private final Properties S3ListenProperties;
    private final Bucket bucket;
    private final Duration timeBetweenPolls;
    private final KafkaProducer<String, String> kafkaProducer;
    private String AwsAccessKey;
    private String AwsSecretKey;
    private final Storable storageForS3List;
    private final String bucketName;
    private final HashSet<String> cache;
    /**
     *
     * @param bucket Bucket to poll
     * @param timeBetweenPolls
     * @param storageForS3List
     * @param S3ListenProperties
     */
    public S3Listen(Bucket bucket,
                    Duration timeBetweenPolls,
                    Storable storageForS3List,
                    Properties S3ListenProperties,
                    KafkaProducer<String, String> kafkaProducer){
        this.bucket = bucket;
        this.timeBetweenPolls = timeBetweenPolls;
        this.S3ListenProperties = S3ListenProperties;

        this.kafkaProducer = kafkaProducer;
        AwsAccessKey = "";
        AwsSecretKey = "";
        this.storageForS3List = storageForS3List;
        this.bucketName = S3ListenProperties.getProperty("bucketName");

        determine_if_iam_role_or_secret_key();
        cache = new HashSet<>(20000);
    }

    private void determine_if_iam_role_or_secret_key(){

    }

    /**
     * Begins listening to the S3 bucket - is blocking
     */
    public void listen(){
        // Reads and/or sets up the Storable
        readCurrentCache();

        while(true){
            // Sets up the connection


            // Calls list
            Set<String> currentS3Files = callListOnBucket(bucketName);

            // Compares the called list with the read list
            Set<String> differenceBetween = queryTheDifferenceInCache(currentS3Files);

//            Set<String> differenceBetween = new HashSet<>(currentS3Files);
//            boolean differencesOccured = differenceBetween.removeAll(storageSet);

            // Takes any of the difference, cycles through it
            // Sends any of the differences to the kafka topic setup.
            differenceBetween.forEach((fileKeyInBucketNotRecordedPreviously) -> {
                kafkaProducer.send(
                        new ProducerRecord<>(bucketName + "ListenTopic",
                                fileKeyInBucketNotRecordedPreviously),
                        // CallBack, only runs when the send has been performed
                        (metadata,exceptionNullIfNone)->{if(exceptionNullIfNone == null) writeKeyToCache(fileKeyInBucketNotRecordedPreviously);}
                        );

            });
        }
    }

    /**
     * Uses the cache to query the difference between the S3Bucket now and before
     * Will query the cache backing if it doesn't contain the file in the cache
     * @param currentS3Files
     * @return
     */
    private Set<String> queryTheDifferenceInCache(Set<String> currentS3Files) {
        return null;
    }

    private void writeKeyToCache(String fileKeyInBucketNotRecordedPreviously) {

    }

    /**
     * Takes the storage and loads in as many of the object keys into the cache
     */
    private void readCurrentCache() {
        try{
            for(String keyObtained : storageForS3List.keysIterable()){
                cache.add(keyObtained);
            }
        }
        catch (OutOfMemoryError exc){
            return;
        }
    }

    private Set<String> callListOnBucket(String bucketToList) {
        ListObjectsV2Result listResults = s3.listObjectsV2(bucketToList);
        List<S3ObjectSummary> listResultSummaries = listResults.getObjectSummaries();
        Set<String> setOfKeys = new HashSet<>(listResultSummaries.size());

        // Adds the key of each object to the set
        for(S3ObjectSummary summaries: listResultSummaries){
            setOfKeys.add(summaries.getKey());
        }
        return setOfKeys;
    }
}