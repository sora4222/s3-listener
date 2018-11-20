package com.listener;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static java.lang.Thread.sleep;

/**
 * S3Listen will listen to a provided S3 bucket and return information
 * on the events occuring on it using a polling method. This is
 * intended to be a temporary replacement for those that are not
 * allowed to use the standard SNS or Lambda methods.
 */
public class S3Listen {
    private final AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
    private final Properties S3ListenProperties;
    private final Duration timeBetweenPolls;
    private final KafkaProducer<String, String> kafkaProducer;
    private final Storable storageForS3List;
    private final String bucketName;
    private static boolean runBool;
    private static final Logger logger = LoggerFactory.getLogger(S3Listen.class.getName());
    /**
     *
     * @param timeBetweenPolls A duration between pings for the s3bucket
     * @param S3ListenProperties A properties that will eventually determine the objects behaviour
     *                           currently, it only obtains the "bucketName" from this.
     * @param storageForS3List An object that implements the {@link Storable} interface, this will
     *                         be used to store the file locations processed.
     * @param kafkaProducer A {@link KafkaProducer} that will be used to store the files
     */
    public S3Listen(Duration timeBetweenPolls,
                    Properties S3ListenProperties,
                    Storable storageForS3List,
                    KafkaProducer<String, String> kafkaProducer){


        this.timeBetweenPolls = timeBetweenPolls;
        this.S3ListenProperties = S3ListenProperties;

        this.kafkaProducer = kafkaProducer;
        this.storageForS3List = storageForS3List;

        this.bucketName = S3ListenProperties.getProperty("bucketName");

        logger.info("The bucket name has been set to: " + bucketName);
        runBool = true;
    }

    /**
     * Begins listening to the S3 bucket - is blocking
     */
    public void listen(){
        try {

            // Adds a shutdown hook for this thread.
            final Thread mainThread = Thread.currentThread();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("The shutdown hook has been triggered");
                try {
                    // Stops anymore runs from occurring, gives a five second cooldown.
                    runBool = false;
                    mainThread.join(5000);

                    // Closes all connections
                    storageForS3List.close();
                    kafkaProducer.close();
                    logger.info("All shutdown actions have been completed successfully.");
                }
                catch (InterruptedException exc){
                    logger.warn("An exception has occured during shutdown: \n" + exc.getMessage());
                }
            }));

            while (runBool) {
                logger.trace("A poll run is beginning");
                // Calls list
                Set<String> currentS3Files = callListOnBucket(bucketName);
                logger.info("The number of files listed is: {}", currentS3Files.size());

                // Compares the called list with the read list
                Set<String> differenceBetween = queryTheDifferenceFromStorable(currentS3Files);
                logger.info("The number of files not in the storable: {}", differenceBetween.size());

                // Takes any of the difference, cycles through it
                // Sends any of the differences to the kafka topic setup.
                differenceBetween.forEach((fileKeyInBucketNotRecordedPreviously) -> {
                    kafkaProducer.send(
                            new ProducerRecord<>(bucketName + "ListenTopic",
                                    fileKeyInBucketNotRecordedPreviously),
                            // CallBack, only runs when the send has been performed
                            (metadata, exceptionNullIfNone) -> {
                                if (exceptionNullIfNone == null)
                                    writeKeyToStorage(fileKeyInBucketNotRecordedPreviously);
                                else logger.warn("A key has failed to be sent to kafka, " +
                                        "File location: " + fileKeyInBucketNotRecordedPreviously);
                            });

                });

                logger.debug("Going to sleep for: " + timeBetweenPolls.toString());
                // Sleep for the intended period of time
                sleep(this.timeBetweenPolls.toMillis());
            }
        } catch (InterruptedException exc){
            logger.warn("An exception has occured during execution: " + exc.getMessage());
        }
    }

    /**
     * Uses the cache to query the difference between the S3Bucket now and before
     * Will query the cache backing if it doesn't contain the file in the cache
     * @param currentS3Files the S3 objects that is in the bucket
     * @return the S3Key files that have been read before
     */
    private Set<String> queryTheDifferenceFromStorable(Set<String> currentS3Files) {
        logger.trace("queryTheDifferenceFromStorable");
        currentS3Files.removeIf(storageForS3List::keyAlreadyRead);
        return currentS3Files;
    }

    /**
     * Writes the key to the storable
     * @param fileKeyInBucketNotRecordedPreviously A string of the file location to be stored in the storable
     */
    private void writeKeyToStorage(String fileKeyInBucketNotRecordedPreviously) {
        storageForS3List.putKey(fileKeyInBucketNotRecordedPreviously);
    }

    /**
     * Calls list on the bucket and returns the set of keys
     *
     * @param bucketToList A string naming the bucket required
     * @return The set of keys as a {@link Set}
     */
    private Set<String> callListOnBucket(String bucketToList) {
        logger.debug("callListOnBucket");
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
