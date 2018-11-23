package com.listener;

import com.listener.filesystem.FileSystem;
import com.listener.storable.Storable;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import java.util.Set;

import static java.lang.Thread.sleep;

/**
 * FileSystemListen will listen_forever to a provided S3 bucket and return information
 * on the events occuring on it using a polling method. This is
 * intended to be a temporary replacement for those that are not
 * allowed to use the standard SNS or Lambda methods.
 */
public class FileSystemListen {
    private static final Logger logger = LoggerFactory.getLogger(FileSystemListen.class.getName());
    private static boolean runBool;
    private final Properties S3ListenProperties;
    private final Duration timeBetweenPolls;
    private final Producer<String, String> kafkaProducer;
    private final Storable storageForLocationsPreviouslyLocated;
    private final String bucketName;
    private final FileSystem fileSystem;

    /**
     * @param fileSystem                           An implementation of filesystem that will be polled for file locations
     * @param timeBetweenPolls                     A duration between pings for the s3bucket
     * @param S3ListenProperties                   A properties that will eventually determine the objects behaviour
     *                                             currently, it only obtains the "bucketName" from this.
     * @param storageForLocationsPreviouslyLocated An object that implements the {@link Storable} interface, this will
     *                                             be used to store the file locations processed.
     * @param kafkaProducer                        A {@link KafkaProducer} that will be used to store the files
     */
    public FileSystemListen(FileSystem fileSystem,
                            Duration timeBetweenPolls,
                            Properties S3ListenProperties,
                            Storable storageForLocationsPreviouslyLocated,
                            Producer<String, String> kafkaProducer) {

        this.fileSystem = fileSystem;
        this.timeBetweenPolls = timeBetweenPolls;
        this.S3ListenProperties = S3ListenProperties;

        this.kafkaProducer = kafkaProducer;
        this.storageForLocationsPreviouslyLocated = storageForLocationsPreviouslyLocated;

        this.bucketName = S3ListenProperties.getProperty("bucketName");

        logger.info("The bucket name has been set to: " + bucketName);
        runBool = true;
    }

    /**
     * Listens to the file system in an infinite loop.
     */
    public void listen_forever() {
        runBool = true;
        while (runBool) {
            listen_once();
            try {
                sleep(this.timeBetweenPolls.toMillis());
            } catch (InterruptedException e) {
                logger.debug("An interrupt occurred: {}", e.getMessage());
            }
        }
    }

    /**
     * Performs a single poll of the filesystem.
     */
    public void listen_once() {


            // Adds a shutdown hook for this thread.
            final Thread mainThread = Thread.currentThread();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("The shutdown hook has been triggered");
                try {
                    // Stops anymore runs from occurring, gives a five second cooldown.
                    runBool = false;

                    // Closes all connections
                    storageForLocationsPreviouslyLocated.close();
                    kafkaProducer.close();
                    logger.info("All shutdown actions have been completed successfully.");

                    mainThread.join(5000);

                } catch (InterruptedException | IOException exc) {
                    logger.warn("An exception has occured during shutdown: \n" + exc.getMessage());
                }
            }));

        logger.trace("A poll of the filesystem {} is beginning", fileSystem.getIdentifier());
        // Calls list
        Set<String> currentS3Files = fileSystem.list();
        logger.info("The number of files listed is: {}", currentS3Files.size());

        // Compares the called list with the read list
        Set<String> differenceBetween = queryTheDifferenceFromStorable(currentS3Files);
        logger.info("The number of files not in the storable: {}", differenceBetween.size());

        // Takes any of the difference, cycles through it
        // Sends any of the differences to the kafka topic setup.
        differenceBetween.forEach((fileKeyInBucketNotRecordedPreviously) -> kafkaProducer.send(
                new ProducerRecord<>(bucketName + "ListenTopic",
                        fileKeyInBucketNotRecordedPreviously),
                // CallBack, only runs when the send has been performed
                (metadata, exceptionNullIfNone) -> {
                    if (exceptionNullIfNone == null)
                        writeKeyToStorage(fileKeyInBucketNotRecordedPreviously);
                    else logger.warn("A key has failed to be sent to kafka, " +
                                    "File location: {}\nException", fileKeyInBucketNotRecordedPreviously,
                            exceptionNullIfNone.getMessage());
                }));

        logger.debug("Going to sleep for: " + timeBetweenPolls.toString());
        // Sleep for the intended period of time

    }

    /**
     * Uses the cache to query the difference between the S3Bucket now and before
     * Will query the cache backing if it doesn't contain the file in the cache
     *
     * @param currentS3Files the S3 objects that is in the bucket
     * @return the S3Key files that have been read before
     */
    private Set<String> queryTheDifferenceFromStorable(Set<String> currentS3Files) {
        logger.trace("queryTheDifferenceFromStorable");
        currentS3Files.removeIf(storageForLocationsPreviouslyLocated::keyAlreadyRead);
        return currentS3Files;
    }

    /**
     * Writes the key to the storable
     *
     * @param fileKeyInBucketNotRecordedPreviously A string of the file location to be stored in the storable
     */
    private void writeKeyToStorage(String fileKeyInBucketNotRecordedPreviously) {
        storageForLocationsPreviouslyLocated.putKey(fileKeyInBucketNotRecordedPreviously);
    }
}
