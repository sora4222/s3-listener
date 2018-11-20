package com.listener.com.jesse;

import com.listener.S3Listen;
import com.listener.storable.SQLLiteStorable;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Properties;

import static java.lang.System.exit;

/**
 * Runs the listener
 */
public class run {
    private final static Logger logger = LoggerFactory.getLogger(run.class.getName());

    public static Properties loadProperties(String fileName) {
        Properties propertiesConfig = null;
        try {
            FileInputStream propFileStream = new FileInputStream(fileName);
            propertiesConfig.load(propFileStream);
        } catch (IOException e) {
            logger.debug(e.getMessage());
            exit(1);
        }
    }

    public static void main(String[] args) {
        Properties propertiesToWrite = null;
        try {
            FileInputStream awsPropFile = new FileInputStream("aws.properties");
            propertiesToWrite = new Properties(System.getProperties());
            propertiesToWrite.load(awsPropFile);
        } catch (IOException e) {
            logger.debug("AWS_ACCESS_KEY_ID has not been uploaded to the system, this will" +
                    "cause the IAM role to be loaded if it exists : " + e.getMessage());
        }
        System.setProperties(propertiesToWrite);

        Properties bucketConfig = loadProperties("config.properties");
        Properties kafkaProducerProperties = loadProperties("kafkaProducer.properties");

        S3Listen fileListener = new S3Listen(Duration.ofSeconds(20),
                bucketConfig,
                new SQLLiteStorable()
        new KafkaProducer<String, String>(kafkaProducerProperties, new StringSerializer(), new StringSerializer()));
    }
}
