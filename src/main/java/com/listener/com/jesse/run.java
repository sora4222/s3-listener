package com.listener.com.jesse;

import com.listener.FileSystemListen;
import com.listener.filesystem.S3FileSystem;
import com.listener.storable.SQLiteStorable;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Properties;

import static java.lang.System.exit;

/**
 * Runs the listener
 */
public class run {
    private final static Logger logger = LoggerFactory.getLogger(run.class.getName());

    private static Properties loadProperties(String fileName) {
        Properties propertiesConfig = new Properties();
        try {
            InputStream propFileStream = run.class.getClassLoader().getResourceAsStream(fileName);
            propertiesConfig.load(propFileStream);
            return propertiesConfig;
        } catch (IOException e) {
            logger.debug("Loading property file: " + fileName + "\n" + e.getMessage());
            exit(1);
            return null;
        }
    }

    public static void main(String[] args) {
        Properties propertiesToWrite = null;
        try {
            InputStream awsPropFile = run.class.getClassLoader().getResourceAsStream("aws.properties");
            propertiesToWrite = new Properties(System.getProperties());
            propertiesToWrite.load(awsPropFile);
        } catch (IOException | NullPointerException e) {
            logger.debug("AWS_ACCESS_KEY_ID has not been uploaded to the system, this will" +
                    "cause the IAM role to be loaded if it exists or files from the default" +
                    "credentials file: " + e.getMessage());
        }
        System.setProperties(propertiesToWrite);

        Properties generalConfig = loadProperties("config.properties");
        Properties kafkaProducerProperties = loadProperties("kafkaProducer.properties");

        assert generalConfig != null;

        FileSystemListen fileListener = new FileSystemListen(
                new S3FileSystem(generalConfig.getProperty("bucketName")),
                Duration.ofSeconds(20),
                generalConfig,
                new SQLiteStorable(generalConfig),
                new KafkaProducer<>(kafkaProducerProperties, new StringSerializer(), new StringSerializer()));

        fileListener.listen();
    }
}
