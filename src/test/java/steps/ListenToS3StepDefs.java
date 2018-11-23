package steps;

import com.listener.FileSystemListen;
import com.listener.filesystem.TestFileSystem;
import com.listener.storable.SQLiteStorable;
import com.listener.storable.Storable;
import cucumber.api.java8.En;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.junit.Assert;

import java.time.Duration;
import java.util.*;


public class ListenToS3StepDefs implements En {
    static private Random random = new Random();
    private FileSystemListen listen = null;
    private Storable storable = null;
    private TestFileSystem fileSystem = null;
    private Producer<String, String> kafkaProducer = null;
    private Set<String> filesListed;

    public ListenToS3StepDefs() {

        // Givens
        Given("^a FileSystem with a list ability$", () ->
        {
            fileSystem = new TestFileSystem();
        });

        And("^a Storable$", () -> {
            Properties properties = new Properties();
            properties.setProperty("InMemory", "true");
            storable = new SQLiteStorable(properties);
        });

        And("^a KafkaProducer$", () -> {
            kafkaProducer = new MockProducer<>();
        });

        And("^a FileSystemListen$", () -> {
            Properties properties = new Properties();
            properties.setProperty("bucketName", "testFileSystem");
            listen = new FileSystemListen(fileSystem,
                    Duration.ofSeconds(1),
                    properties,
                    storable,
                    kafkaProducer);
        });

        // Whens
        When("^the FileSystem is empty$", () -> {
            filesListed = new HashSet<>();
            return;
        });

        When("^the FileSystem has just had (\\d+) objects put in it$", (Integer arg0) -> {
            filesListed = new HashSet<>(arg0);
            while (filesListed.size() < arg0) {
                String randomLocation = generateRandomFileLocations();
                filesListed.add(randomLocation);
                fileSystem.addFile(randomLocation);
            }
        });

        And("^the FileSystemListen listens to the bucket$", () -> {
            listen.listen_once();
        });


        And("^the KafkaProducer (successfully|unsuccessfully) sent (all|70 percent) of the messages$",
                (String success, String allOrMost) -> {
                    MockProducer<String, String> mockProducer = (MockProducer<String, String>) kafkaProducer;
                    int number_to_retain = fileSystem.list().size();

                    if (success.equals("unsuccessfully") && allOrMost.equals("all")) {
                        number_to_retain = 0;
                    } else if (success.equals("unsuccessfully") && allOrMost.equals("70 percent")) {
                        number_to_retain = (int) Math.floor(number_to_retain * 0.30);
                    } else if (success.equals("successfully") && allOrMost.equals("70 percent")) {
                        number_to_retain = (int) Math.floor(number_to_retain * 0.70);
                    }

                    // Go through and accept or reject the needed packets
                    int number_retained = 0;
                    while (number_retained < number_to_retain) {
                        if (!mockProducer.completeNext()) break;
                        number_retained++;
                    }
                    while (true) {
                        if (!mockProducer.errorNext(new KafkaException("A fake error"))) break;
                        System.out.println("1");
                    }
                });
        // Thens

        Then("^the KafkaProducer should have had (\\d+) messages sent to it\\.$", (Integer arg0) -> {
            MockProducer mockProducer = (MockProducer) kafkaProducer;
            List<ProducerRecord<String, String>> recordsSent = mockProducer.history();

            for (ProducerRecord<String, String> record : recordsSent) {
                Assert.assertTrue(
                        "Not all the messages listed were sent to kafka\nMissing:" + record.value(),
                        filesListed.contains(record.value()));
            }

            Assert.assertEquals("The correct number of messages was not sent",
                    arg0.intValue(), recordsSent.size());
        });

        But("^the Storable should have had (\\d+) messages written to it$", (Integer messagesWritten) -> {
            // Test the number of files written to the fileSystem
            SQLiteStorable storableAsSQL = (SQLiteStorable) storable;

            Assert.assertEquals(
                    "The correct number of messages was not sent",
                    messagesWritten.intValue(),
                    storableAsSQL.count());
        });

    }

    private String generateRandomFileLocations() {
        int numberOfDirectiories = 1 + random.nextInt(30);
        StringBuilder fileLocationAndName = new StringBuilder();
        for (int i = 0; i < numberOfDirectiories; i++) {
            fileLocationAndName.append(UUID.randomUUID().toString());
            fileLocationAndName.append("/");
        }
        return fileLocationAndName.toString();
    }
}
