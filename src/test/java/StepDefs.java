import com.listener.FileSystemListen;
import com.listener.Storable;
import com.listener.filesystem.TestFileSystem;
import com.listener.storable.SQLiteStorable;
import cucumber.api.java8.En;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Assert;

import java.time.Duration;
import java.util.*;

public class StepDefs implements En {
    private FileSystemListen listen = null;
    private Storable storable = null;
    private TestFileSystem fileSystem = null;
    private Producer<String, String> kafkaProducer = null;
    private Set<String> filesListed;
    static private Random random = new Random();

    private String generateRandomFileLocations() {
        int numberOfDirectiories = 1 + random.nextInt(30);
        StringBuilder fileLocationAndName = new StringBuilder();
        for (int i = 0; i < numberOfDirectiories; i++) {
            fileLocationAndName.append(UUID.randomUUID().toString());
        }
        return fileLocationAndName.toString();
    }

    public StepDefs() {

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

        And("^a FileSystemListen$", (Integer arg0) -> {
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

        When("^the FileSystem has just had (\\d) objects put in it$", (Integer arg0) -> {
            filesListed = new HashSet<>(arg0);
            while (filesListed.size() < arg0) {
                String randomLocation = generateRandomFileLocations();
                filesListed.add(randomLocation);
                fileSystem.addFile(randomLocation);
            }
        });

        // Thens

        Then("^the KafkaProducer should have (\\d) messages sent to it\\.$", (Integer arg0) -> {
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

    }
}
