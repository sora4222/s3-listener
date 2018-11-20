import cucumber.api.PendingException;
import cucumber.api.java8.En;

public class StepDefs implements En {
    public StepDefs() {
        Given("^an S3 bucket$", (Integer arg0) -> {
            // Write code here that turns the phrase above into concrete actions

            throw new PendingException();
        });
        And("^a S3 Listener$", (Integer arg0) -> {
            // Write code here that turns the phrase above into concrete actions
            throw new PendingException();
        });
        And("^a Storable$", () -> {
            // Write code here that turns the phrase above into concrete actions
            throw new PendingException();
        });
        And("^a KafkaProducer$", () -> {
            // Write code here that turns the phrase above into concrete actions
            throw new PendingException();
        });
        When("^the S3 bucket is empty$", (Integer arg0) -> {
            // Write code here that turns the phrase above into concrete actions
            throw new PendingException();
        });
        Then("^(?:no|\\d) messages should be sent to the KafkaProducer$", () -> {
            // Write code here that turns the phrase above into concrete actions
            throw new PendingException();
        });
        When("^the S(\\d+) has just had <number_of_objects> objects put in it$", (Integer arg0) -> {
            // Write code here that turns the phrase above into concrete actions
            throw new PendingException();
        });
        Then("^the KafkaProducer should have <number_of_objects> messages sent to it\.$", () -> {
            // Write code here that turns the phrase above into concrete actions
            throw new PendingException();
        });
    }
}
