package steps;

import com.listener.filesystem.S3FileSystem;
import cucumber.api.java8.En;
import org.junit.Assert;

import java.util.Set;

public class S3FileSystemStepDefs implements En {
    private S3FileSystem s3FileSystem;
    private Set<String> resultSet;

    public S3FileSystemStepDefs() {
        Given("^a S3FileSystem connected to \"([^\"]*)\"$", (String bucketAddress) -> {
            s3FileSystem = new S3FileSystem(bucketAddress);
        });
        When("^the S3FileSystem does a list$", () -> {
            resultSet = s3FileSystem.list();
        });
        Then("^the returned result is over ([\\d]+)$", (String minimum) -> {
            System.out.println(minimum);
            Long value = Long.parseLong(minimum);
            Assert.assertTrue("The expected set length is greater than " + minimum + ", only "
                            + resultSet.size() + " were returned.",
                    resultSet.size() >= value);
        });
    }
}
