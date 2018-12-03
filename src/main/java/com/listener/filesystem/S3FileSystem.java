package com.listener.filesystem;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.listener.FileSystemListen;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class S3FileSystem implements FileSystem {
    private final Logger logger = LoggerFactory.getLogger(FileSystemListen.class.getName());
    private final AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
    private final String bucketToList;
    private final int maxNumberToListInOneGo;

    /**
     * @param bucketToList The bucket resource to list.
     */
    public S3FileSystem(String bucketToList) {
        this(bucketToList, 10000);
    }

    /**
     * Controls the max number of keys to list from the bucket in one list request.
     * But will not stop downloading until all keys are downloaded.
     *
     * @param bucketToList
     * @param maxNumberToListInOneGo
     */
    public S3FileSystem(String bucketToList, int maxNumberToListInOneGo) {
        this.bucketToList = bucketToList;
        this.maxNumberToListInOneGo = maxNumberToListInOneGo;
    }

    /**
     * Lists the objects contained in the S3 bucket
     * TODO: Need to make this list all, currently limited to first 1000.
     * @return A set of all the locations in string format
     */
    @Override
    public Set<String> list() {
        logger.debug("Listing S3 bucket: {}", bucketToList);
        ListObjectsV2Request bucketRequest = new ListObjectsV2Request().withBucketName(bucketToList)
                .withMaxKeys(this.maxNumberToListInOneGo);
        ListObjectsV2Result listResults = s3.listObjectsV2(bucketRequest);

        // Obtain the list of objects
        List<S3ObjectSummary> listResultSummaries = listResults.getObjectSummaries();

        // Create the first hashset
        Set<String> setOfKeys = new HashSet<>(listResultSummaries.size());
        // Adds the key of each object to the set
        for (S3ObjectSummary summaries : listResultSummaries) {
            setOfKeys.add(summaries.getKey());
        }


        // Check if the results were truncated
        while (listResults.isTruncated()) {
            logger.info("The s3FileSystem is continuing to the next {} keys", listResults.getMaxKeys());
            String nextContinuationToken = listResults.getNextContinuationToken();
            logger.debug("Next continuation token for S3 is: {}", nextContinuationToken);
            bucketRequest.setContinuationToken(nextContinuationToken);

            // Loop: Obtain next results
            listResults = s3.listObjectsV2(bucketRequest);
            listResultSummaries = listResults.getObjectSummaries();
            listResultSummaries.forEach((summary) -> {
                if (setOfKeys.contains(summary.getKey()))
                    logger.warn("The key {} was already included in the S3FileSystem.", summary.getKey());
                setOfKeys.add(summary.getKey());
            });
        }

        return setOfKeys;
    }

    @Override
    public String getIdentifier() {
        return "S3 FileSystem: " + bucketToList;
    }

}
