package com.listener.filesystem;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
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

    public S3FileSystem(String bucketToList) {
        this.bucketToList = bucketToList;
    }

    /**
     * Lists the objects contained in the S3 bucket
     *
     * @return A set of all the locations in string format
     */
    @Override
    public Set<String> list() {
        logger.debug("Listing S3 bucket: {}", bucketToList);
        ListObjectsV2Result listResults = s3.listObjectsV2(bucketToList);
        List<S3ObjectSummary> listResultSummaries = listResults.getObjectSummaries();
        Set<String> setOfKeys = new HashSet<>(listResultSummaries.size());

        // Adds the key of each object to the set
        for (S3ObjectSummary summaries : listResultSummaries) {
            setOfKeys.add(summaries.getKey());
        }
        return setOfKeys;
    }

    @Override
    public String getIdentifier() {
        return "S3 FileSystem: " + bucketToList;
    }

}
