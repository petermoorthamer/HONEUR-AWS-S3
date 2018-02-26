package com.jnj.honeur.aws.s3;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class AmazonS3Service {

    private static final Logger LOGGER = LoggerFactory.getLogger(AmazonS3Service.class);

    private AmazonS3 s3;

    public AmazonS3Service(final AmazonS3 s3) {
        this.s3 = s3;
    }

    public AmazonS3Service(final BasicAWSCredentials credentials) {
        s3 = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(credentials)).build();
    }

    public Bucket getBucket(String bucketName) {
        Bucket namedBucket = null;
        List<Bucket> buckets = s3.listBuckets();
        for (Bucket b : buckets) {
            if (b.getName().equals(bucketName)) {
                namedBucket = b;
            }
        }
        return namedBucket;
    }

    public List<Bucket> getAllBuckets() {
        return s3.listBuckets();
    }

    public void logAllBuckets() {
        final List<Bucket> buckets = s3.listBuckets();
        LOGGER.info("Your Amazon S3 buckets are:");
        for (Bucket b : buckets) {
            LOGGER.info("* " + b.getName());
        }
    }

    public Bucket createBucket(String bucketName, String region) throws AmazonS3Exception {
        if (s3.doesBucketExistV2(bucketName)) {
            LOGGER.info("Bucket %s already exists.\n", bucketName);
            return getBucket(bucketName);
        } else {
            return s3.createBucket(new CreateBucketRequest(bucketName, region));
        }
    }

    public void deleteBucket(final String bucketName) throws AmazonServiceException {
        LOGGER.debug("Deleting S3 bucket: " + bucketName);
        LOGGER.debug(" - removing objects from bucket");
        ObjectListing objectListing = s3.listObjects(bucketName);
        while (true) {
            for (Iterator<?> iterator = objectListing.getObjectSummaries().iterator(); iterator.hasNext(); ) {
                S3ObjectSummary summary = (S3ObjectSummary) iterator.next();
                s3.deleteObject(bucketName, summary.getKey());
            }
            // more objectListing to retrieve?
            if (objectListing.isTruncated()) {
                objectListing = s3.listNextBatchOfObjects(objectListing);
            } else {
                break;
            }
        }
        ;

        LOGGER.debug(" - removing versions from bucket");
        VersionListing versionListing = s3.listVersions(new ListVersionsRequest().withBucketName(bucketName));
        while (true) {
            for (Iterator<?> iterator = versionListing.getVersionSummaries().iterator(); iterator.hasNext(); ) {
                S3VersionSummary vs = (S3VersionSummary) iterator.next();
                s3.deleteVersion(bucketName, vs.getKey(), vs.getVersionId());
            }

            if (versionListing.isTruncated()) {
                versionListing = s3.listNextBatchOfVersions(versionListing);
            } else {
                break;
            }
        }

        LOGGER.debug(" OK, bucket ready to delete!");
        s3.deleteBucket(bucketName);

        LOGGER.debug("Bucket deleted!");
    }

    public S3Object getObject(String bucketName, String keyName) throws AmazonServiceException, IOException {
        LOGGER.debug("Downloading %s from S3 bucket %s...\n", keyName, bucketName);
        return s3.getObject(bucketName, keyName);
    }

    public File getObjectFile(String bucketName, String keyName) throws AmazonServiceException, IOException {
        LOGGER.debug("Downloading %s from S3 bucket %s...\n", keyName, bucketName);
        S3Object s3Object = s3.getObject(bucketName, keyName);
        File file = new File(keyName);
        writeFile(s3Object, file);
        return file;
    }

    private void writeFile(S3Object s3Object, File file) throws IOException {
        S3ObjectInputStream s3is = s3Object.getObjectContent();
        FileOutputStream fos = new FileOutputStream(file);
        byte[] read_buf = new byte[1024];
        int read_len = 0;
        while ((read_len = s3is.read(read_buf)) > 0) {
            fos.write(read_buf, 0, read_len);
        }
        s3is.close();
        fos.close();
    }

    public void putObject(String bucketName, File file) throws AmazonServiceException {
        String filePath = file.getAbsolutePath();
        String keyName = file.getName();
        LOGGER.debug("Uploading %s to S3 bucket %s...\n", filePath, bucketName);
        s3.putObject(bucketName, keyName, filePath);
    }

    public void copyObject(String objectKey, String fromBucket, String toBucket) throws AmazonServiceException {
        s3.copyObject(fromBucket, objectKey, toBucket, objectKey);
    }

    public ListObjectsV2Result getObjects(String bucketName) {
        return s3.listObjectsV2(bucketName);
    }

    public void logObjects(String bucketName) {
        ListObjectsV2Result result = s3.listObjectsV2(bucketName);
        List<S3ObjectSummary> objects = result.getObjectSummaries();
        for (S3ObjectSummary os: objects) {
            LOGGER.info("* " + os.getKey());
        }
    }

    public void deleteObject(String bucketName, String objectKey) throws AmazonServiceException {
        s3.deleteObject(bucketName, objectKey);
    }

    public void deleteObjects(String bucketName, String... objectKeys) throws AmazonServiceException {
        DeleteObjectsRequest dor = new DeleteObjectsRequest(bucketName).withKeys(objectKeys);
        s3.deleteObjects(dor);
    }
}
