package com.jnj.honeur.aws.s3;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSSessionCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Iterator;
import java.util.List;

public class AmazonS3Service {

    private static final Logger LOGGER = LoggerFactory.getLogger(AmazonS3Service.class);

    private static final String DEFAULT_REGION = Regions.EU_WEST_1.getName();

    private AWSSessionCredentialsProvider sessionCredentialsProvider;
    private AmazonS3 s3;
    private TransferManager transferManager;

    public AmazonS3Service() {
        this(AmazonS3ClientBuilder.defaultClient());
    }

    public AmazonS3Service(final AmazonS3 s3) {
        this.s3 = s3;
        this.transferManager = TransferManagerBuilder.standard().withS3Client(s3).build();
    }

    public AmazonS3Service(final AWSCredentials credentials) {
        this.s3 = HoneurAmazonS3ClientBuilder.standardClient(credentials);
        this.transferManager = TransferManagerBuilder.standard().withS3Client(s3).build();
    }

    public AmazonS3Service(final AWSSessionCredentialsProvider sessionCredentialsProvider) {
        this.sessionCredentialsProvider = sessionCredentialsProvider;
    }

    private AmazonS3 getS3() {
        if(this.s3 != null) {
            return this.s3;
        } else {
            return HoneurAmazonS3ClientBuilder.sessionClient(sessionCredentialsProvider);
        }
    }

    private TransferManager getTransferManager(final AmazonS3 s3) {
        if(this.s3 == s3) {
            return transferManager;
        } else {
            return TransferManagerBuilder.standard().withS3Client(s3).build();
        }
    }

    public Bucket getBucket(String bucketName) {
        Bucket namedBucket = null;
        List<Bucket> buckets = getS3().listBuckets();
        for (Bucket b : buckets) {
            if (b.getName().equals(bucketName)) {
                namedBucket = b;
            }
        }
        return namedBucket;
    }

    public List<Bucket> getAllBuckets() {
        return getS3().listBuckets();
    }

    public void logAllBuckets() {
        final List<Bucket> buckets = getS3().listBuckets();
        LOGGER.info("Your Amazon S3 buckets are:");
        for (Bucket b : buckets) {
            LOGGER.info("* " + b.getName());
        }
    }

    public boolean doesBucketExist(String bucketName) {
        return getS3().doesBucketExistV2(bucketName);
    }

    public Bucket createBucket(String bucketName) throws AmazonS3Exception {
        return createBucket(bucketName, DEFAULT_REGION);
    }

    public Bucket createBucket(String bucketName, String region) throws AmazonS3Exception {
        if (getS3().doesBucketExistV2(bucketName)) {
            LOGGER.info("Bucket %s already exists.\n", bucketName);
            return getBucket(bucketName);
        } else {
            return getS3().createBucket(new CreateBucketRequest(bucketName, region));
        }
    }

    public void deleteBucket(final String bucketName) throws AmazonServiceException {
        LOGGER.debug("Deleting S3 bucket: " + bucketName);
        LOGGER.debug(" - removing objects from bucket");
        ObjectListing objectListing = getS3().listObjects(bucketName);
        while (true) {
            for (Iterator<?> iterator = objectListing.getObjectSummaries().iterator(); iterator.hasNext(); ) {
                S3ObjectSummary summary = (S3ObjectSummary) iterator.next();
                getS3().deleteObject(bucketName, summary.getKey());
            }
            // more objectListing to retrieve?
            if (objectListing.isTruncated()) {
                objectListing = getS3().listNextBatchOfObjects(objectListing);
            } else {
                break;
            }
        }

        LOGGER.debug(" - removing versions from bucket");
        VersionListing versionListing = getS3().listVersions(new ListVersionsRequest().withBucketName(bucketName));
        while (true) {
            for (Iterator<?> iterator = versionListing.getVersionSummaries().iterator(); iterator.hasNext(); ) {
                S3VersionSummary vs = (S3VersionSummary) iterator.next();
                getS3().deleteVersion(bucketName, vs.getKey(), vs.getVersionId());
            }

            if (versionListing.isTruncated()) {
                versionListing = getS3().listNextBatchOfVersions(versionListing);
            } else {
                break;
            }
        }

        LOGGER.debug(" OK, bucket ready to delete!");
        getS3().deleteBucket(bucketName);

        LOGGER.debug("Bucket deleted!");
    }

    public S3Object getObject(String bucketName, String keyName) throws AmazonServiceException {
        LOGGER.debug("Downloading %s from S3 bucket %s...\n", keyName, bucketName);
        return getS3().getObject(bucketName, keyName);
    }

    public File getObjectFile(String bucketName, String keyName) throws AmazonServiceException, IOException {
        return getObjectFile(bucketName, keyName, createTempFile(keyName));
    }

    public File getObjectFile(String bucketName, String keyName, File targetFile) throws AmazonServiceException, IOException {
        LOGGER.debug("Downloading %s from S3 bucket %s...\n", keyName, bucketName);
        S3Object s3Object = getS3().getObject(bucketName, keyName);
        Files.copy(s3Object.getObjectContent(), targetFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        return targetFile;
    }

    public void downloadFile(String bucketName, String keyName, File targetFile) throws AmazonServiceException, InterruptedException {
        LOGGER.debug("Downloading to file: " + targetFile.getAbsolutePath());

        Download download = getTransferManager(getS3()).download(bucketName, keyName, targetFile);
        download.waitForCompletion();
    }

    public void uploadFile(String bucketName, String keyName, File file) throws AmazonServiceException, InterruptedException {
        LOGGER.debug("Uploading file: " + file.getAbsolutePath());

        Upload upload = getTransferManager(getS3()).upload(bucketName, keyName, file);
        upload.waitForCompletion();
    }

    public File createTempFile(String objectKey) throws IOException {
        String prefix = com.google.common.io.Files.getNameWithoutExtension(objectKey);
        prefix = StringUtils.rightPad(prefix, 3, '_');
        String suffix = "." + com.google.common.io.Files.getFileExtension(objectKey);
        return File.createTempFile(prefix, suffix);
    }

    public void putObject(String bucketName, File file) throws AmazonServiceException {
        putObject(bucketName, file.getName(), file);
    }

    public void putObject(String bucketName, String keyName, File file) throws AmazonServiceException {
        String filePath = file.getAbsolutePath();
        LOGGER.debug("Uploading %s to S3 bucket %s...\n", filePath, bucketName);
        getS3().putObject(bucketName, keyName, filePath);
    }

    public void copyObject(String objectKey, String fromBucket, String toBucket) throws AmazonServiceException {
        getS3().copyObject(fromBucket, objectKey, toBucket, objectKey);
    }

    public ListObjectsV2Result getObjects(String bucketName) {
        return s3.listObjectsV2(bucketName);
    }

    public ListObjectsV2Result getObjects(String bucketName, String prefix) {
        return getS3().listObjectsV2(bucketName, prefix);
    }

    public void logObjects(String bucketName) {
        ListObjectsV2Result result = getS3().listObjectsV2(bucketName);
        List<S3ObjectSummary> objects = result.getObjectSummaries();
        for (S3ObjectSummary os: objects) {
            LOGGER.info("* " + os.getKey());
        }
    }

    public void deleteObject(String bucketName, String objectKey) throws AmazonServiceException {
        getS3().deleteObject(bucketName, objectKey);
    }

    public void deleteObjects(String bucketName, String... objectKeys) throws AmazonServiceException {
        DeleteObjectsRequest dor = new DeleteObjectsRequest(bucketName).withKeys(objectKeys);
        getS3().deleteObjects(dor);
    }
}
