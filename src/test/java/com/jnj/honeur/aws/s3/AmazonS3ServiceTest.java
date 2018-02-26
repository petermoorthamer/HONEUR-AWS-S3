package com.jnj.honeur.aws.s3;

import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class AmazonS3ServiceTest {

    private static final String TEST_BUCKET = "aws-s3.test.bucket";
    private static final String REGION = Region.EU_Ireland.getFirstRegionId();

    private AmazonS3Service s3Service = new AmazonS3Service(AmazonS3ClientBuilder.standard().withRegion("eu-west-1").build());
    private UUID uuid;

    @BeforeEach
    public void before() {
        s3Service.createBucket(TEST_BUCKET, REGION);
        this.uuid = UUID.randomUUID();
    }

    @AfterEach
    public void after() {
        s3Service.deleteBucket(TEST_BUCKET);
    }

    @Test
    void createTempFile() throws IOException {
        File tmpFile = s3Service.createTempFile("S2.png");
        assertTrue(tmpFile.getName().contains("S2_"));
        assertTrue(tmpFile.getName().endsWith(".png"));

        tmpFile = s3Service.createTempFile("S22.png");
        assertTrue(tmpFile.getName().contains("S22"));
        assertTrue(tmpFile.getName().endsWith(".png"));
    }

    @Test
    void getBucket() {
        Bucket bucket = s3Service.getBucket(TEST_BUCKET);
        assertNotNull(bucket);
        assertEquals(TEST_BUCKET, bucket.getName());
    }

    @Test
    void getBucketNotExisting() {
        Bucket bucket = s3Service.getBucket(UUID.randomUUID().toString());
        assertNull(bucket);
    }

    @Test
    void getAllBuckets() {
        List<Bucket> buckets = s3Service.getAllBuckets();
        assertNotNull(buckets);
        assertTrue(buckets.size() >= 1);
    }

    @Test
    void logAllBuckets() {
        s3Service.logAllBuckets();
    }

    @Test
    void createBucket() {
        Bucket bucket = s3Service.createBucket(uuid.toString(), REGION);
        assertNotNull(bucket);
        assertEquals(uuid.toString(), bucket.getName());
        s3Service.deleteBucket(uuid.toString());
    }

    @Test
    void deleteBucket() {
        Bucket bucket = s3Service.createBucket(uuid.toString(), REGION);
        assertNotNull(bucket);
        s3Service.deleteBucket(uuid.toString());
        Bucket deletedBucket = s3Service.getBucket(uuid.toString());
        assertNull(deletedBucket);
    }

    @Test
    void putGetObject() throws IOException {
        File tmpFile = createTmpFile("test", "test");
        s3Service.putObject(TEST_BUCKET, tmpFile);
        S3Object object = s3Service.getObject(TEST_BUCKET, tmpFile.getName());
        assertNotNull(object);
        assertEquals(tmpFile.getName(), object.getKey());
    }

    @Test
    void putCopyGetObject() throws IOException {
        // Prepare
        File tmpFile = createTmpFile("test", "test");
        s3Service.putObject(TEST_BUCKET, tmpFile);

        Bucket bucket = s3Service.createBucket(uuid.toString(), REGION);
        s3Service.copyObject(tmpFile.getName(), TEST_BUCKET, uuid.toString());

        // Copy
        S3Object copiedObject = s3Service.getObject(uuid.toString(), tmpFile.getName());
        assertNotNull(copiedObject);
        assertEquals(tmpFile.getName(), copiedObject.getKey());

        // Cleanup
        s3Service.deleteBucket(uuid.toString());
    }

    @Test
    void getObjects() throws IOException {
        // Prepare
        File tmpFile = createTmpFile("test", "test");
        s3Service.putObject(TEST_BUCKET, tmpFile);

        // Log
        s3Service.logObjects(TEST_BUCKET);

        ListObjectsV2Result result = s3Service.getObjects(TEST_BUCKET);
        List<S3ObjectSummary> summaryList = result.getObjectSummaries();
        boolean found = false;
        for(S3ObjectSummary summary:summaryList) {
            if(tmpFile.getName().equals(summary.getKey())) {
                found = true;
                break;
            }
        }
        assertTrue(found);
    }

    @Test
    void deleteObject() throws IOException {
        // Create test data
        File tmpFile = createTmpFile("test", "test");
        s3Service.putObject(TEST_BUCKET, tmpFile);
        // Check test data creation
        S3Object object = s3Service.getObject(TEST_BUCKET, tmpFile.getName());
        assertNotNull(object);
        // Delete
        s3Service.deleteObject(TEST_BUCKET, tmpFile.getName());
        // Check successful delete
        try {
            object = s3Service.getObject(TEST_BUCKET, tmpFile.getName());
        } catch (AmazonS3Exception e) {
            assertTrue(e.getMessage().contains("The specified key does not exist"));
        }
    }

    @Test
    void deleteObjects() throws IOException {
        // Create test data
        File tmpFile1 = createTmpFile("test1", "test");
        s3Service.putObject(TEST_BUCKET, tmpFile1);
        File tmpFile2 = createTmpFile("test2", "test");
        s3Service.putObject(TEST_BUCKET, tmpFile2);
        // Check test data creation
        ListObjectsV2Result result = s3Service.getObjects(TEST_BUCKET);
        assertEquals(2, result.getObjectSummaries().size());
        // Delete
        s3Service.deleteObjects(TEST_BUCKET, tmpFile1.getName(), tmpFile2.getName());
        // Check successful delete
        result = s3Service.getObjects(TEST_BUCKET);
        assertTrue(result.getObjectSummaries().isEmpty());
    }

    private File createTmpFile(String fileName, String fileContent) throws IOException {
        File tmpFile = File.createTempFile(fileName, ".tmp");
        FileWriter writer = new FileWriter(tmpFile);
        writer.write(fileContent);
        writer.close();
        return tmpFile;
    }
}