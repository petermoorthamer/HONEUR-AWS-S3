package com.jnj.honeur.aws.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectListing;

import java.io.IOException;


public class TempCredentialTest {

    private static String bucketName = "honeur-central";

    private static void listObjects(final AmazonS3 s3) {
        ObjectListing objects = s3.listObjects(bucketName);
        System.out.println("No. of Objects = " +
                objects.getObjectSummaries().size());
    }

    public static void main(String[] args) throws IOException {
        System.out.println("List objects with default S3 client");
        listObjects(HoneurAmazonS3ClientBuilder.defaultClient());

        System.out.println("List objects with session S3 client");
        listObjects(HoneurAmazonS3ClientBuilder.sessionClient());

        System.out.println("List objects with session S3 client v2");
        listObjects(HoneurAmazonS3ClientBuilder.sessionClient2());
    }
}

