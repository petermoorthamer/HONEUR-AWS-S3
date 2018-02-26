package com.jnj.honeur.aws.s3;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import java.io.File;
import java.util.List;

public class AwsTest {

    public AwsTest() {}

    public void startFileExchange() {
        BasicAWSCredentials honeurCentralCredentials = new BasicAWSCredentials("accessKey", "secretKey");
        BasicAWSCredentials honeurLocalCredentials = new BasicAWSCredentials("accessKey", "secretKey");

        AmazonS3 honeurCentralClient = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(honeurCentralCredentials))
                .build();

        AmazonS3 honeurLocalClient = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(honeurLocalCredentials))
                .build();

        File requestFile = new File("/Users/peter/Downloads/exchange_request.txt");
        File responseFile = new File("/Users/peter/Downloads/exchange_response.txt");

        String honeurOut = "honeur-out";
        String honeurIn = "honeur-in";

        try {
            System.out.println(String.format("Delete request file on %s", honeurOut));
            honeurCentralClient.deleteObject(honeurOut, requestFile.getName());
            System.out.println(String.format("Delete response file on %s", honeurIn));
            honeurLocalClient.deleteObject(honeurIn, responseFile.getName());

            System.out.println(String.format("Listing files on %s", honeurOut));
            listObjects(honeurCentralClient, honeurOut);
            System.out.println(String.format("Listing files on %s", honeurIn));
            listObjects(honeurLocalClient, honeurIn);


            System.out.println("**********************************************************");

            System.out.println(String.format("Writing %s to %s as %s", requestFile.getName(), honeurOut, "honeur-central"));
            honeurCentralClient.putObject(honeurOut, requestFile.getName(), requestFile.getAbsolutePath());
            System.out.println("Writing done");
            System.out.println(String.format("Listing files on %s", honeurOut));
            listObjects(honeurCentralClient, honeurOut);

            System.out.println(String.format("Reading from %s as %s", honeurOut, "honeur-local"));
            S3Object requestObject = honeurLocalClient.getObject("honeur-out", requestFile.getName());
            System.out.println("Reading done: " + requestObject.getKey());


            System.out.println(String.format("Writing %s to %s as %s", responseFile.getName(), honeurIn, "honeur-local"));
            honeurLocalClient.putObject(honeurIn, responseFile.getName(), responseFile.getAbsolutePath());
            System.out.println("Writing done");
            System.out.println(String.format("Listing files on %s", honeurIn));
            listObjects(honeurLocalClient, honeurIn);

            System.out.println(String.format("Reading from %s as %s", honeurIn, "honeur-central"));
            S3Object o = honeurCentralClient.getObject(honeurIn, responseFile.getName());
            System.out.println("Reading done: " + o.getKey());

        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            System.exit(1);
        }
    }

    public void listObjects(AmazonS3 s3, String bucketName) {
        ListObjectsV2Result result = s3.listObjectsV2(bucketName);
        List<S3ObjectSummary> objects = result.getObjectSummaries();
        for (S3ObjectSummary os: objects) {
            System.out.println("* " + os.getKey());
        }
    }

    public static void main(String[] args) {
        AwsTest test = new AwsTest();
        test.startFileExchange();
    }
}
