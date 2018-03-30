package com.jnj.honeur.aws.s3;

import com.amazonaws.auth.*;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.Credentials;
import com.amazonaws.services.securitytoken.model.GetSessionTokenRequest;
import com.amazonaws.services.securitytoken.model.GetSessionTokenResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HONEUR utility builder for AmazonS3
 * @author Peter Moorthamer
 */
public class HoneurAmazonS3ClientBuilder {

    private static final Logger LOGGER = LoggerFactory.getLogger(HoneurAmazonS3ClientBuilder.class);

    private static final Regions DEFAULT_REGION = Regions.EU_WEST_1;

    private static AWSSecurityTokenService tokenService = AWSSecurityTokenServiceClientBuilder.defaultClient();

    public static AmazonS3 defaultClient() {
        return AmazonS3ClientBuilder.defaultClient();
    }

    public static AmazonS3 standardClient(final AWSCredentials credentials) {
        return AmazonS3ClientBuilder.standard()
                .withForceGlobalBucketAccessEnabled(true)
                .withRegion(DEFAULT_REGION)
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .build();
    }

    public static AmazonS3 sessionClient() {
        return sessionClient(HoneurAmazonS3ClientBuilder.tokenService);
    }

    public static AmazonS3 sessionClient(final AWSSecurityTokenService tokenService) {
        return sessionClient(new STSSessionCredentialsProvider(tokenService));
    }

    public static AmazonS3 sessionClient(final AWSSessionCredentialsProvider credentialsProvider) {
        return AmazonS3ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .build();
    }

    public static AmazonS3 sessionClient2() {
        // Start a session.
        GetSessionTokenRequest getSessionTokenRequest = new GetSessionTokenRequest();
        //getSessionTokenRequest.setDurationSeconds(900); // 43200 seconds (12 hours) is used by default

        GetSessionTokenResult sessionTokenResult = tokenService.getSessionToken(getSessionTokenRequest);
        Credentials sessionCredentials = sessionTokenResult.getCredentials();
        LOGGER.debug("Session Credentials: " + sessionCredentials.toString());

        // Package the session credentials as a BasicSessionCredentials
        // object for an S3 client object to use.
        BasicSessionCredentials basicSessionCredentials =
                new BasicSessionCredentials(sessionCredentials.getAccessKeyId(),
                        sessionCredentials.getSecretAccessKey(),
                        sessionCredentials.getSessionToken());

        return AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(basicSessionCredentials))
                .build();
    }

}
