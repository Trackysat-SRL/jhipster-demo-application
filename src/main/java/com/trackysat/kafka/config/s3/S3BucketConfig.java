package com.trackysat.kafka.config.s3;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.trackysat.kafka.config.ApplicationProperties;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class S3BucketConfig {

    private static final Logger log = LoggerFactory.getLogger(S3BucketConfig.class);

    @Bean
    public AmazonS3 s3Client(ApplicationProperties applicationProperties) {
        log.info("Initialize aws");
        var credentials = new BasicAWSCredentials(
            applicationProperties.getAwsProperties().getAccessKey(),
            applicationProperties.getAwsProperties().getSecretKey()
        );
        var s3client = AmazonS3ClientBuilder
            .standard()
            .withRegion(Regions.EU_CENTRAL_1)
            .withCredentials(new AWSStaticCredentialsProvider(credentials))
            .build();
        log.info("End initialize aws");
        return s3client;
    }

    @Bean
    public org.apache.hadoop.conf.Configuration parquetConfiguration(ApplicationProperties applicationProperties) {
        if (System.getProperty("os.name").startsWith("Windows")) {
            System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
            System.load("C:\\hadoop\\bin\\hadoop.dll");
        }
        var conf = new org.apache.hadoop.conf.Configuration();
        conf.set("fs.s3a.access.key", applicationProperties.getAwsProperties().getAccessKey());
        conf.set("fs.s3a.secret.key", applicationProperties.getAwsProperties().getSecretKey());
        conf.set("fs.s3a.endpoint", "s3.eu-central-1.amazonaws.com");
        conf.set("fs.s3a.impl", S3AFileSystem.class.getName());
        return conf;
    }
}
