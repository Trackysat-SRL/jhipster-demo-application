package com.trackysat.kafka.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Properties specific to Demo Kafka J Hipster.
 * <p>
 * Properties are configured in the {@code application.yml} file.
 * See {@link tech.jhipster.config.JHipsterProperties} for a good example.
 */
@ConfigurationProperties(prefix = "application", ignoreUnknownFields = false)
public class ApplicationProperties {

    // jhipster-needle-application-properties-property
    // jhipster-needle-application-properties-property-getter
    // jhipster-needle-application-properties-property-class

    private AwsProperties awsProperties;

    public AwsProperties getAwsProperties() {
        return awsProperties;
    }

    public void setAwsProperties(AwsProperties awsProperties) {
        this.awsProperties = awsProperties;
    }

    public static class AwsProperties {

        private String accessKey;
        private String secretKey;
        private String bucketName;
        private String dirBucket;

        public String getAccessKey() {
            return accessKey;
        }

        public String getSecretKey() {
            return secretKey;
        }

        public String getBucketName() {
            return bucketName;
        }

        public String getDirBucket() {
            return dirBucket;
        }

        public void setAccessKey(String accessKey) {
            this.accessKey = accessKey;
        }

        public void setSecretKey(String secretKey) {
            this.secretKey = secretKey;
        }

        public void setBucketName(String bucketName) {
            this.bucketName = bucketName;
        }

        public void setDirBucket(String dirBucket) {
            this.dirBucket = dirBucket;
        }
    }
}
