# Connect to Cassandra Keyspaces

# Create keyspace fleettrack_test and tables

Using AWS Keyspaces we can create the keyspace and CQL Editor to create all tables described in src/main/resources/config/cql/*.cql

# Create user fleettrack-cass in IAM

First create a new user with Programmatic Access and add the Role "AmazonKeyspacesFullAccess". And then go to the user site and click on the Tab "Security Credentials", to generate "Credentials for Amazon Keyspaces (for Apache Cassandra)". 
Copy username/password from "Credentials for Amazon Keyspaces (for Apache Cassandra)" and use it in the Java app.

# Download TLS from AWS
From [AWS Connect to Keyspaces from Java](https://docs.aws.amazon.com/keyspaces/latest/devguide/using_java_driver.html)

## Download and convert
```
cd src/main/resources/config/tls
curl https://certs.secureserver.net/repository/sf-class2-root.crt -O
openssl x509 -outform der -in sf-class2-root.crt -out temp_file.der
keytool -import -alias cassandra -keystore cassandra_truststore.jks -file temp_file.der
```

## Attach the trustStore file in the JVM arguments:
```
-Djavax.net.ssl.trustStore=src/main/resources/config/tls/cassandra_truststore.jks
-Djavax.net.ssl.trustStorePassword=trackyf00d
```
Or add to data.cassandra config and to application-tls config.
