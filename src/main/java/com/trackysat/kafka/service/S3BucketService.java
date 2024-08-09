package com.trackysat.kafka.service;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.trackysat.kafka.config.ApplicationProperties;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.OutputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class S3BucketService {

    private static final Logger log = LoggerFactory.getLogger(S3BucketService.class);
    private final AmazonS3 s3Client;
    private final Configuration configuration;
    private final String bucketName;
    private final String dirBucket;

    public S3BucketService(AmazonS3 s3Client, Configuration configuration, ApplicationProperties applicationProperties) {
        this.s3Client = s3Client;
        this.configuration = configuration;
        this.bucketName = applicationProperties.getAwsProperties().getBucketName();
        this.dirBucket = applicationProperties.getAwsProperties().getDirBucket();
    }

    public void uploadFile(String path, File file) {
        s3Client.putObject(bucketName, dirBucket + "/" + path, file);
        // return s3Client.getUrl(bucketName, dirBucket + "/" + path).getPath();
    }

    public String uploadInputStream(String path, InputStream data, ObjectMetadata metadata) {
        s3Client.putObject(bucketName, dirBucket + "/" + path, data, metadata);
        return s3Client.getUrl(bucketName, dirBucket + "/" + path).getPath();
    }

    public void deleteFileFromS3Bucket(String path) {
        s3Client.deleteObject(new DeleteObjectRequest(bucketName, formatPath(path)));
    }

    public S3ObjectInputStream getFileFroms3bucket(String fileName) {
        return s3Client.getObject(bucketName, formatPath(fileName)).getObjectContent();
    }

    public <T> List<T> readFromS3ParquetFile(String path) throws IllegalAccessException {
        if (path == null) throw new IllegalAccessException("The path of file cannot be null!");

        List<T> records = new ArrayList<>();
        try (
            var reader = AvroParquetReader
                .<T>builder(HadoopInputFile.fromPath(new Path(formatPath(path)), configuration))
                .withConf(configuration)
                .build()
        ) {
            T record;
            while ((record = reader.read()) != null) {
                records.add(record);
            }
        } catch (IOException ex) {
            log.error("Unable to read to specified path: {}", ex.getMessage());
        }

        return records;
    }

    public <T> void writeToS3ParquetFile(T obj, String path, Function<T, GenericData.Record> recordSupplier) throws IllegalAccessException {
        if (obj == null) throw new IllegalAccessException("The object to write cannot be null!");
        if (path == null) throw new IllegalAccessException("The path of file cannot be null!");
        if (recordSupplier == null) throw new IllegalAccessException("The schema supplier cannot be null!");

        /* if (!fileExistsInS3(path)) {
            log.debug("The specified file does not exist, creating a new parquet file '{}.parquet'", path);
            var fileName = path.substring(path.lastIndexOf('/') + 1);
            uploadFile(path, new File("report.txt"));//deve essere un file esistente
        }*/

        var record = recordSupplier.apply(obj);

        try (
            var parquetWriter = AvroParquetWriter
                .<GenericData.Record>builder(new Path("s3a://" + bucketName + "/" + path))
                .withConf(configuration)
                .withSchema(record.getSchema())
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .build()
        ) {
            parquetWriter.write(record);
        } catch (IOException ex) {
            log.error("Unable to write to specified path: {}", ex.getMessage());
        }
    }

    private boolean fileExistsInS3(String path) {
        return s3Client.doesObjectExist(bucketName, dirBucket + "/" + formatPath(path));
    }

    private String formatPath(String path) {
        return path.startsWith("/") ? path.replaceFirst("/", "") : path;
    }
}
