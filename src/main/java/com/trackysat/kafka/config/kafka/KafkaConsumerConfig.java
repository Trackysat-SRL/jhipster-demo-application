package com.trackysat.kafka.config.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.trackysat.kafka.domain.Vmson;
import com.trackysat.kafka.exeptions.ErrorHandlingDeserializerSupport;
import com.trackysat.kafka.repository.DeadLetterQueueRepository;
import com.trackysat.kafka.service.CassandraConsumerService;
import com.trackysat.kafka.utils.CustomJsonDeserializer;
import com.trackysat.kafka.utils.JSONUtils;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

@EnableKafka
@Configuration
public class KafkaConsumerConfig {

    @Value(value = "${kafka.consumer.enabled}")
    private String CONSUMER_ENABLED;

    @Value(value = "${kafka.bootstrap.server}")
    private List<String> BOOTSTRAP_SERVER;

    @Value(value = "${kafka.security.protocol}")
    private String SECURITY_PROTOCOL;

    @Value(value = "${kafka.sasl.mechanism}")
    private String SASL_MECHANISM;

    @Value(value = "${kafka.sasl.jaas.config}")
    private String SASL_JAAS_CONFIG;

    @Value(value = "${kafka.topic.enrichment}")
    private String TOPIC_ENRICHMENT;

    @Value(value = "${kafka.consumer.client.id}")
    private String CONSUMER_CLIENT_ID;

    @Value(value = "${kafka.consumer.group.id}")
    private String CONSUMER_GROUP_ID;

    @Value(value = "${kafka.consumer.number}")
    private Integer CONSUMER_NUMBER;

    @Value(value = "${kafka.consumer.auto.offset.reset}")
    private String CONSUMER_AUTO_OFFSET_RESET;

    @Value(value = "${kafka.consumer.fetch.min.bytes}")
    private String CONSUMER_FETCH_MIN_BYTES;

    @Value(value = "${kafka.consumer.max.partition.fetch.bytes}")
    private String CONSUMER_MAX_PARTITION_FETCH_BYTES;

    @Value(value = "${kafka.consumer.fetch.max.wait.ms}")
    private String CONSUMER_FETCH_MAX_WAIT_MS;

    @Value(value = "${kafka.consumer.session.timeout.ms}")
    private String CONSUMER_SESSION_TIMEOUT_MS;

    @Value(value = "${kafka.consumer.max.poll.records}")
    private String CONSUMER_MAX_POLL_RECORDS;

    @Value(value = "${kafka.consumer.max.poll.interval.ms}")
    private String CONSUMER_MAX_POLL_INTERVAL_MS;

    @Value(value = "${kafka.consumer.enable.auto.commit}")
    private String CONSUMER_ENABLE_AUTO_COMMIT;

    @Value(value = "${kafka.consumer.backoff.max.attempts}")
    private Long CONSUMER_BACKOFF_MAX_ATTEMPTS;

    @Value(value = "${kafka.consumer.backoff.interval}")
    private Long CONSUMER_BACKOFF_INTERVAL;

    @Value(value = "${kafka.consumer.threadPool.corePoolSize}")
    private String CONSUMER_THREADPOOL_COREPOOLSIZE;

    @Value(value = "${kafka.consumer.threadPool.maxPoolSize}")
    private String CONSUMER_THREADPOOL_MAXPOOLSIZE;

    @Value(value = "${kafka.consumer.threadPool.queueCapacity}")
    private String CONSUMER_THREADPOOL_QUEUECAPACITY;

    @Value(value = "${kafka.consumer.threadPool.awaitTerminationSeconds}")
    private String CONSUMER_THREADPOOL_AWAITTERMINATIONSECONDS;

    // TODO Dead Letter Queue Producer
    //    @Autowired()
    //    @Qualifier("kafkaTemplate")
    //    private KafkaOperations<String, Object> kafkaTemplate;

    @Autowired
    private DeadLetterQueueRepository deadLetterQueueRepository;

    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_ID);
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SECURITY_PROTOCOL);
        props.put(SaslConfigs.SASL_MECHANISM, SASL_MECHANISM);
        props.put(SaslConfigs.SASL_JAAS_CONFIG, SASL_JAAS_CONFIG);
        //        props.put(ConsumerConfig.TOPIC_ENRICHMENT, TOPIC_ENRICHMENT);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, CONSUMER_CLIENT_ID);
        //        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_ID);
        //        props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, CONSUMER_NUMBER);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, CONSUMER_AUTO_OFFSET_RESET);
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, CONSUMER_FETCH_MIN_BYTES);
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, CONSUMER_MAX_PARTITION_FETCH_BYTES);
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, CONSUMER_FETCH_MAX_WAIT_MS);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, CONSUMER_SESSION_TIMEOUT_MS);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, CONSUMER_MAX_POLL_RECORDS);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, CONSUMER_MAX_POLL_INTERVAL_MS);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, CONSUMER_ENABLE_AUTO_COMMIT);
        //        props.put(ConsumerConfig.THREADPOOL_COREPOOLSIZE_CONFIG, CONSUMER_THREADPOOL_COREPOOLSIZE);
        //        props.put(ConsumerConfig.THREADPOOL_MAXPOOLSIZE_CONFIG, CONSUMER_THREADPOOL_MAXPOOLSIZE);
        //        props.put(ConsumerConfig.THREADPOOL_QUEUECAPACITY_CONFIG, CONSUMER_THREADPOOL_QUEUECAPACITY);
        //        props.put(ConsumerConfig.THREADPOOL_AWAITTERMINATIONSECONDS_CONFIG, CONSUMER_THREADPOOL_AWAITTERMINATIONSECONDS);
        //        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        //        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), valueDeserializer());
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaTrackysatListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());

        factory.setConcurrency(CONSUMER_NUMBER);
        factory.setBatchListener(false);

        // TODO Dead Letter Queue, vogliamo?
        //                BackOff backoff = new FixedBackOff(CONSUMER_BACKOFF_INTERVAL, CONSUMER_BACKOFF_MAX_ATTEMPTS);
        //                DefaultErrorHandler error = new DefaultErrorHandler(
        //                    new CustomDeadLetterPublishingRecoverer(kafkaTemplate), backoff);
        //                factory.setCommonErrorHandler(error);

        //        factory.setRecordFilterStrategy(new CustomRecordFilterStrategy());
        return factory;
    }

    private ErrorHandlingDeserializer<Object> valueDeserializer() {
        ObjectMapper om = JSONUtils.buildObjectMapper();
        CustomJsonDeserializer<Object> j = new CustomJsonDeserializer<>(om);
        Map<String, Object> configs = new HashMap<>();
        configs.put(JsonDeserializer.VALUE_DEFAULT_TYPE, Vmson.class);
        j.configure(configs, false);
        j.addTrustedPackages("*");
        ErrorHandlingDeserializer<Object> ehd = new ErrorHandlingDeserializer<>(j);
        ehd.setFailedDeserializationFunction(new ErrorHandlingDeserializerSupport<Object>().supply(deadLetterQueueRepository));
        return ehd;
    }
}
