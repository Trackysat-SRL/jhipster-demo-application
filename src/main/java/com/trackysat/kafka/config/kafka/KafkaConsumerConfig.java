package com.trackysat.kafka.config.kafka;

import com.trackysat.kafka.repository.DeadLetterQueueRepository;
import java.util.*;
import javax.annotation.PreDestroy;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@EnableKafka
@Configuration
public class KafkaConsumerConfig {

    private final Logger log = LoggerFactory.getLogger(KafkaConsumerConfig.class);

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
    private Integer CONSUMER_THREADPOOL_COREPOOLSIZE;

    @Value(value = "${kafka.consumer.threadPool.maxPoolSize}")
    private Integer CONSUMER_THREADPOOL_MAXPOOLSIZE;

    @Value(value = "${kafka.consumer.threadPool.queueCapacity}")
    private Integer CONSUMER_THREADPOOL_QUEUECAPACITY;

    @Value(value = "${kafka.consumer.threadPool.awaitTerminationSeconds}")
    private Integer CONSUMER_THREADPOOL_AWAITTERMINATIONSECONDS;

    @Autowired
    Environment env;

    // TODO Dead Letter Queue Producer
    //    @Autowired()
    //    @Qualifier("kafkaTemplate")
    //    private KafkaOperations<String, Object> kafkaTemplate;

    @Autowired
    private DeadLetterQueueRepository deadLetterQueueRepository;

    @Bean
    ThreadPoolTaskExecutor consumerProcessorExecutor() {
        log.info("Creating a consumerProcessorExecutor pool with {} threads", CONSUMER_THREADPOOL_COREPOOLSIZE);
        ThreadPoolTaskExecutor exec = new ThreadPoolTaskExecutor();
        exec.setCorePoolSize(CONSUMER_THREADPOOL_COREPOOLSIZE);
        exec.setMaxPoolSize(CONSUMER_THREADPOOL_MAXPOOLSIZE);
        exec.setQueueCapacity(CONSUMER_THREADPOOL_QUEUECAPACITY);
        exec.setAwaitTerminationSeconds(CONSUMER_THREADPOOL_AWAITTERMINATIONSECONDS);
        exec.setAllowCoreThreadTimeOut(true);
        exec.setThreadNamePrefix("consumer-");
        return exec;
    }

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_ID);
        if (Arrays.stream(env.getActiveProfiles()).noneMatch(p -> Objects.equals(p, "dev"))) {
            props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SECURITY_PROTOCOL);
            props.put(SaslConfigs.SASL_MECHANISM, SASL_MECHANISM);
            props.put(SaslConfigs.SASL_JAAS_CONFIG, SASL_JAAS_CONFIG);
        }
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, CONSUMER_CLIENT_ID);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, CONSUMER_AUTO_OFFSET_RESET);
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, CONSUMER_FETCH_MIN_BYTES);
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, CONSUMER_MAX_PARTITION_FETCH_BYTES);
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, CONSUMER_FETCH_MAX_WAIT_MS);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, CONSUMER_SESSION_TIMEOUT_MS);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, CONSUMER_MAX_POLL_RECORDS);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, CONSUMER_MAX_POLL_INTERVAL_MS);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, CONSUMER_ENABLE_AUTO_COMMIT);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        //        props.put(ConsumerConfig.TOPIC_ENRICHMENT, TOPIC_ENRICHMENT);
        //        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_ID);
        return new DefaultKafkaConsumerFactory<String, String>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaTrackysatListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());

        factory.setConcurrency(CONSUMER_NUMBER);
        factory.setBatchListener(false);

        factory.setRecordFilterStrategy(new CustomRecordFilterStrategy());

        factory.getContainerProperties().setConsumerTaskExecutor(consumerProcessorExecutor());
        return factory;
    }

    @PreDestroy
    public void destroy() {
        log.info("Shutdown consumer id: {}, group: {}", CONSUMER_CLIENT_ID, CONSUMER_GROUP_ID);
    }
}
