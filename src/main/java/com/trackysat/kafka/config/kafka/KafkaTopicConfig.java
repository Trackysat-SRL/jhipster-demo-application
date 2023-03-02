package com.trackysat.kafka.config.kafka;

import com.trackysat.kafka.service.CassandraConsumerService;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

@Configuration
public class KafkaTopicConfig {

    @Value(value = "${kafka.bootstrap.server}")
    private String bootstrapAddress;

    @Value(value = "${kafka.consumer.number}")
    private final String CONSUMER_NUMBER = "10";

    @Value(value = "${kafka.producer.enabled}")
    private boolean enabled;

    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        return new KafkaAdmin(configs);
    }

    @Bean
    public NewTopic trackysatTopic() {
        if (!enabled) return null;
        return new NewTopic(CassandraConsumerService.TRACKYSAT_TOPIC, Integer.parseInt(CONSUMER_NUMBER), (short) 1);
    }
}
