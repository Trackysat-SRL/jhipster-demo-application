package com.trackysat.kafka.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.trackysat.kafka.config.kafka.KafkaTopicConfig;
import com.trackysat.kafka.domain.TrackyPosition;
import com.trackysat.kafka.domain.Vmson;
import com.trackysat.kafka.repository.TrackyPositionRepository;
import com.trackysat.kafka.utils.JSONUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class CassandraConsumerService {

    public static final String TRACKYSAT_TOPIC = "a-19";
    public static final String TRACKYSAT_GROUP = "trackysat";

    private final Logger log = LoggerFactory.getLogger(CassandraConsumerService.class);

    private final TrackyPositionRepository trackyPositionRepository;

    public CassandraConsumerService(TrackyPositionRepository trackyPositionRepository) {
        this.trackyPositionRepository = trackyPositionRepository;
    }

    @KafkaListener(
        topics = KafkaTopicConfig.TRACKY_TOPIC,
        groupId = KafkaTopicConfig.TRACKY_GROUP,
        containerFactory = "kafkaDemoListenerContainerFactory"
    )
    public void listenGroupExample(String message) {
        log.info("Received Message in group " + KafkaTopicConfig.TRACKY_GROUP + " msg: " + message);
        TrackyPosition t = null;
        try {
            t = JSONUtils.toJson(message, TrackyPosition.class);
            trackyPositionRepository.save(t);
        } catch (JsonProcessingException e) {
            log.error("Couldn't parse message: " + message, e.getMessage());
        }
    }

    @KafkaListener(topics = TRACKYSAT_TOPIC, groupId = TRACKYSAT_GROUP, containerFactory = "kafkaTrackysatListenerContainerFactory")
    public void listenGroupTrackysat(Vmson message) throws InterruptedException {
        try {
            log.info("Received Message in group " + KafkaTopicConfig.TRACKY_GROUP + " msg: " + JSONUtils.toString(message));
        } catch (JsonProcessingException e) {
            log.error("Cannot parse message", e);
        }
        Thread.sleep(300000);
    }
}
