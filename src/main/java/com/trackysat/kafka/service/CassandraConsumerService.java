package com.trackysat.kafka.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.trackysat.kafka.domain.DeadLetterQueue;
import com.trackysat.kafka.domain.TrackysatEvent;
import com.trackysat.kafka.domain.Vmson;
import com.trackysat.kafka.repository.DeadLetterQueueRepository;
import com.trackysat.kafka.repository.TrackysatEventRepository;
import com.trackysat.kafka.service.dto.StatusDTO;
import com.trackysat.kafka.service.mapper.TrackysatEventMapper;
import com.trackysat.kafka.utils.JSONUtils;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class CassandraConsumerService {

    public static final String TRACKYSAT_TOPIC = "a-19";
    public static final String TRACKYSAT_GROUP = "trackysat";

    private final Logger log = LoggerFactory.getLogger(CassandraConsumerService.class);

    private final DeadLetterQueueRepository deadLetterQueueRepository;

    private final TrackysatEventRepository trackysatEventRepository;

    private final TrackysatEventMapper trackysatEventMapper;

    private final AtomicBoolean isEnabled = new AtomicBoolean(false);
    private final AtomicInteger eventCounter = new AtomicInteger(0);
    private final AtomicInteger errorCounter = new AtomicInteger(0);
    private Instant startDate = Instant.now();

    public CassandraConsumerService(
        DeadLetterQueueRepository deadLetterQueueRepository,
        TrackysatEventRepository trackysatEventRepository,
        TrackysatEventMapper trackysatEventMapper
    ) {
        this.deadLetterQueueRepository = deadLetterQueueRepository;
        this.trackysatEventRepository = trackysatEventRepository;
        this.trackysatEventMapper = trackysatEventMapper;
    }

    @KafkaListener(topics = TRACKYSAT_TOPIC, groupId = TRACKYSAT_GROUP, containerFactory = "kafkaTrackysatListenerContainerFactory")
    public void listenGroupTrackysat(String message) throws InterruptedException {
        if (message == null) return;
        try {
            long duration = Instant.now().getEpochSecond() - startDate.getEpochSecond();
            log.info("Received VMSON in group " + TRACKYSAT_GROUP + ", running for " + duration + "s");
            if (isEnabled.get()) {
                log.debug("Input json: {}", message);
                if (message.startsWith("<")) {
                    processError(message, "XML");
                } else {
                    processEvent(message);
                }
            }
        } catch (JsonProcessingException e) {
            log.error("Cannot parse message", e);
            processError(message, e.getMessage());
        } catch (Exception e) {
            log.error("GENERIC ERROR", e);
            processError(message, e.getMessage());
        }
    }

    private void processEvent(String message) throws JsonProcessingException {
        log.debug("processEvent input: {}", message);
        eventCounter.incrementAndGet();
        Vmson record = JSONUtils.toJson(message, Vmson.class);
        TrackysatEvent event = trackysatEventMapper.fromVmson(record);
        trackysatEventRepository.save(event);
    }

    private void processError(String msg, String errorMessage) {
        log.debug("processError input: {}", msg);
        errorCounter.incrementAndGet();
        String hash = null;
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            hash = new String(digest.digest(msg.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        if (hash == null) hash = "generic-id";
        DeadLetterQueue dlq = new DeadLetterQueue();
        dlq.setEventId(hash);
        dlq.setData(msg);
        dlq.setException(errorMessage);
        deadLetterQueueRepository.save(dlq);
    }

    public AtomicBoolean getIsEnabled() {
        return isEnabled;
    }

    public void setIsEnabled(Boolean isEnabled) {
        this.isEnabled.set(isEnabled);
    }

    public AtomicInteger getErrorCounter() {
        return errorCounter;
    }

    public Optional<StatusDTO> getStatus() {
        StatusDTO status = new StatusDTO();
        long duration = Instant.now().getEpochSecond() - startDate.getEpochSecond();
        status.setRunningTime(duration);
        status.setEnabled(isEnabled.get());
        status.setEventCounter(eventCounter.get());
        status.setErrorCounter(errorCounter.get());
        return Optional.of(status);
    }

    public boolean reset() {
        startDate = Instant.now();
        eventCounter.set(0);
        errorCounter.set(0);
        isEnabled.set(true);
        return true;
    }
}
