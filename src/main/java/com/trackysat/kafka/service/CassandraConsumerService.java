package com.trackysat.kafka.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.trackysat.kafka.domain.DeadLetterQueue;
import com.trackysat.kafka.domain.TrackyEvent;
import com.trackysat.kafka.domain.Vmson;
import com.trackysat.kafka.repository.DeadLetterQueueRepository;
import com.trackysat.kafka.repository.DeviceRepository;
import com.trackysat.kafka.repository.TrackyEventRepository;
import com.trackysat.kafka.service.dto.StatusDTO;
import com.trackysat.kafka.service.mapper.DeviceMapper;
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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class CassandraConsumerService {

    public static final String TRACKYSAT_GROUP = "trackysat";
    public static final String TRACKYSAT_TOPIC = "a-19";

    @Value(value = "${kafka.consumer.number}")
    private final String CONSUMER_NUMBER = "3";

    private final DeadLetterQueueRepository deadLetterQueueRepository;
    private final Logger log = LoggerFactory.getLogger(CassandraConsumerService.class);

    private final TrackyEventRepository trackyEventRepository;

    private final TrackysatEventMapper trackysatEventMapper;

    private final DeviceMapper deviceMapper;

    private final DeviceRepository deviceRepository;

    private final AtomicBoolean isEnabled = new AtomicBoolean(false);
    private final AtomicInteger eventCounter = new AtomicInteger(0);
    private final AtomicInteger errorCounter = new AtomicInteger(0);
    private Instant startDate = Instant.now();

    public CassandraConsumerService(
        DeadLetterQueueRepository deadLetterQueueRepository,
        TrackyEventRepository trackyEventRepository,
        TrackysatEventMapper trackysatEventMapper,
        DeviceMapper deviceMapper,
        DeviceRepository deviceRepository
    ) {
        this.deadLetterQueueRepository = deadLetterQueueRepository;
        this.trackyEventRepository = trackyEventRepository;
        this.trackysatEventMapper = trackysatEventMapper;
        this.deviceMapper = deviceMapper;
        this.deviceRepository = deviceRepository;
    }

    @KafkaListener(
        topics = TRACKYSAT_TOPIC,
        groupId = TRACKYSAT_GROUP,
        containerFactory = "kafkaTrackysatListenerContainerFactory",
        concurrency = CONSUMER_NUMBER
    )
    public void listenGroupTrackysat(String message) throws InterruptedException {
        log.debug("Received VMSON in group " + TRACKYSAT_GROUP + " msg: " + message);
        if (message == null) return;
        try {
            processEvent(message);
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
        //        eventCounter.incrementAndGet();
        //        if (isEnabled.get()) {
        Vmson record = JSONUtils.toJson(message, Vmson.class);
        TrackyEvent event = trackysatEventMapper.fromVmson(record);
        trackyEventRepository.save(event);
        deviceRepository.save(deviceMapper.fromVmson(record));
        //        }
    }

    private void processError(String msg, String errorMessage) {
        log.debug("processError input: {}", msg);
        //        errorCounter.incrementAndGet();
        //        if (isEnabled.get()) {
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
        //        }
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
