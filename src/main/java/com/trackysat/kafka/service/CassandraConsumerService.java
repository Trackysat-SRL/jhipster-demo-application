package com.trackysat.kafka.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.trackysat.kafka.domain.TrackysatEvent;
import com.trackysat.kafka.domain.Vmson;
import com.trackysat.kafka.repository.TrackysatEventRepository;
import com.trackysat.kafka.service.dto.StatusDTO;
import com.trackysat.kafka.service.mapper.TrackysatEventMapper;
import com.trackysat.kafka.utils.JSONUtils;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import jnr.ffi.annotations.In;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class CassandraConsumerService {

    public static final String TRACKYSAT_TOPIC = "a-19";
    public static final String TRACKYSAT_GROUP = "trackysat";

    private final Logger log = LoggerFactory.getLogger(CassandraConsumerService.class);

    private final TrackysatEventRepository trackysatEventRepository;

    private final TrackysatEventMapper trackysatEventMapper;

    private final AtomicBoolean isEnabled = new AtomicBoolean(false);
    private final AtomicInteger eventCounter = new AtomicInteger(0);
    private final AtomicInteger errorCounter = new AtomicInteger(0);
    private Instant startDate = Instant.now();

    public CassandraConsumerService(TrackysatEventRepository trackysatEventRepository, TrackysatEventMapper trackysatEventMapper) {
        this.trackysatEventRepository = trackysatEventRepository;
        this.trackysatEventMapper = trackysatEventMapper;
    }

    @KafkaListener(topics = TRACKYSAT_TOPIC, groupId = TRACKYSAT_GROUP, containerFactory = "kafkaTrackysatListenerContainerFactory")
    public void listenGroupTrackysat(Vmson message) throws InterruptedException {
        try {
            int counter = eventCounter.incrementAndGet();
            long duration = Instant.now().getEpochSecond() - startDate.getEpochSecond();
            log.info("Received VMSON in group " + TRACKYSAT_GROUP + " counter: " + counter + ", running for " + duration + "s");
            if (isEnabled.get()) {
                if (message != null) {
                    //                    log.info("Received Message in group " + TRACKYSAT_GROUP + " msg: " + JSONUtils.toString(message));
                    TrackysatEvent event = trackysatEventMapper.fromVmson(message);
                    //                    log.info("Saving event: " + JSONUtils.toString(event));
                    trackysatEventRepository.save(event);
                }
            }
        } catch (JsonProcessingException e) {
            log.error("Cannot parse message", e);
        } catch (Exception e) {
            log.error("GENERIC ERROR", e);
        }
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
