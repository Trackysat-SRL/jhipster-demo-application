package com.trackysat.kafka.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.trackysat.kafka.domain.*;
import com.trackysat.kafka.domain.vmson.VmsonCon;
import com.trackysat.kafka.repository.*;
import com.trackysat.kafka.service.dto.StatusDTO;
import com.trackysat.kafka.service.mapper.*;
import com.trackysat.kafka.utils.JSONUtils;
import com.trackysat.kafka.utils.cache.AbstractCache;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.Metric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

@Service
public class CassandraConsumerService {

    //public static final String TRACKYSAT_GROUP = "trackysat";
    public static final String TRACKYSAT_GROUP = "prod-trackysat-consumer-group";
    // public static final String TRACKYSAT_TOPIC = "a-19";
    public static final String TRACKYSAT_TOPIC = "partner-trackysat";

    @Value(value = "${kafka.consumer.number}")
    private final String CONSUMER_NUMBER = "3";

    private final DeadLetterQueueRepository deadLetterQueueRepository;
    private final Logger log = LoggerFactory.getLogger(CassandraConsumerService.class);

    private final TrackyEventRepository trackyEventRepository;

    private final TrackysatEventMapper trackysatEventMapper;

    private final DeviceMapper deviceMapper;

    private final DeviceRepository deviceRepository;

    private final LastGpsPositionMapper lastGpsPositionMapper;

    private final LastGpsPositionRepository lastGpsPositionRepository;

    private final AbstractCache<String, LastGpsPosition> lastGpsPositionAbstractCache;

    private final LastTellTaleInfoRepository lastTellTaleInfoRepository;

    private final LastTellTaleInfoMapper lastTellTaleInfoMapper;

    private final TellTaleInfoRepository tellTaleInfoRepository;

    private final TellTaleInfoMapper tellTaleInfoMapper;

    private final AtomicBoolean isEnabled = new AtomicBoolean(true);
    private final AtomicInteger eventCounter = new AtomicInteger(0);
    private final AtomicInteger errorCounter = new AtomicInteger(0);
    private Map<Integer, String> partitionsLag = new HashMap<Integer, String>();
    private Instant startDate = Instant.now();

    public CassandraConsumerService(
        DeadLetterQueueRepository deadLetterQueueRepository,
        TrackyEventRepository trackyEventRepository,
        TrackysatEventMapper trackysatEventMapper,
        DeviceMapper deviceMapper,
        DeviceRepository deviceRepository,
        LastGpsPositionMapper lastGpsPositionMapper,
        LastGpsPositionRepository lastGpsPositionRepository,
        AbstractCache<String, LastGpsPosition> lastGpsPositionAbstractCache,
        LastTellTaleInfoRepository lastTellTaleInfoRepository,
        LastTellTaleInfoMapper lastTellTaleInfoMapper,
        TellTaleInfoRepository tellTaleInfoRepository,
        TellTaleInfoMapper tellTaleInfoMapper
    ) {
        this.deadLetterQueueRepository = deadLetterQueueRepository;
        this.trackyEventRepository = trackyEventRepository;
        this.trackysatEventMapper = trackysatEventMapper;
        this.deviceMapper = deviceMapper;
        this.deviceRepository = deviceRepository;
        this.lastGpsPositionMapper = lastGpsPositionMapper;
        this.lastGpsPositionRepository = lastGpsPositionRepository;
        this.lastGpsPositionAbstractCache = lastGpsPositionAbstractCache;
        this.lastTellTaleInfoRepository = lastTellTaleInfoRepository;
        this.lastTellTaleInfoMapper = lastTellTaleInfoMapper;
        this.tellTaleInfoRepository = tellTaleInfoRepository;
        this.tellTaleInfoMapper = tellTaleInfoMapper;
    }

    @KafkaListener(
        topics = TRACKYSAT_TOPIC,
        groupId = TRACKYSAT_GROUP,
        containerFactory = "kafkaTrackysatListenerContainerFactory",
        concurrency = CONSUMER_NUMBER
    )
    public void listenGroupTrackysat(
        String message,
        @Header(KafkaHeaders.RECEIVED_PARTITION_ID) Integer partition,
        @Header(KafkaHeaders.OFFSET) Long offset,
        Consumer<String, String> consumer
    ) throws InterruptedException {
        String lag = consumer
            .metrics()
            .values()
            .stream()
            .filter(m -> "records-lag-max".equals(m.metricName().name()))
            .map(Metric::metricValue)
            .map(Object::toString)
            .distinct()
            .collect(Collectors.joining(""));
        log.debug("Read VMSON group {} | partition: {} | lag: {} | offset: {}", TRACKYSAT_GROUP, partition, lag, offset);
        partitionsLag.put(partition, lag);
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

    public void processEvent(String message) throws JsonProcessingException {
        eventCounter.incrementAndGet();
        if (isEnabled.get()) {
            Vmson record = JSONUtils.toJson(message, Vmson.class);
            TrackyEvent event = trackysatEventMapper.fromVmson(record);
            trackyEventRepository.save(event);
            deviceRepository.save(deviceMapper.fromVmson(record));
            log.debug("Event saved. device: {} date: {}", event.getDeviceId(), event.getCreatedDate());

            /* cache insert of the last position received */
            var position = lastGpsPositionMapper.fromVmson(record);
            if (position == null) return;

            var isRecent = new AtomicBoolean(true);
            lastGpsPositionAbstractCache.putIf(
                position.getDeviceId(),
                position,
                key -> lastGpsPositionRepository.findById(key).orElse(null),
                (old, newPos) -> {
                    isRecent.set(old.getEventPositionDate().compareTo(newPos.getEventPositionDate()) < 0);
                    return isRecent.get();
                }
            );

            if (isRecent.get()) {
                var infoList = lastTellTaleInfoMapper.getLastTellTaleInfoListFromLastPosition(position);
                infoList.forEach(lastTellTaleInfoRepository::save);
            }

            tellTaleInfoMapper.fromVmson(record).parallelStream().forEach(tellTaleInfoRepository::save);
        }
    }

    private void processError(String msg, String errorMessage) {
        errorCounter.incrementAndGet();
        if (isEnabled.get()) {
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
        status.setLag(partitionsLag);
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
