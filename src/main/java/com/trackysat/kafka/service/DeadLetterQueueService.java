package com.trackysat.kafka.service;

import com.trackysat.kafka.domain.DeadLetterQueue;
import com.trackysat.kafka.repository.DeadLetterQueueRepository;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Service class for managing devices.
 */
@Service
public class DeadLetterQueueService {

    private final Logger log = LoggerFactory.getLogger(DeadLetterQueueService.class);

    private final DeadLetterQueueRepository deadLetterQueueRepository;

    private final CassandraConsumerService cassandraConsumerService;

    public DeadLetterQueueService(DeadLetterQueueRepository deadLetterQueueRepository, CassandraConsumerService cassandraConsumerService) {
        this.deadLetterQueueRepository = deadLetterQueueRepository;
        this.cassandraConsumerService = cassandraConsumerService;
    }

    public List<DeadLetterQueue> getAll() {
        return deadLetterQueueRepository.findAll();
    }

    public void reprocess() {
        log.info("Start reprocessing events for DLQ");
        List<DeadLetterQueue> unprocessed = getAll().stream().filter(dlq -> !dlq.getProcessed()).collect(Collectors.toList());
        log.info("reprocessing {} events", unprocessed.size());
        unprocessed.forEach(dlq -> {
            try {
                cassandraConsumerService.processEvent(dlq.getData());
                dlq.setProcessed(true);
                deadLetterQueueRepository.save(dlq);
            } catch (Exception e) {
                log.error("error processing event with id: {}. Exception: {}", dlq.getEventId(), e);
            }
        });
        log.info("Finished reprocessing events for DLQ");
    }
}
