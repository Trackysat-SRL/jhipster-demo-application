package com.trackysat.kafka.config.kafka;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.adapter.RecordFilterStrategy;

public class CustomRecordFilterStrategy implements RecordFilterStrategy<String, String> {

    private final Logger log = LoggerFactory.getLogger(CustomRecordFilterStrategy.class);

    @Override
    public boolean filter(ConsumerRecord<String, String> consumerRecord) {
        boolean filter = (consumerRecord.value()).startsWith("<");
        if (filter) log.debug("Filtered msg: " + consumerRecord.value());
        return filter;
    }

    @Override
    public List<ConsumerRecord<String, String>> filterBatch(List<ConsumerRecord<String, String>> consumerRecords) {
        return consumerRecords.stream().filter(this::filter).collect(Collectors.toList());
    }
}
