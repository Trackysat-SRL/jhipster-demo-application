package com.trackysat.kafka.config.kafka;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.adapter.RecordFilterStrategy;

public class CustomRecordFilterStrategy implements RecordFilterStrategy<String, Object> {

    @Override
    public boolean filter(ConsumerRecord<String, Object> consumerRecord) {
        return ((String) consumerRecord.value()).startsWith("<");
    }

    @Override
    public List<ConsumerRecord<String, Object>> filterBatch(List<ConsumerRecord<String, Object>> consumerRecords) {
        return consumerRecords.stream().filter(this::filter).collect(Collectors.toList());
    }
}
