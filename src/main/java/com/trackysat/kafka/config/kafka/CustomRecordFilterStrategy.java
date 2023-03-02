package com.trackysat.kafka.config.kafka;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.adapter.RecordFilterStrategy;

public class CustomRecordFilterStrategy implements RecordFilterStrategy<String, String> {

    @Override
    public boolean filter(ConsumerRecord<String, String> consumerRecord) {
        return (consumerRecord.value()).startsWith("<");
    }

    @Override
    public List<ConsumerRecord<String, String>> filterBatch(List<ConsumerRecord<String, String>> consumerRecords) {
        return consumerRecords.stream().filter(this::filter).collect(Collectors.toList());
    }
}
