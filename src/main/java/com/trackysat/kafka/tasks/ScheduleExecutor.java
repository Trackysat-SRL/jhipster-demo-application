package com.trackysat.kafka.tasks;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.trackysat.kafka.config.kafka.KafkaTopicConfig;
import com.trackysat.kafka.domain.TrackyPosition;
import com.trackysat.kafka.utils.JSONUtils;
import java.text.SimpleDateFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Component
public class ScheduleExecutor {

    private static final Logger log = LoggerFactory.getLogger(ScheduleExecutor.class);
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    //    @Scheduled(fixedRate = 5000) TODO uncomment to test producer
    public void produceJson() throws JsonProcessingException {
        log.info("[produceJson] started");
        TrackyPosition p = new TrackyPosition();
        p.setCustomerId("raul");
        p.setTrigger("test data");
        String message = JSONUtils.toString(p);
        ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(KafkaTopicConfig.TRACKY_TOPIC, message);

        future.addCallback(
            new ListenableFutureCallback<SendResult<String, String>>() {
                @Override
                public void onSuccess(SendResult<String, String> result) {
                    log.info("Sent message=[" + message + "] with offset=[" + result.getRecordMetadata().offset() + "]");
                }

                @Override
                public void onFailure(Throwable ex) {
                    log.error("Unable to send message=[" + message + "] due to : " + ex.getMessage());
                }
            }
        );
        log.info("[produceJson] finished");
    }
}
