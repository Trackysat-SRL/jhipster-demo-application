package com.trackysat.kafka.exeptions;

import com.trackysat.kafka.domain.DeadLetterQueue;
import com.trackysat.kafka.repository.DeadLetterQueueRepository;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.serializer.FailedDeserializationInfo;

public class ErrorHandlingDeserializerSupport<T> {

    private static final Logger logger = LoggerFactory.getLogger(ErrorHandlingDeserializerSupport.class);

    public Function<FailedDeserializationInfo, T> supply(DeadLetterQueueRepository deadLetterQueueRepository) {
        return message -> {
            byte[] data = message.getData();
            String msg = new String(data);
            if (!msg.startsWith("<")) {
                try {
                    logger.error("message input: {}", msg);
                    MessageDigest digest = MessageDigest.getInstance("SHA-256");
                    byte[] hash = digest.digest(data);
                    DeadLetterQueue dlq = new DeadLetterQueue();
                    dlq.setEventId(new String(hash));
                    dlq.setData(msg);
                    dlq.setException(message.getException().getMessage());
                    deadLetterQueueRepository.save(dlq);
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
            }
            return null;
        };
    }
}
