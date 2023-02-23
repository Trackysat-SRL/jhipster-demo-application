package com.trackysat.kafka.exeptions;

import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.serializer.FailedDeserializationInfo;

public class ErrorHandlingDeserializerSupport<T> {

    private static final Logger logger = LoggerFactory.getLogger(ErrorHandlingDeserializerSupport.class);

    public Function<FailedDeserializationInfo, T> supply() {
        return message -> {
            byte[] data = message.getData();
            logger.error("message input: {}", new String(data));
            return null;
        };
    }
}
