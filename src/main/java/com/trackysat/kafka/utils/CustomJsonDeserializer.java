package com.trackysat.kafka.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.trackysat.kafka.exeptions.UnwantedTypeException;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

/**
 * Convenient class for logger and customization
 * <p>
 * Use json deserializer
 *
 * @param <T>
 */
public class CustomJsonDeserializer<T> extends JsonDeserializer {

    private static final Logger LOGGER = LoggerFactory.getLogger(CustomJsonDeserializer.class);

    public CustomJsonDeserializer(ObjectMapper objectMapper) {
        super(objectMapper);
    }

    @Override
    public Object deserialize(String topic, byte[] data) {
        if (!ArrayUtils.isEmpty(data)) {
            LOGGER.debug("Input json: {}", new String(data));
        }
        String dataString = new String(data);
        if (dataString.startsWith("<")) {
            throw new UnwantedTypeException(dataString);
        }
        return super.deserialize(topic, data);
    }

    @Override
    public Object deserialize(String topic, Headers headers, byte[] data) {
        if (!ArrayUtils.isEmpty(data)) {
            LOGGER.debug("Input json: {} - {}", headers, new String(data));
        }
        String dataString = new String(data);
        if (dataString.startsWith("<")) {
            throw new UnwantedTypeException(dataString);
        }
        return super.deserialize(topic, headers, data);
    }
}
