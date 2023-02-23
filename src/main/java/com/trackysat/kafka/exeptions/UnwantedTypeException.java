package com.trackysat.kafka.exeptions;

import org.apache.kafka.common.errors.SerializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UnwantedTypeException extends SerializationException {

    private static final Logger LOGGER = LoggerFactory.getLogger(UnwantedTypeException.class);

    public UnwantedTypeException(String message) {
        LOGGER.debug("XML Filtered: {}", message);
    }
}
