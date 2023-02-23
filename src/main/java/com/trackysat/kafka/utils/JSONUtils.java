package com.trackysat.kafka.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.stereotype.Component;

@Component
public final class JSONUtils {

    public static String toString(Object value) throws JsonProcessingException {
        return JSONUtils.buildObjectMapper().writeValueAsString(value);
    }

    public static <T> T toJson(String value, Class<T> clazz) throws JsonProcessingException {
        return JSONUtils.buildObjectMapper().readValue(value, clazz);
    }

    public static ObjectMapper buildObjectMapper() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        return objectMapper;
    }
}
