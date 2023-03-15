package com.trackysat.kafka.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
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

    public static <T> T toJson(String value, TypeReference<T> tr) throws JsonProcessingException {
        return JSONUtils.buildObjectMapper().readValue(value, tr);
    }

    public static ObjectMapper buildObjectMapper() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return objectMapper;
    }
}
