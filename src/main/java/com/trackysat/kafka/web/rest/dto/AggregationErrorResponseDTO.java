package com.trackysat.kafka.web.rest.dto;

import java.util.List;
import java.util.Map;

public class AggregationErrorResponseDTO {

    private Map<String, List<String>> errors;

    public AggregationErrorResponseDTO() {}

    public AggregationErrorResponseDTO(Map<String, List<String>> errors) {
        this.errors = errors;
    }

    public Map<String, List<String>> getErrors() {
        return errors;
    }

    public void setErrors(Map<String, List<String>> errors) {
        this.errors = errors;
    }
}
