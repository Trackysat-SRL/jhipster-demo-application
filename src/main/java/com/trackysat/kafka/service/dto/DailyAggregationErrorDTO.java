package com.trackysat.kafka.service.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.trackysat.kafka.domain.aggregations.PositionDTO;
import com.trackysat.kafka.domain.aggregations.SensorStatsDTO;
import java.time.Instant;
import java.util.List;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class DailyAggregationErrorDTO {

    String deviceId;
    Instant aggregatedDate = Instant.now();

    String error;

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public Instant getAggregatedDate() {
        return aggregatedDate;
    }

    public void setAggregatedDate(Instant aggregatedDate) {
        this.aggregatedDate = aggregatedDate;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }
}
