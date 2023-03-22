package com.trackysat.kafka.service.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.trackysat.kafka.domain.aggregations.PositionDTO;
import java.time.Instant;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class DailyAggregationDTO {

    String deviceId;
    Instant aggregatedDate = Instant.now();
    List<PositionDTO> positions;

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

    public List<PositionDTO> getPositions() {
        return positions;
    }

    public void setPositions(List<PositionDTO> positions) {
        this.positions = positions;
    }
}
