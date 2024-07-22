package com.trackysat.kafka.service.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.time.Instant;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class LastGpsPositionDTO {

    private String deviceId;
    private Instant eventPositionDate;
    private String gpsPosition;

    public LastGpsPositionDTO() {}

    public LastGpsPositionDTO(String deviceId, Instant eventPositionDate, String gpsPosition) {
        this.deviceId = deviceId;
        this.eventPositionDate = eventPositionDate;
        this.gpsPosition = gpsPosition;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public Instant getEventPositionDate() {
        return eventPositionDate;
    }

    public void setEventPositionDate(Instant eventPositionDate) {
        this.eventPositionDate = eventPositionDate;
    }

    public String getGpsPosition() {
        return gpsPosition;
    }

    public void setGpsPosition(String gpsPosition) {
        this.gpsPosition = gpsPosition;
    }
}
