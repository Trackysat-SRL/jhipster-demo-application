package com.trackysat.kafka.service.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.time.Instant;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class LastGpsPositionDTO {

    private String deviceId;
    private Instant eventPositionDate;
    private String gpsPosition;
    private Double lon;
    private Double lat;

    public LastGpsPositionDTO() {}

    public LastGpsPositionDTO(String deviceId, Instant eventPositionDate, String gpsPosition, Double lon, Double lat) {
        this.deviceId = deviceId;
        this.eventPositionDate = eventPositionDate;
        this.gpsPosition = gpsPosition;
        this.lon = lon;
        this.lat = lat;
    }

    public Double getLon() {
        return lon;
    }

    public void setLon(Double lon) {
        this.lon = lon;
    }

    public Double getLat() {
        return lat;
    }

    public void setLat(Double lat) {
        this.lat = lat;
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
