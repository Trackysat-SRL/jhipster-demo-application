package com.trackysat.kafka.service.dto;

import java.time.Instant;

public class LastTellTaleInfoDTO {

    private String deviceId;
    private String iid;
    private String state;
    private Instant eventCreatedDate;
    private Double lat;
    private Double lon;

    public LastTellTaleInfoDTO() {}

    public LastTellTaleInfoDTO(String deviceId, String iid, String state, Instant eventCreatedDate, Double lon, Double lat) {
        this.deviceId = deviceId;
        this.iid = iid;
        this.state = state;
        this.eventCreatedDate = eventCreatedDate;
        this.lat = lat;
        this.lon = lon;
    }

    public Double getLat() {
        return lat;
    }

    public void setLat(Double lat) {
        this.lat = lat;
    }

    public Double getLon() {
        return lon;
    }

    public void setLon(Double lon) {
        this.lon = lon;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public String getIid() {
        return iid;
    }

    public void setIid(String iid) {
        this.iid = iid;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public Instant getEventCreatedDate() {
        return eventCreatedDate;
    }

    public void setEventCreatedDate(Instant eventCreatedDate) {
        this.eventCreatedDate = eventCreatedDate;
    }
}
