package com.trackysat.kafka.service.dto;

import java.time.Instant;

public class TellTaleInfoDTO {

    private String deviceId;
    private String iid;
    private String ets;
    private String sen;
    private Instant createdDate;
    private Instant eventCreatedDate;

    public TellTaleInfoDTO() {}

    public TellTaleInfoDTO(String deviceId, String iid, String ets, String sen, Instant createdDate, Instant eventCreatedDate) {
        this.deviceId = deviceId;
        this.iid = iid;
        this.ets = ets;
        this.sen = sen;
        this.createdDate = createdDate;
        this.eventCreatedDate = eventCreatedDate;
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

    public String getEts() {
        return ets;
    }

    public void setEts(String ets) {
        this.ets = ets;
    }

    public String getSen() {
        return sen;
    }

    public void setSen(String sen) {
        this.sen = sen;
    }

    public Instant getCreatedDate() {
        return createdDate;
    }

    public void setCreatedDate(Instant createdDate) {
        this.createdDate = createdDate;
    }

    public Instant getEventCreatedDate() {
        return eventCreatedDate;
    }

    public void setEventCreatedDate(Instant eventCreatedDate) {
        this.eventCreatedDate = eventCreatedDate;
    }
}
