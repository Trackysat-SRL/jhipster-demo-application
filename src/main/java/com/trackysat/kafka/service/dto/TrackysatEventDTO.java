package com.trackysat.kafka.service.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.trackysat.kafka.domain.vmson.Des;
import com.trackysat.kafka.domain.vmson.Ets;
import com.trackysat.kafka.domain.vmson.Ori;
import com.trackysat.kafka.domain.vmson.VmsonCon;
import java.time.Instant;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class TrackysatEventDTO {

    String deviceId;
    Instant createdDate;
    Instant eventDate;
    String uid;
    String ver;
    Des des;
    Ets ets;
    Ori ori;
    List<VmsonCon> con;

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public Instant getCreatedDate() {
        return createdDate;
    }

    public void setCreatedDate(Instant createdDate) {
        this.createdDate = createdDate;
    }

    public Instant getEventDate() {
        return eventDate;
    }

    public void setEventDate(Instant eventDate) {
        this.eventDate = eventDate;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getVer() {
        return ver;
    }

    public void setVer(String ver) {
        this.ver = ver;
    }

    public Des getDes() {
        return des;
    }

    public void setDes(Des des) {
        this.des = des;
    }

    public Ets getEts() {
        return ets;
    }

    public void setEts(Ets ets) {
        this.ets = ets;
    }

    public Ori getOri() {
        return ori;
    }

    public void setOri(Ori ori) {
        this.ori = ori;
    }

    public List<VmsonCon> getCon() {
        return con;
    }

    public void setCon(List<VmsonCon> con) {
        this.con = con;
    }
}
