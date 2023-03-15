package com.trackysat.kafka.domain;

import com.datastax.oss.driver.api.mapper.annotations.ClusteringColumn;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.NamingStrategy;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.datastax.oss.driver.api.mapper.entity.naming.NamingConvention;
import java.io.Serializable;
import java.time.Instant;

/**
 * A TrackysatEvent from vmson.
 */
@Entity
@NamingStrategy(convention = NamingConvention.SNAKE_CASE_INSENSITIVE)
public class TrackyEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    @PartitionKey
    private String deviceId;

    @ClusteringColumn
    private Instant createdDate = Instant.now();

    private Instant eventDate = Instant.now();

    String uid;
    String ver;
    String des;
    String ets;
    String ori;
    String con;

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

    public String getDes() {
        return des;
    }

    public void setDes(String des) {
        this.des = des;
    }

    public String getEts() {
        return ets;
    }

    public void setEts(String ets) {
        this.ets = ets;
    }

    public String getOri() {
        return ori;
    }

    public void setOri(String ori) {
        this.ori = ori;
    }

    public String getCon() {
        return con;
    }

    public void setCon(String con) {
        this.con = con;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TrackyEvent)) {
            return false;
        }
        return deviceId != null && deviceId.equals(((TrackyEvent) o).deviceId);
    }

    @Override
    public int hashCode() {
        // see https://vladmihalcea.com/how-to-implement-equals-and-hashcode-using-the-jpa-entity-identifier/
        return getClass().hashCode();
    }

    // prettier-ignore

    @Override
    public String toString() {
        return "TrackysatEvent{" +
            "deviceId='" + deviceId + '\'' +
            ", createdDate=" + createdDate +
            ", eventDate=" + eventDate +
            ", uid='" + uid + '\'' +
            ", ver='" + ver + '\'' +
            ", des=" + des +
            ", ets=" + ets +
            ", ori=" + ori +
            ", con=" + con +
            '}';
    }
}
