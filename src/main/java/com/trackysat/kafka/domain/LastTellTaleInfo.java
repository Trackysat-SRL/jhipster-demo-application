package com.trackysat.kafka.domain;

import com.datastax.oss.driver.api.mapper.annotations.ClusteringColumn;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.NamingStrategy;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.datastax.oss.driver.api.mapper.entity.naming.NamingConvention;
import java.io.Serializable;
import java.time.Instant;

@Entity
@NamingStrategy(convention = NamingConvention.SNAKE_CASE_INSENSITIVE)
public class LastTellTaleInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    @PartitionKey
    private String deviceId;

    @ClusteringColumn
    private String iid;

    private String state;

    private Instant eventCreatedDate;

    private Double lat;
    private Double lon;

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

    @Override
    public int hashCode() {
        // see https://vladmihalcea.com/how-to-implement-equals-and-hashcode-using-the-jpa-entity-identifier/
        return getClass().hashCode();
    }

    // prettier-ignore
    @Override
    public String toString() {
        return "LastTellTaleInfo{" +
            "deviceId='" + deviceId + '\'' +
            ", iid='" + iid + '\'' +
            ", eventCreatedDate='" + eventCreatedDate + '\'' +
            ", state='" + state + '\'' +
            ", lat='" + lat + '\'' +
            ", lon='" + lon + '\'' +
            '}';
    }
}
