package com.trackysat.kafka.domain;

import com.datastax.oss.driver.api.mapper.annotations.ClusteringColumn;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.NamingStrategy;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.datastax.oss.driver.api.mapper.entity.naming.NamingConvention;
import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

@Entity
@NamingStrategy(convention = NamingConvention.SNAKE_CASE_INSENSITIVE)
public class LastGpsPosition implements Serializable {

    private static final long serialVersionUID = 1L;

    @PartitionKey
    String deviceId;

    Instant eventPositionDate;

    String gpsPosition;

    public LastGpsPosition() {}

    public LastGpsPosition(String deviceId, Instant eventPositionDate, String gpsPosition) {
        this.deviceId = deviceId;
        this.eventPositionDate = eventPositionDate;
        this.gpsPosition = gpsPosition;
    }

    public String getGpsPosition() {
        return gpsPosition;
    }

    public void setGpsPosition(String gpsPosition) {
        this.gpsPosition = gpsPosition;
    }

    public Instant getEventPositionDate() {
        return eventPositionDate;
    }

    public void setEventPositionDate(Instant eventPositionDate) {
        this.eventPositionDate = eventPositionDate;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof LastGpsPosition)) return false;
        var that = (LastGpsPosition) o;
        return (
            (Objects.equals(getDeviceId(), that.getDeviceId()) && Objects.equals(getEventPositionDate(), that.getEventPositionDate())) &&
            Objects.equals(getGpsPosition(), that.getGpsPosition())
        );
    }

    @Override
    public int hashCode() {
        return Objects.hash(getDeviceId(), getEventPositionDate(), getGpsPosition());
    }

    @Override
    public String toString() {
        return (
            "LastGpsPosition{" +
            "deviceId='" +
            deviceId +
            '\'' +
            ", eventPositionDate=" +
            eventPositionDate +
            ", gpsPosition='" +
            gpsPosition +
            '\'' +
            '}'
        );
    }
}
