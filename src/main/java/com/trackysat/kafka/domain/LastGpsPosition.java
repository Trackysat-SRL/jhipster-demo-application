package com.trackysat.kafka.domain;

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

    Double lat;

    Double lon;

    public LastGpsPosition() {}

    public LastGpsPosition(String deviceId, Instant eventPositionDate, String gpsPosition, Double lat, Double lon) {
        this.deviceId = deviceId;
        this.eventPositionDate = eventPositionDate;
        this.gpsPosition = gpsPosition;
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
            Objects.equals(getGpsPosition(), that.getGpsPosition()) &&
            Objects.equals(getLat(), that.getLat()) &&
            Objects.equals(getLon(), that.getLon())
        );
    }

    @Override
    public int hashCode() {
        return Objects.hash(getDeviceId(), getEventPositionDate(), getGpsPosition(), getLat(), getLon());
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
            ", lat='" +
            lat +
            "'" +
            ", lon='" +
            lon +
            "' }"
        );
    }
}
