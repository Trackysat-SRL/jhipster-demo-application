package com.trackysat.kafka.domain.aggregations;

import java.time.Instant;
import java.util.Objects;

public class PositionDTO {

    private Instant timestamp;
    private Double longitude; // lon
    private Double latitude; // lat
    private Integer altitude; // alt
    private Integer speed; // spe
    private Integer numberOfSatellites; // snr
    private Integer direction; // dir
    private String signal; // sig
    private String fix; // fix Fix State Enumeration Enum
    private String typ; // Localization system type Enum

    public Instant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    public Double getLongitude() {
        return longitude;
    }

    public void setLongitude(Double longitude) {
        this.longitude = longitude;
    }

    public Double getLatitude() {
        return latitude;
    }

    public void setLatitude(Double latitude) {
        this.latitude = latitude;
    }

    public Integer getAltitude() {
        return altitude;
    }

    public void setAltitude(Integer altitude) {
        this.altitude = altitude;
    }

    public Integer getSpeed() {
        return speed;
    }

    public void setSpeed(Integer speed) {
        this.speed = speed;
    }

    public Integer getNumberOfSatellites() {
        return numberOfSatellites;
    }

    public void setNumberOfSatellites(Integer numberOfSatellites) {
        this.numberOfSatellites = numberOfSatellites;
    }

    public String getSignal() {
        return signal;
    }

    public void setSignal(String signal) {
        this.signal = signal;
    }

    public Integer getDirection() {
        return direction;
    }

    public void setDirection(Integer direction) {
        this.direction = direction;
    }

    public String getFix() {
        return fix;
    }

    public void setFix(String fix) {
        this.fix = fix;
    }

    public String getTyp() {
        return typ;
    }

    public void setTyp(String typ) {
        this.typ = typ;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PositionDTO)) return false;
        PositionDTO that = (PositionDTO) o;
        return (
            getLongitude().equals(that.getLongitude()) &&
            getLatitude().equals(that.getLatitude()) &&
            getAltitude().equals(that.getAltitude()) &&
            getSpeed().equals(that.getSpeed())
        );
    }

    @Override
    public int hashCode() {
        return Objects.hash(getLongitude(), getLatitude(), getAltitude(), getSpeed());
    }
}
