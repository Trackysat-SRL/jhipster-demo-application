package com.trackysat.kafka.domain;

import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.NamingStrategy;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.datastax.oss.driver.api.mapper.entity.naming.NamingConvention;
import com.trackysat.kafka.domain.aggregations.PositionDTO;
import java.io.Serializable;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * Daily aggregation of TrackyEvents for device in a single date
 */
@Entity
@NamingStrategy(convention = NamingConvention.SNAKE_CASE_INSENSITIVE)
public class MonthlyAggregation implements Serializable {

    private static final long serialVersionUID = 1L;

    @PartitionKey
    String deviceId;

    Instant timestamp = Instant.now();

    String positions;

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    public String getPositions() {
        return positions;
    }

    public void setPositions(String positions) {
        this.positions = positions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MonthlyAggregation)) return false;
        MonthlyAggregation dailyAggregation = (MonthlyAggregation) o;
        return (
            Objects.equals(getDeviceId(), dailyAggregation.getDeviceId()) && Objects.equals(getTimestamp(), dailyAggregation.getTimestamp())
        );
    }

    @Override
    public int hashCode() {
        // see https://vladmihalcea.com/how-to-implement-equals-and-hashcode-using-the-jpa-entity-identifier/
        return getClass().hashCode();
    }

    // prettier-ignore
    @Override
    public String toString() {
        return "DailyAggregation{" +
            "deviceId='" + deviceId + '\'' +
            ", timestamp=" + timestamp +
            '}';
    }
}
