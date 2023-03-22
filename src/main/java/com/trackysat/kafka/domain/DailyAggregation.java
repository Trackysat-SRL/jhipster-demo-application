package com.trackysat.kafka.domain;

import com.datastax.oss.driver.api.mapper.annotations.ClusteringColumn;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.NamingStrategy;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.datastax.oss.driver.api.mapper.entity.naming.NamingConvention;
import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

/**
 * Daily aggregation of TrackyEvents for device in a single date
 */
@Entity
@NamingStrategy(convention = NamingConvention.SNAKE_CASE_INSENSITIVE)
public class DailyAggregation implements Serializable {

    private static final long serialVersionUID = 1L;

    @PartitionKey
    String deviceId;

    @ClusteringColumn
    Instant aggregatedDate = Instant.now();

    String positions;

    String sensors;

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public Instant getAggregatedDate() {
        return aggregatedDate;
    }

    public void setAggregatedDate(Instant aggregatedDate) {
        this.aggregatedDate = aggregatedDate;
    }

    public String getPositions() {
        return positions;
    }

    public void setPositions(String positions) {
        this.positions = positions;
    }

    public String getSensors() {
        return sensors;
    }

    public void setSensors(String sensors) {
        this.sensors = sensors;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DailyAggregation)) return false;
        DailyAggregation dailyAggregation = (DailyAggregation) o;
        return (
            Objects.equals(getDeviceId(), dailyAggregation.getDeviceId()) &&
            Objects.equals(getAggregatedDate(), dailyAggregation.getAggregatedDate())
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
            ", aggregatedDate=" + aggregatedDate +
            '}';
    }
}
