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
public class DailyAggregationError implements Serializable {

    private static final long serialVersionUID = 1L;

    @PartitionKey
    String deviceId;

    @ClusteringColumn
    Instant aggregatedDate = Instant.now();

    String error;

    public DailyAggregationError() {}

    public DailyAggregationError(String deviceId, Instant aggregatedDate, String error) {
        this.deviceId = deviceId;
        this.aggregatedDate = aggregatedDate;
        this.error = error;
    }

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

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DailyAggregationError)) return false;
        DailyAggregationError that = (DailyAggregationError) o;
        return (
            Objects.equals(getDeviceId(), that.getDeviceId()) &&
            Objects.equals(getAggregatedDate(), that.getAggregatedDate()) &&
            Objects.equals(getError(), that.getError())
        );
    }

    @Override
    public int hashCode() {
        return Objects.hash(getDeviceId(), getAggregatedDate(), getError());
    }

    @Override
    public String toString() {
        return (
            "DailyAggregationError{" +
            "deviceId='" +
            deviceId +
            '\'' +
            ", aggregatedDate=" +
            aggregatedDate +
            ", error='" +
            error +
            '\'' +
            '}'
        );
    }
}
