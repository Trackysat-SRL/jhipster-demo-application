package com.trackysat.kafka.domain.aggregations;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Objects;

public class SensorValDTO {

    Integer value;
    Instant creationDate;

    public Integer getValue() {
        return value;
    }

    public void setValue(Integer value) {
        this.value = value;
    }

    public Instant getCreationDate() {
        return creationDate;
    }

    public void setCreationDate(Instant creationDate) {
        this.creationDate = creationDate;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SensorValDTO)) return false;
        SensorValDTO that = (SensorValDTO) o;
        return (
            Objects.equals(getValue(), that.getValue()) &&
            OffsetDateTime
                .from(getCreationDate())
                .truncatedTo(ChronoUnit.MINUTES)
                .equals(OffsetDateTime.from(that.getCreationDate()).truncatedTo(ChronoUnit.MINUTES))
        );
    }

    @Override
    public int hashCode() {
        return Objects.hash(getValue(), getCreationDate());
    }
}
