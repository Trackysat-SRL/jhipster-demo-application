package com.trackysat.kafka.domain;

import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.NamingStrategy;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.datastax.oss.driver.api.mapper.entity.naming.NamingConvention;
import java.io.Serializable;
import java.time.Instant;

/**
 * A Dead Letter Queue for errors in processing events.
 */
@Entity
@NamingStrategy(convention = NamingConvention.SNAKE_CASE_INSENSITIVE)
public class DeadLetterQueue implements Serializable {

    private static final long serialVersionUID = 1L;

    @PartitionKey(0)
    private String eventId;

    @PartitionKey(1)
    private Instant createdDate = Instant.now();

    private String data;

    private String exception;

    private Boolean processed = false;

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public Instant getCreatedDate() {
        return createdDate;
    }

    public void setCreatedDate(Instant createdDate) {
        this.createdDate = createdDate;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public String getException() {
        return exception;
    }

    public void setException(String exception) {
        this.exception = exception;
    }

    public Boolean getProcessed() {
        return processed;
    }

    public void setProcessed(Boolean processed) {
        this.processed = processed;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DeadLetterQueue)) {
            return false;
        }
        return eventId != null && eventId.equals(((DeadLetterQueue) o).eventId);
    }

    @Override
    public int hashCode() {
        // see https://vladmihalcea.com/how-to-implement-equals-and-hashcode-using-the-jpa-entity-identifier/
        return getClass().hashCode();
    }

    // prettier-ignore

    @Override
    public String toString() {
        return "DeadLetterQueue{" +
            "eventId='" + eventId + '\'' +
            ", createdDate=" + createdDate +
            ", data='" + data + '\'' +
            ", exception='" + exception + '\'' +
            ", processed=" + processed +
            '}';
    }
}
