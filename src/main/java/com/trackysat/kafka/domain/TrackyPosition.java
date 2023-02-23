package com.trackysat.kafka.domain;

import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.NamingStrategy;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.datastax.oss.driver.api.mapper.entity.naming.NamingConvention;
import java.io.Serializable;
import java.time.Instant;
import javax.validation.constraints.Size;

/**
 * A trackysat position from json.
 */
@Entity
@NamingStrategy(convention = NamingConvention.SNAKE_CASE_INSENSITIVE)
public class TrackyPosition implements Serializable {

    private static final long serialVersionUID = 1L;

    @PartitionKey(0)
    private String customerId;

    @PartitionKey(1)
    private Instant createdDate = Instant.now();

    @Size(max = 255)
    private String trigger;

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public Instant getCreatedDate() {
        return createdDate;
    }

    public void setCreatedDate(Instant createdDate) {
        this.createdDate = createdDate;
    }

    public String getTrigger() {
        return trigger;
    }

    public void setTrigger(String trigger) {
        this.trigger = trigger;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TrackyPosition)) {
            return false;
        }
        return customerId != null && customerId.equals(((TrackyPosition) o).customerId);
    }

    @Override
    public int hashCode() {
        // see https://vladmihalcea.com/how-to-implement-equals-and-hashcode-using-the-jpa-entity-identifier/
        return getClass().hashCode();
    }

    // prettier-ignore
    @Override
    public String toString() {
        return "TrackyPosition{" +
            "customerId='" + customerId + '\'' +
            ", createdDate=" + createdDate +
            ", trigger='" + trigger + '\'' +
            '}';
    }
}
