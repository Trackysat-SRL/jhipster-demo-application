package com.trackysat.kafka.domain;

import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.NamingStrategy;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.datastax.oss.driver.api.mapper.entity.naming.NamingConvention;
import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

/**
 * A status of the last job execution
 */
@Entity
@NamingStrategy(convention = NamingConvention.SNAKE_CASE_INSENSITIVE)
public class JobStatus implements Serializable {

    private static final long serialVersionUID = 1L;

    @PartitionKey
    String jobId;

    Instant updatedDate = Instant.now();
    Boolean lock;
    String data;

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public Instant getUpdatedDate() {
        return updatedDate;
    }

    public void setUpdatedDate(Instant updatedDate) {
        this.updatedDate = updatedDate;
    }

    public Boolean getLock() {
        return lock;
    }

    public void setLock(Boolean lock) {
        this.lock = lock;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof JobStatus)) return false;
        JobStatus devices = (JobStatus) o;
        return Objects.equals(getJobId(), devices.getJobId());
    }

    @Override
    public int hashCode() {
        // see https://vladmihalcea.com/how-to-implement-equals-and-hashcode-using-the-jpa-entity-identifier/
        return getClass().hashCode();
    }

    // prettier-ignore

    @Override
    public String toString() {
        return "JobStatus{" +
            "jobId='" + jobId + '\'' +
            ", updatedDate=" + updatedDate +
            ", lock=" + lock +
            ", data='" + data + '\'' +
            '}';
    }
}
