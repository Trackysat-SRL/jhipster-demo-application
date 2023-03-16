package com.trackysat.kafka.repository;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.mapper.annotations.*;
import com.trackysat.kafka.domain.JobStatus;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import javax.validation.ConstraintViolation;
import javax.validation.ConstraintViolationException;
import javax.validation.Validator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.cassandra.CassandraProperties;
import org.springframework.stereotype.Repository;

/**
 * Spring Data Cassandra repository for the {@link JobStatus} entity.
 */
@Repository
public class JobStatusRepository {

    private final Logger log = LoggerFactory.getLogger(JobStatusRepository.class);

    private final CqlSession session;

    private final Validator validator;

    private final JobStatusDao jobStatusDao;

    public JobStatusRepository(CqlSession session, Validator validator, CassandraProperties cassandraProperties) {
        this.session = session;
        this.validator = validator;
        JobStatusTokenMapper jobStatusTokenMapper = new JobStatusTokenMapperBuilder(session).build();
        jobStatusDao = jobStatusTokenMapper.jobStatusTokenDao(CqlIdentifier.fromCql(cassandraProperties.getKeyspaceName()));
    }

    // -- CRUD -- //

    public Optional<JobStatus> findById(String id) {
        return jobStatusDao.get(id);
    }

    public List<JobStatus> findAll() {
        return jobStatusDao.findAll().all();
    }

    public JobStatus save(JobStatus jobStatus) {
        Set<ConstraintViolation<JobStatus>> violations = validator.validate(jobStatus);
        if (violations != null && !violations.isEmpty()) {
            throw new ConstraintViolationException(violations);
        }
        BatchStatementBuilder batch = BatchStatement.builder(DefaultBatchType.UNLOGGED);
        batch.addStatement(jobStatusDao.saveQuery(jobStatus));
        session.execute(batch.build());
        return jobStatus;
    }

    public void delete(JobStatus jobStatus) {
        BatchStatementBuilder batch = BatchStatement.builder(DefaultBatchType.UNLOGGED);
        batch.addStatement(jobStatusDao.deleteQuery(jobStatus));
        session.execute(batch.build());
    }
}

@Dao
interface JobStatusDao {
    @Select
    Optional<JobStatus> get(String jobStatusId);

    @Select
    PagingIterable<JobStatus> findAll();

    @Insert
    BoundStatement saveQuery(JobStatus jobStatus);

    @Delete
    BoundStatement deleteQuery(JobStatus jobStatus);
}

@Mapper
interface JobStatusTokenMapper {
    @DaoFactory
    JobStatusDao jobStatusTokenDao(@DaoKeyspace CqlIdentifier keyspace);
}
