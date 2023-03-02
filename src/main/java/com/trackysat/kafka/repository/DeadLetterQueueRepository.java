package com.trackysat.kafka.repository;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.mapper.annotations.*;
import com.trackysat.kafka.domain.DeadLetterQueue;
import java.util.List;
import java.util.Set;
import javax.validation.ConstraintViolation;
import javax.validation.ConstraintViolationException;
import javax.validation.Validator;
import org.springframework.boot.autoconfigure.cassandra.CassandraProperties;
import org.springframework.stereotype.Repository;

/**
 * Spring Data Cassandra repository for the {@link DeadLetterQueue} entity.
 */
@Repository
public class DeadLetterQueueRepository {

    private final CqlSession session;

    private final Validator validator;

    private final DeadLetterQueueDao deadLetterQueueDao;

    public DeadLetterQueueRepository(CqlSession session, Validator validator, CassandraProperties cassandraProperties) {
        this.session = session;
        this.validator = validator;
        DeadLetterQueueTokenMapper deadLetterQueueTokenMapper = new DeadLetterQueueTokenMapperBuilder(session).build();
        deadLetterQueueDao =
            deadLetterQueueTokenMapper.deadLetterQueueTokenDao(CqlIdentifier.fromCql(cassandraProperties.getKeyspaceName()));
    }

    public List<DeadLetterQueue> findAll() {
        return deadLetterQueueDao.findAll().all();
    }

    public DeadLetterQueue save(DeadLetterQueue deadLetterQueue) {
        Set<ConstraintViolation<DeadLetterQueue>> violations = validator.validate(deadLetterQueue);
        if (violations != null && !violations.isEmpty()) {
            throw new ConstraintViolationException(violations);
        }
        //        DeadLetterQueue oldDeadLetterQueue = deadLetterQueueDao.get(deadLetterQueue.getCustomerId()).orElse(null);
        BatchStatementBuilder batch = BatchStatement.builder(DefaultBatchType.UNLOGGED);
        batch.addStatement(deadLetterQueueDao.saveQuery(deadLetterQueue));
        session.execute(batch.build());
        return deadLetterQueue;
    }

    public void delete(DeadLetterQueue deadLetterQueue) {
        BatchStatementBuilder batch = BatchStatement.builder(DefaultBatchType.UNLOGGED);
        batch.addStatement(deadLetterQueueDao.deleteQuery(deadLetterQueue));
        session.execute(batch.build());
    }
}

@Dao
interface DeadLetterQueueDao {
    @Select
    PagingIterable<DeadLetterQueue> findAll();

    @Insert
    BoundStatement saveQuery(DeadLetterQueue deadLetterQueue);

    @Delete
    BoundStatement deleteQuery(DeadLetterQueue deadLetterQueue);
}

@Mapper
interface DeadLetterQueueTokenMapper {
    @DaoFactory
    DeadLetterQueueDao deadLetterQueueTokenDao(@DaoKeyspace CqlIdentifier keyspace);
}
