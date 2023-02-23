package com.trackysat.kafka.repository;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.mapper.annotations.*;
import com.trackysat.kafka.domain.TrackyPosition;
import java.util.List;
import java.util.Set;
import javax.validation.ConstraintViolation;
import javax.validation.ConstraintViolationException;
import javax.validation.Validator;
import org.springframework.boot.autoconfigure.cassandra.CassandraProperties;
import org.springframework.stereotype.Repository;

/**
 * Spring Data Cassandra repository for the {@link TrackyPosition} entity.
 */
@Repository
public class TrackyPositionRepository {

    private final CqlSession session;

    private final Validator validator;

    private TrackyPositionDao trackyPositionDao;

    public TrackyPositionRepository(CqlSession session, Validator validator, CassandraProperties cassandraProperties) {
        this.session = session;
        this.validator = validator;
        TrackyPositionTokenMapper trackyPositionTokenMapper = new TrackyPositionTokenMapperBuilder(session).build();
        trackyPositionDao = trackyPositionTokenMapper.trackyPositionTokenDao(CqlIdentifier.fromCql(cassandraProperties.getKeyspaceName()));
    }

    public List<TrackyPosition> findAll() {
        return trackyPositionDao.findAll().all();
    }

    public TrackyPosition save(TrackyPosition trackyPosition) {
        Set<ConstraintViolation<TrackyPosition>> violations = validator.validate(trackyPosition);
        if (violations != null && !violations.isEmpty()) {
            throw new ConstraintViolationException(violations);
        }
        //        TrackyPosition oldTrackyPosition = trackyPositionDao.get(trackyPosition.getCustomerId()).orElse(null);
        BatchStatementBuilder batch = BatchStatement.builder(DefaultBatchType.LOGGED);
        batch.addStatement(trackyPositionDao.saveQuery(trackyPosition));
        session.execute(batch.build());
        return trackyPosition;
    }

    public void delete(TrackyPosition trackyPosition) {
        BatchStatementBuilder batch = BatchStatement.builder(DefaultBatchType.LOGGED);
        batch.addStatement(trackyPositionDao.deleteQuery(trackyPosition));
        session.execute(batch.build());
    }
}

@Dao
interface TrackyPositionDao {
    @Select
    PagingIterable<TrackyPosition> findAll();

    @Insert
    BoundStatement saveQuery(TrackyPosition trackyPosition);

    @Delete
    BoundStatement deleteQuery(TrackyPosition trackyPosition);
}

@Mapper
interface TrackyPositionTokenMapper {
    @DaoFactory
    TrackyPositionDao trackyPositionTokenDao(@DaoKeyspace CqlIdentifier keyspace);
}
