package com.trackysat.kafka.repository;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.mapper.annotations.*;
import com.trackysat.kafka.domain.LastGpsPosition;
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
 * Spring Data Cassandra repository for the {@link LastGpsPosition} entity.
 */
@Repository
public class LastGpsRepository {

    private final Logger log = LoggerFactory.getLogger(LastGpsRepository.class);

    private final CqlSession session;

    private final Validator validator;

    private final LastGpsPositionDao lastGpsPositionDao;

    public LastGpsRepository(CqlSession session, Validator validator, CassandraProperties cassandraProperties) {
        this.session = session;
        this.validator = validator;
        LastGpsPositionTokenMapper lastGpsPositionTokenMapper = new LastGpsPositionTokenMapperBuilder(session).build();
        lastGpsPositionDao =
            lastGpsPositionTokenMapper.lastGpsPositionTokenDao(CqlIdentifier.fromCql(cassandraProperties.getKeyspaceName()));
    }

    // -- CRUD -- //

    public Optional<LastGpsPosition> findById(String id) {
        return lastGpsPositionDao.get(id);
    }

    public List<LastGpsPosition> findAll() {
        return lastGpsPositionDao.findAll().all();
    }

    public LastGpsPosition save(LastGpsPosition lastGpsPosition) {
        Set<ConstraintViolation<LastGpsPosition>> violations = validator.validate(lastGpsPosition);
        if (violations != null && !violations.isEmpty()) {
            throw new ConstraintViolationException(violations);
        }
        BatchStatementBuilder batch = BatchStatement.builder(DefaultBatchType.UNLOGGED);
        batch.addStatement(lastGpsPositionDao.saveQuery(lastGpsPosition));
        session.execute(batch.build());
        return lastGpsPosition;
    }

    public boolean delete(String id) {
        BatchStatementBuilder batch = BatchStatement.builder(DefaultBatchType.UNLOGGED);
        Optional<LastGpsPosition> gpsPosition = lastGpsPositionDao.get(id);
        if (gpsPosition.isEmpty()) {
            return false;
        } else {
            batch.addStatement(lastGpsPositionDao.deleteQuery(gpsPosition.get()));
            session.execute(batch.build());
            return true;
        }
    }
}

@Dao
interface LastGpsPositionDao {
    @Select
    Optional<LastGpsPosition> get(String deviceId);

    @Select
    PagingIterable<LastGpsPosition> findAll();

    @Insert
    BoundStatement saveQuery(LastGpsPosition lastGpsPosition);

    @Delete
    BoundStatement deleteQuery(LastGpsPosition lastGpsPosition);
}

@Mapper
interface LastGpsPositionTokenMapper {
    @DaoFactory
    LastGpsPositionDao lastGpsPositionTokenDao(@DaoKeyspace CqlIdentifier keyspace);
}
