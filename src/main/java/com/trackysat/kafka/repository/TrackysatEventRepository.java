package com.trackysat.kafka.repository;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.mapper.annotations.*;
import com.trackysat.kafka.domain.TrackysatEvent;
import java.util.List;
import java.util.Set;
import javax.validation.ConstraintViolation;
import javax.validation.ConstraintViolationException;
import javax.validation.Validator;
import org.springframework.boot.autoconfigure.cassandra.CassandraProperties;
import org.springframework.stereotype.Repository;

/**
 * Spring Data Cassandra repository for the {@link TrackysatEvent} entity.
 */
@Repository
public class TrackysatEventRepository {

    private final CqlSession session;

    private final Validator validator;

    private final TrackysatEventDao trackysatEventDao;

    public TrackysatEventRepository(CqlSession session, Validator validator, CassandraProperties cassandraProperties) {
        this.session = session;
        this.validator = validator;
        TrackysatEventTokenMapper trackysatEventTokenMapper = new TrackysatEventTokenMapperBuilder(session).build();
        trackysatEventDao = trackysatEventTokenMapper.trackysatEventTokenDao(CqlIdentifier.fromCql(cassandraProperties.getKeyspaceName()));
    }

    public List<TrackysatEvent> findAll() {
        return trackysatEventDao.findAll().all();
    }

    public TrackysatEvent save(TrackysatEvent trackysatEvent) {
        Set<ConstraintViolation<TrackysatEvent>> violations = validator.validate(trackysatEvent);
        if (violations != null && !violations.isEmpty()) {
            throw new ConstraintViolationException(violations);
        }
        //        TrackysatEvent oldTrackysatEvent = trackysatEventDao.get(trackysatEvent.getCustomerId()).orElse(null);
        BatchStatementBuilder batch = BatchStatement.builder(DefaultBatchType.LOGGED);
        batch.addStatement(trackysatEventDao.saveQuery(trackysatEvent));
        session.execute(batch.build());
        return trackysatEvent;
    }

    public void delete(TrackysatEvent trackysatEvent) {
        BatchStatementBuilder batch = BatchStatement.builder(DefaultBatchType.LOGGED);
        batch.addStatement(trackysatEventDao.deleteQuery(trackysatEvent));
        session.execute(batch.build());
    }
}

@Dao
interface TrackysatEventDao {
    @Select
    PagingIterable<TrackysatEvent> findAll();

    @Insert
    BoundStatement saveQuery(TrackysatEvent trackysatEvent);

    @Delete
    BoundStatement deleteQuery(TrackysatEvent trackysatEvent);
}

@Mapper
interface TrackysatEventTokenMapper {
    @DaoFactory
    TrackysatEventDao trackysatEventTokenDao(@DaoKeyspace CqlIdentifier keyspace);
}
