package com.trackysat.kafka.repository;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.api.mapper.annotations.*;
import com.trackysat.kafka.domain.DailyAggregation;
import com.trackysat.kafka.domain.aggregations.PositionDTO;
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
 * Spring Data Cassandra repository for the {@link DailyAggregation} entity.
 */
@Repository
public class DailyAggregationRepository {

    private final Logger log = LoggerFactory.getLogger(DailyAggregationRepository.class);

    private final CqlSession session;

    private final Validator validator;

    private final DailyAggregationDao dailyAggregationDao;

    public DailyAggregationRepository(CqlSession session, Validator validator, CassandraProperties cassandraProperties) {
        this.session = session;
        this.validator = validator;
        DailyAggregationTokenMapper dailyAggregationTokenMapper = new DailyAggregationTokenMapperBuilder(session).build();
        dailyAggregationDao =
            dailyAggregationTokenMapper.dailyAggregationTokenDao(CqlIdentifier.fromCql(cassandraProperties.getKeyspaceName()));
    }

    // -- CRUD -- //

    public Optional<DailyAggregation> findById(String id) {
        return dailyAggregationDao.get(id);
    }

    public List<DailyAggregation> findAll() {
        return dailyAggregationDao.findAll().all();
    }

    public DailyAggregation save(DailyAggregation dailyAggregation) {
        Set<ConstraintViolation<DailyAggregation>> violations = validator.validate(dailyAggregation);
        if (violations != null && !violations.isEmpty()) {
            throw new ConstraintViolationException(violations);
        }
        //        DailyAggregation oldDailyAggregation = dailyAggregationDao.get(dailyAggregation.getCustomerId()).orElse(null);
        BatchStatementBuilder batch = BatchStatement.builder(DefaultBatchType.UNLOGGED);
        batch.addStatement(dailyAggregationDao.saveQuery(dailyAggregation));
        session.execute(batch.build());
        return dailyAggregation;
    }

    public void delete(DailyAggregation dailyAggregation) {
        BatchStatementBuilder batch = BatchStatement.builder(DefaultBatchType.UNLOGGED);
        batch.addStatement(dailyAggregationDao.deleteQuery(dailyAggregation));
        session.execute(batch.build());
    }
}

@Dao
interface DailyAggregationDao {
    @Select
    Optional<DailyAggregation> get(String dailyAggregationId);

    @Select
    PagingIterable<DailyAggregation> findAll();

    @Insert
    BoundStatement saveQuery(DailyAggregation dailyAggregation);

    @Delete
    BoundStatement deleteQuery(DailyAggregation dailyAggregation);
}

@Mapper
interface DailyAggregationTokenMapper {
    @DaoFactory
    DailyAggregationDao dailyAggregationTokenDao(@DaoKeyspace CqlIdentifier keyspace);
}
