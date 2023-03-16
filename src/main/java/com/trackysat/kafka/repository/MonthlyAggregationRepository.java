package com.trackysat.kafka.repository;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.mapper.annotations.*;
import com.trackysat.kafka.domain.MonthlyAggregation;
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
 * Spring Data Cassandra repository for the {@link MonthlyAggregation} entity.
 */
@Repository
public class MonthlyAggregationRepository {

    private final Logger log = LoggerFactory.getLogger(MonthlyAggregationRepository.class);

    private final CqlSession session;

    private final Validator validator;

    private final MonthlyAggregationDao monthlyAggregationDao;

    public MonthlyAggregationRepository(CqlSession session, Validator validator, CassandraProperties cassandraProperties) {
        this.session = session;
        this.validator = validator;
        MonthlyAggregationTokenMapper monthlyAggregationTokenMapper = new MonthlyAggregationTokenMapperBuilder(session).build();
        monthlyAggregationDao =
            monthlyAggregationTokenMapper.monthlyAggregationTokenDao(CqlIdentifier.fromCql(cassandraProperties.getKeyspaceName()));
    }

    // -- CRUD -- //

    public Optional<MonthlyAggregation> findById(String id) {
        return monthlyAggregationDao.get(id);
    }

    public List<MonthlyAggregation> findAll() {
        return monthlyAggregationDao.findAll().all();
    }

    public MonthlyAggregation save(MonthlyAggregation monthlyAggregation) {
        Set<ConstraintViolation<MonthlyAggregation>> violations = validator.validate(monthlyAggregation);
        if (violations != null && !violations.isEmpty()) {
            throw new ConstraintViolationException(violations);
        }
        //        MonthlyAggregation oldMonthlyAggregation = monthlyAggregationDao.get(monthlyAggregation.getCustomerId()).orElse(null);
        BatchStatementBuilder batch = BatchStatement.builder(DefaultBatchType.UNLOGGED);
        batch.addStatement(monthlyAggregationDao.saveQuery(monthlyAggregation));
        session.execute(batch.build());
        return monthlyAggregation;
    }

    public void delete(MonthlyAggregation monthlyAggregation) {
        BatchStatementBuilder batch = BatchStatement.builder(DefaultBatchType.UNLOGGED);
        batch.addStatement(monthlyAggregationDao.deleteQuery(monthlyAggregation));
        session.execute(batch.build());
    }
}

@Dao
interface MonthlyAggregationDao {
    @Select
    Optional<MonthlyAggregation> get(String monthlyAggregationId);

    @Select
    PagingIterable<MonthlyAggregation> findAll();

    @Insert
    BoundStatement saveQuery(MonthlyAggregation monthlyAggregation);

    @Delete
    BoundStatement deleteQuery(MonthlyAggregation monthlyAggregation);
}

@Mapper
interface MonthlyAggregationTokenMapper {
    @DaoFactory
    MonthlyAggregationDao monthlyAggregationTokenDao(@DaoKeyspace CqlIdentifier keyspace);
}
