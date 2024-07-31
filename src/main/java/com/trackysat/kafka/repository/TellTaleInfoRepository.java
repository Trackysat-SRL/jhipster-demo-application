package com.trackysat.kafka.repository;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.mapper.annotations.*;
import com.trackysat.kafka.domain.TellTaleInfo;
import java.time.Instant;
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
 * Spring Data Cassandra repository for the {@link TellTaleInfo} entity.
 */
@Repository
public class TellTaleInfoRepository {

    private final Logger log = LoggerFactory.getLogger(TellTaleInfoRepository.class);

    private final CqlSession session;

    private final Validator validator;

    private final TellTaleInfoDao tellTaleInfoDao;

    public TellTaleInfoRepository(CqlSession session, Validator validator, CassandraProperties cassandraProperties) {
        this.session = session;
        this.validator = validator;
        TellTaleInfoTokenMapper tellTaleInfoTokenMapper = new TellTaleInfoTokenMapperBuilder(session).build();
        tellTaleInfoDao = tellTaleInfoTokenMapper.tellTaleInfoTokenDao(CqlIdentifier.fromCql(cassandraProperties.getKeyspaceName()));
    }

    // -- CRUD -- //
    public Optional<TellTaleInfo> findById(String deviceId, Instant createdDate) {
        return tellTaleInfoDao.get(deviceId, createdDate);
    }

    public List<TellTaleInfo> findByDeviceIdAndRangeDate(String deviceId, Instant from, Instant to) {
        return tellTaleInfoDao.findByDeviceIdAndRangeDate(deviceId, from, to).all();
    }

    public List<TellTaleInfo> findAll() {
        return tellTaleInfoDao.findAll().all();
    }

    public TellTaleInfo save(TellTaleInfo tellTaleInfo) {
        Set<ConstraintViolation<TellTaleInfo>> violations = validator.validate(tellTaleInfo);
        if (violations != null && !violations.isEmpty()) {
            throw new ConstraintViolationException(violations);
        }
        BatchStatementBuilder batch = BatchStatement.builder(DefaultBatchType.UNLOGGED);
        batch.addStatement(tellTaleInfoDao.saveQuery(tellTaleInfo));
        session.execute(batch.build());
        return tellTaleInfo;
    }

    public boolean delete(TellTaleInfo info) {
        BatchStatementBuilder batch = BatchStatement.builder(DefaultBatchType.UNLOGGED);
        batch.addStatement(tellTaleInfoDao.deleteQuery(info));
        session.execute(batch.build());
        return true;
    }
}

@Dao
interface TellTaleInfoDao {
    @Select
    Optional<TellTaleInfo> get(String deviceId, Instant createdDate);

    @Select
    PagingIterable<TellTaleInfo> findAll();

    @Insert
    BoundStatement saveQuery(TellTaleInfo tellTaleInfo);

    @Delete
    BoundStatement deleteQuery(TellTaleInfo tellTaleInfo);

    @Query(
        "select * from tell_tale_info where device_id = :deviceId and event_created_date >= :fromDate and event_created_date <= :toDate order by event_created_date desc"
    )
    PagingIterable<TellTaleInfo> findByDeviceIdAndRangeDate(String deviceId, Instant fromDate, Instant toDate);
}

@Mapper
interface TellTaleInfoTokenMapper {
    @DaoFactory
    TellTaleInfoDao tellTaleInfoTokenDao(@DaoKeyspace CqlIdentifier keyspace);
}
