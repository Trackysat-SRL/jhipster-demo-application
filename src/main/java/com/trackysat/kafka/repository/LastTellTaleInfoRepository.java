package com.trackysat.kafka.repository;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.mapper.annotations.*;
import com.trackysat.kafka.domain.LastTellTaleInfo;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import javax.validation.ConstraintViolation;
import javax.validation.ConstraintViolationException;
import javax.validation.Validator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.cassandra.CassandraProperties;
import org.springframework.data.cassandra.repository.Consistency;
import org.springframework.stereotype.Repository;

/**
 * Spring Data Cassandra repository for the {@link LastTellTaleInfo} entity.
 */
@Repository
public class LastTellTaleInfoRepository {

    private final Logger log = LoggerFactory.getLogger(LastTellTaleInfoRepository.class);

    private final CqlSession session;

    private final Validator validator;

    private final LastTellTaleInfoDao lastTellTaleInfoDao;

    public LastTellTaleInfoRepository(CqlSession session, Validator validator, CassandraProperties cassandraProperties) {
        this.session = session;
        this.validator = validator;
        LastTellTaleInfoTokenMapper tellTaleInfoTokenMapper = new LastTellTaleInfoTokenMapperBuilder(session).build();
        lastTellTaleInfoDao =
            tellTaleInfoTokenMapper.lastTellTaleInfoTokenDao(CqlIdentifier.fromCql(cassandraProperties.getKeyspaceName()));
    }

    // -- CRUD -- //
    public Optional<LastTellTaleInfo> findById(String deviceId, String iid) {
        return lastTellTaleInfoDao.get(deviceId, iid);
    }

    public List<LastTellTaleInfo> findByDeviceId(String deviceId) {
        return lastTellTaleInfoDao.findByDeviceId(deviceId).all();
    }

    public List<LastTellTaleInfo> findByDeviceIdIn(List<String> devices) {
        return lastTellTaleInfoDao.findByDeviceIn(devices).all();
    }

    public List<LastTellTaleInfo> findAll() {
        return lastTellTaleInfoDao.findAll().all();
    }

    public LastTellTaleInfo save(LastTellTaleInfo lastTellTaleInfo) {
        Set<ConstraintViolation<LastTellTaleInfo>> violations = validator.validate(lastTellTaleInfo);
        if (violations != null && !violations.isEmpty()) {
            throw new ConstraintViolationException(violations);
        }
        BatchStatementBuilder batch = BatchStatement.builder(DefaultBatchType.UNLOGGED);
        batch.addStatement(lastTellTaleInfoDao.saveQuery(lastTellTaleInfo));
        session.execute(batch.build());
        return lastTellTaleInfo;
    }

    public boolean delete(LastTellTaleInfo info) {
        BatchStatementBuilder batch = BatchStatement.builder(DefaultBatchType.UNLOGGED);
        batch.addStatement(lastTellTaleInfoDao.deleteQuery(info));
        session.execute(batch.build());
        return true;
    }
}

@Dao
interface LastTellTaleInfoDao {
    @Select
    Optional<LastTellTaleInfo> get(String deviceId, String iid);

    @Select
    PagingIterable<LastTellTaleInfo> findAll();

    @Insert
    BoundStatement saveQuery(LastTellTaleInfo lastTellTaleInfo);

    @Delete
    BoundStatement deleteQuery(LastTellTaleInfo lastTellTaleInfo);

    @Query("select * from last_tell_tale_info where device_id = :deviceId")
    PagingIterable<LastTellTaleInfo> findByDeviceId(String deviceId);

    @Query("select * from last_tell_tale_info where device_id in :devices")
    PagingIterable<LastTellTaleInfo> findByDeviceIn(List<String> devices);
}

@Mapper
interface LastTellTaleInfoTokenMapper {
    @DaoFactory
    LastTellTaleInfoDao lastTellTaleInfoTokenDao(@DaoKeyspace CqlIdentifier keyspace);
}
