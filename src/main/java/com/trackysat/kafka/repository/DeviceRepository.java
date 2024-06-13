package com.trackysat.kafka.repository;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.mapper.annotations.*;
import com.trackysat.kafka.domain.Device;
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
 * Spring Data Cassandra repository for the {@link Device} entity.
 */
@Repository
public class DeviceRepository {

    private final Logger log = LoggerFactory.getLogger(DeviceRepository.class);

    private final CqlSession session;

    private final Validator validator;

    private final DeviceDao deviceDao;

    public DeviceRepository(CqlSession session, Validator validator, CassandraProperties cassandraProperties) {
        this.session = session;
        this.validator = validator;
        DeviceTokenMapper deviceTokenMapper = new DeviceTokenMapperBuilder(session).build();
        deviceDao = deviceTokenMapper.deviceTokenDao(CqlIdentifier.fromCql(cassandraProperties.getKeyspaceName()));
    }

    // -- CRUD -- //

    public Optional<Device> findById(String id) {
        return deviceDao.get(id);
    }

    public List<Device> findAll() {
        return deviceDao.findAll().all();
    }

    public Device save(Device device) {
        Set<ConstraintViolation<Device>> violations = validator.validate(device);
        if (violations != null && !violations.isEmpty()) {
            throw new ConstraintViolationException(violations);
        }
        //        Device oldDevice = deviceDao.get(device.getCustomerId()).orElse(null);
        BatchStatementBuilder batch = BatchStatement.builder(DefaultBatchType.UNLOGGED);
        batch.addStatement(deviceDao.saveQuery(device));
        session.execute(batch.build());
        return device;
    }

    public void delete(Device device) {
        BatchStatementBuilder batch = BatchStatement.builder(DefaultBatchType.UNLOGGED);
        batch.addStatement(deviceDao.deleteQuery(device));
        session.execute(batch.build());
    }

    public void update(Device device) {
        BatchStatementBuilder batch = BatchStatement.builder(DefaultBatchType.UNLOGGED);
        batch.addStatement(deviceDao.updateQuery(device));
        session.execute(batch.build());
    }
}

@Dao
interface DeviceDao {
    @Select
    Optional<Device> get(String deviceId);

    @Select
    PagingIterable<Device> findAll();

    @Insert
    BoundStatement saveQuery(Device device);

    @Delete
    BoundStatement deleteQuery(Device device);

    @Update
    BoundStatement updateQuery(Device device);
}

@Mapper
interface DeviceTokenMapper {
    @DaoFactory
    DeviceDao deviceTokenDao(@DaoKeyspace CqlIdentifier keyspace);
}
