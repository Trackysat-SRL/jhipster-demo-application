package com.trackysat.kafka.repository;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.mapper.annotations.*;
import com.trackysat.kafka.domain.DailyAggregation;
import com.trackysat.kafka.domain.DailyAggregationError;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
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
public class DailyAggregationErrorRepository {

    private final Logger log = LoggerFactory.getLogger(DailyAggregationErrorRepository.class);

    private final CqlSession session;

    private final Validator validator;

    private final DailyAggregationErrorDao dailyAggregationErrorDao;

    private final PreparedStatement findOne;

    private final PreparedStatement findAllByDeviceIdAndDates;

    public DailyAggregationErrorRepository(CqlSession session, Validator validator, CassandraProperties cassandraProperties) {
        this.session = session;
        this.validator = validator;
        DailyAggregationErrorTokenMapper dailyAggregationErrorTokenMapper = new DailyAggregationErrorTokenMapperBuilder(session).build();
        dailyAggregationErrorDao =
            dailyAggregationErrorTokenMapper.dailyAggregationErrorTokenDao(CqlIdentifier.fromCql(cassandraProperties.getKeyspaceName()));

        findOne = session.prepare("SELECT device_id, aggregated_date " + "FROM daily_aggregation_error limit 1");
        findAllByDeviceIdAndDates =
            session.prepare(
                "SELECT * FROM daily_aggregation_error " +
                "WHERE device_id = :device_id and aggregated_date >= :from_date and aggregated_date < :to_date"
            );
    }

    public List<DailyAggregationError> findOneByDeviceIdAndDateRange(String deviceId, Instant dateFrom, Instant dateTo) {
        BoundStatement stmt = findAllByDeviceIdAndDates
            .bind()
            .setString("device_id", deviceId)
            .setInstant("from_date", dateFrom)
            .setInstant("to_date", dateTo);
        ResultSet rs = session.execute(stmt.setConsistencyLevel(ConsistencyLevel.LOCAL_ONE));
        return rs.all().stream().map(this::fromRow).collect(Collectors.toList());
    }

    public List<DailyAggregationError> findOneByDateRange(Instant dateFrom, Instant dateTo) {
        BoundStatement stmt = findAllByDeviceIdAndDates.bind().setInstant("from_date", dateFrom).setInstant("to_date", dateTo);
        ResultSet rs = session.execute(stmt.setConsistencyLevel(ConsistencyLevel.LOCAL_ONE));
        return rs.all().stream().map(this::fromRow).collect(Collectors.toList());
    }

    // -- CRUD -- //

    public Optional<DailyAggregationError> findOne() {
        ResultSet rs = session.execute(findOne.bind());
        return Optional
            .ofNullable(rs.one())
            .flatMap(row -> dailyAggregationErrorDao.get(row.getString("device_id"), row.getInstant("aggregated_date")));
    }

    public Optional<DailyAggregationError> findById(String id, Instant date) {
        return dailyAggregationErrorDao.get(id, date);
    }

    public List<DailyAggregationError> findAll() {
        return dailyAggregationErrorDao.findAll().all();
    }

    public DailyAggregationError save(DailyAggregationError dailyAggregation) {
        Set<ConstraintViolation<DailyAggregationError>> violations = validator.validate(dailyAggregation);
        if (violations != null && !violations.isEmpty()) {
            throw new ConstraintViolationException(violations);
        }
        //        DailyAggregation oldDailyAggregation = dailyAggregationDao.get(dailyAggregation.getCustomerId()).orElse(null);
        BatchStatementBuilder batch = BatchStatement.builder(DefaultBatchType.UNLOGGED);
        batch.addStatement(dailyAggregationErrorDao.saveQuery(dailyAggregation));
        session.execute(batch.build());
        return dailyAggregation;
    }

    public void delete(DailyAggregationError dailyAggregation) {
        BatchStatementBuilder batch = BatchStatement.builder(DefaultBatchType.UNLOGGED);
        batch.addStatement(dailyAggregationErrorDao.deleteQuery(dailyAggregation));
        session.execute(batch.build());
    }

    private DailyAggregationError fromRow(Row row) {
        DailyAggregationError te = new DailyAggregationError();
        te.setDeviceId(row.getString("device_id"));
        te.setAggregatedDate(row.getInstant("aggregated_date"));
        te.setError(row.getString("error"));
        return te;
    }
}

@Dao
interface DailyAggregationErrorDao {
    @Select
    Optional<DailyAggregationError> get(String deviceId, Instant aggregatedDate);

    @Select
    PagingIterable<DailyAggregationError> findAll();

    @Insert
    BoundStatement saveQuery(DailyAggregationError dailyAggregation);

    @Delete
    BoundStatement deleteQuery(DailyAggregationError dailyAggregation);
}

@Mapper
interface DailyAggregationErrorTokenMapper {
    @DaoFactory
    DailyAggregationErrorDao dailyAggregationErrorTokenDao(@DaoKeyspace CqlIdentifier keyspace);
}
