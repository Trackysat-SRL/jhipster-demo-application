package com.trackysat.kafka.repository;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.mapper.annotations.*;
import com.trackysat.kafka.domain.DailyAggregation;
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
public class DailyAggregationRepository {

    private final Logger log = LoggerFactory.getLogger(DailyAggregationRepository.class);

    private final CqlSession session;

    private final Validator validator;

    private final DailyAggregationDao dailyAggregationDao;

    private final PreparedStatement findOne;

    private final PreparedStatement findAllByDeviceIdAndDates;

    public DailyAggregationRepository(CqlSession session, Validator validator, CassandraProperties cassandraProperties) {
        this.session = session;
        this.validator = validator;
        DailyAggregationTokenMapper dailyAggregationTokenMapper = new DailyAggregationTokenMapperBuilder(session).build();
        dailyAggregationDao =
            dailyAggregationTokenMapper.dailyAggregationTokenDao(CqlIdentifier.fromCql(cassandraProperties.getKeyspaceName()));

        findOne = session.prepare("SELECT device_id, aggregated_date " + "FROM daily_aggregation limit 1");
        findAllByDeviceIdAndDates =
            session.prepare(
                "SELECT * FROM daily_aggregation " +
                "WHERE device_id = :device_id and aggregated_date >= :from_date and aggregated_date < :to_date limit 100"
            );
    }

    public List<DailyAggregation> findOneByDeviceIdAndDateRange(String deviceId, Instant dateFrom, Instant dateTo) {
        BoundStatement stmt = findAllByDeviceIdAndDates
            .bind()
            .setString("device_id", deviceId)
            .setInstant("from_date", dateFrom)
            .setInstant("to_date", dateTo);
        ResultSet rs = session.execute(stmt);
        return rs.all().stream().map(this::fromRow).collect(Collectors.toList());
    }

    // -- CRUD -- //

    public Optional<DailyAggregation> findOne() {
        ResultSet rs = session.execute(findOne.bind());
        return Optional
            .ofNullable(rs.one())
            .flatMap(row -> dailyAggregationDao.get(row.getString("device_id"), row.getInstant("aggregated_date")));
    }

    public Optional<DailyAggregation> findById(String id, Instant date) {
        return dailyAggregationDao.get(id, date);
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

    private DailyAggregation fromRow(Row row) {
        DailyAggregation te = new DailyAggregation();
        te.setDeviceId(row.getString("device_id"));
        te.setAggregatedDate(row.getInstant("aggregated_date"));
        te.setPositions(row.getString("positions"));
        return te;
    }
}

@Dao
interface DailyAggregationDao {
    @Select
    Optional<DailyAggregation> get(String deviceId, Instant aggregatedDate);

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
