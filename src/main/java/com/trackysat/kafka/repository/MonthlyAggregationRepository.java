package com.trackysat.kafka.repository;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.mapper.annotations.*;
import com.trackysat.kafka.domain.MonthlyAggregation;
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
 * Spring Data Cassandra repository for the {@link MonthlyAggregation} entity.
 */
@Repository
public class MonthlyAggregationRepository {

    private final Logger log = LoggerFactory.getLogger(MonthlyAggregationRepository.class);

    private final CqlSession session;

    private final Validator validator;

    private final MonthlyAggregationDao monthlyAggregationDao;

    private final PreparedStatement findOne;

    private final PreparedStatement findAllByDeviceIdAndDates;

    public MonthlyAggregationRepository(CqlSession session, Validator validator, CassandraProperties cassandraProperties) {
        this.session = session;
        this.validator = validator;
        MonthlyAggregationTokenMapper monthlyAggregationTokenMapper = new MonthlyAggregationTokenMapperBuilder(session).build();
        monthlyAggregationDao =
            monthlyAggregationTokenMapper.monthlyAggregationTokenDao(CqlIdentifier.fromCql(cassandraProperties.getKeyspaceName()));

        findOne = session.prepare("SELECT device_id, aggregated_date " + "FROM monthly_aggregation limit 1");
        findAllByDeviceIdAndDates =
            session.prepare(
                "SELECT * FROM monthly_aggregation " +
                "WHERE device_id = :device_id and aggregated_date >= :from_date and aggregated_date < :to_date limit 100"
            );
    }

    public List<MonthlyAggregation> findOneByDeviceIdAndDateRange(String deviceId, Instant dateFrom, Instant dateTo) {
        BoundStatement stmt = findAllByDeviceIdAndDates
            .bind()
            .setString("device_id", deviceId)
            .setInstant("from_date", dateFrom)
            .setInstant("to_date", dateTo);
        ResultSet rs = session.execute(stmt);
        return rs.all().stream().map(this::fromRow).collect(Collectors.toList());
    }

    // -- CRUD -- //

    public Optional<MonthlyAggregation> findOne() {
        ResultSet rs = session.execute(findOne.bind());
        return Optional
            .ofNullable(rs.one())
            .flatMap(row -> monthlyAggregationDao.get(row.getString("device_id"), row.getInstant("aggregated_date")));
    }

    public Optional<MonthlyAggregation> findById(String id, Instant date) {
        return monthlyAggregationDao.get(id, date);
    }

    public List<MonthlyAggregation> findAll() {
        return monthlyAggregationDao.findAll().all();
    }

    public MonthlyAggregation save(MonthlyAggregation monthlyAggregation) {
        Set<ConstraintViolation<MonthlyAggregation>> violations = validator.validate(monthlyAggregation);
        if (violations != null && !violations.isEmpty()) {
            throw new ConstraintViolationException(violations);
        }
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

    private MonthlyAggregation fromRow(Row row) {
        MonthlyAggregation te = new MonthlyAggregation();
        te.setDeviceId(row.getString("device_id"));
        te.setAggregatedDate(row.getInstant("aggregated_date"));
        te.setPositions(row.getString("positions"));
        return te;
    }
}

@Dao
interface MonthlyAggregationDao {
    @Select
    Optional<MonthlyAggregation> get(String deviceId, Instant aggregatedDate);

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
