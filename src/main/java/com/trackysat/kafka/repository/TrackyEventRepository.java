package com.trackysat.kafka.repository;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.mapper.annotations.*;
import com.trackysat.kafka.domain.TrackyEvent;
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
 * Spring Data Cassandra repository for the {@link TrackyEvent} entity.
 */
@Repository
public class TrackyEventRepository {

    private final Logger log = LoggerFactory.getLogger(TrackyEventRepository.class);

    private final CqlSession session;

    private final Validator validator;

    private final TrackyEventDao trackysatEventDao;

    private final PreparedStatement findOne;

    private final PreparedStatement findAllByDeviceIdAndDates;

    public TrackyEventRepository(CqlSession session, Validator validator, CassandraProperties cassandraProperties) {
        this.session = session;
        this.validator = validator;
        TrackyEventTokenMapper trackysatEventTokenMapper = new TrackyEventTokenMapperBuilder(session).build();
        trackysatEventDao = trackysatEventTokenMapper.trackysatEventTokenDao(CqlIdentifier.fromCql(cassandraProperties.getKeyspaceName()));

        findOne = session.prepare("SELECT device_id, created_date " + "FROM tracky_event limit 1");

        findAllByDeviceIdAndDates =
            session.prepare(
                "SELECT * FROM tracky_event " + "WHERE device_id = :device_id and created_date >= :from_date and created_date < :to_date"
            );
    }

    // -- Queries -- //

    public Optional<TrackyEvent> findOne() {
        ResultSet rs = session.execute(findOne.bind());
        return Optional
            .ofNullable(rs.one())
            .flatMap(row -> trackysatEventDao.get(row.get("device_id", String.class), row.get("created_date", Instant.class)));
    }

    public List<TrackyEvent> findEventsFromDate(String deviceId, Instant date) {
        return trackysatEventDao.getFromInstant(deviceId, date).all();
    }

    public List<TrackyEvent> findOneByDeviceIdAndDateRange(String deviceId, Instant dateFrom, Instant dateTo) {
        log.info("Start query findOneByDeviceIdAndDateRange device_id " + deviceId + " from_date " + dateFrom + " to_date " + dateTo);
        BoundStatement stmt = findAllByDeviceIdAndDates
            .bind()
            .setString("device_id", deviceId)
            .setInstant("from_date", dateFrom)
            .setInstant("to_date", dateTo);
        ResultSet rs = session.execute(stmt);
        List<Row> listrow = rs.all();
        log.info(
            "End query findOneByDeviceIdAndDateRange device_id " +
            deviceId +
            " from_date " +
            dateFrom +
            " to_date " +
            dateTo +
            "tot row " +
            listrow.size()
        );
        return listrow.stream().map(this::fromRow).collect(Collectors.toList());
    }

    public void deleteEventByDate(String deviceId, Instant dateFrom, Instant dateTo) {
        log.info("Start deleteEventByDate device_id " + deviceId + " from_date " + dateFrom + " to_date " + dateTo);
        BoundStatement stmt = findAllByDeviceIdAndDates
            .bind()
            .setString("device_id", deviceId)
            .setInstant("from_date", dateFrom)
            .setInstant("to_date", dateTo);
        ResultSet rs = session.execute(stmt);
        List<Row> listrow = rs.all();
        List<TrackyEvent> trackyEventList = listrow.stream().map(this::fromRow).collect(Collectors.toList());
        trackyEventList.forEach(this::delete);
        log.info("End deleteEventByDate device_id " + deviceId + " from_date " + dateFrom + " to_date " + dateTo);
    }

    // -- CRUD -- //

    public Optional<TrackyEvent> findById(String id, Instant date) {
        return trackysatEventDao.get(id, date);
    }

    public List<TrackyEvent> findAll() {
        return trackysatEventDao.findAll().all();
    }

    public TrackyEvent save(TrackyEvent trackysatEvent) {
        Set<ConstraintViolation<TrackyEvent>> violations = validator.validate(trackysatEvent);
        if (violations != null && !violations.isEmpty()) {
            throw new ConstraintViolationException(violations);
        }
        //        TrackyEvent oldTrackyEvent = trackysatEventDao.get(trackysatEvent.getCustomerId()).orElse(null);
        BatchStatementBuilder batch = BatchStatement.builder(DefaultBatchType.UNLOGGED);
        batch.addStatement(trackysatEventDao.saveQuery(trackysatEvent));
        session.execute(batch.build());
        return trackysatEvent;
    }

    public void delete(TrackyEvent trackysatEvent) {
        BatchStatementBuilder batch = BatchStatement.builder(DefaultBatchType.UNLOGGED);
        batch.addStatement(trackysatEventDao.deleteQuery(trackysatEvent));
        session.execute(batch.build());
    }

    private TrackyEvent fromRow(Row row) {
        TrackyEvent te = new TrackyEvent();
        String devId = row.getString("device_id");
        te.setDeviceId(devId);
        te.setCreatedDate(row.getInstant("created_date"));
        te.setEventDate(row.getInstant("event_date"));
        te.setUid(row.getString("uid"));
        te.setVer(row.getString("ver"));
        te.setDes(row.getString("des"));
        te.setEts(row.getString("ets"));
        te.setOri(row.getString("ori"));
        te.setCon(row.getString("con"));
        return te;
    }
}

@Dao
interface TrackyEventDao {
    @Select
    Optional<TrackyEvent> get(String deviceId, Instant createdDate);

    @Select
    PagingIterable<TrackyEvent> findAll();

    @Insert
    BoundStatement saveQuery(TrackyEvent trackysatEvent);

    @Delete
    BoundStatement deleteQuery(TrackyEvent trackysatEvent);

    @Query("select * from tracky_event where device_id = :deviceId and created_date >= :instant")
    PagingIterable<TrackyEvent> getFromInstant(String deviceId, Instant instant);
}

@Mapper
interface TrackyEventTokenMapper {
    @DaoFactory
    TrackyEventDao trackysatEventTokenDao(@DaoKeyspace CqlIdentifier keyspace);
}
