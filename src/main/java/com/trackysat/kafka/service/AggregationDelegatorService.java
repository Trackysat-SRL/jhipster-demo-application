package com.trackysat.kafka.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.trackysat.kafka.domain.aggregations.PositionDTO;
import com.trackysat.kafka.domain.aggregations.SensorStatsDTO;
import com.trackysat.kafka.service.dto.DailyAggregationDTO;
import com.trackysat.kafka.service.dto.TrackysatEventDTO;
import com.trackysat.kafka.service.mapper.DailyAggregationMapper;
import com.trackysat.kafka.utils.DateUtils;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class AggregationDelegatorService {

    private static final int MAX_DAY_TO_USE_DAILY = 60;

    private final Logger log = LoggerFactory.getLogger(DailyAggregationService.class);

    private final DailyAggregationMapper dailyAggregationMapper;

    private final TrackyEventQueryService trackyEventQueryService;

    private final DailyAggregationService dailyAggregationService;

    private final MonthlyAggregationService monthlyAggregationService;

    private final JobStatusService jobStatusService;

    public AggregationDelegatorService(
        TrackyEventQueryService trackyEventQueryService,
        DailyAggregationMapper dailyAggregationMapper,
        DailyAggregationService dailyAggregationService,
        MonthlyAggregationService monthlyAggregationService,
        JobStatusService jobStatusService
    ) {
        this.trackyEventQueryService = trackyEventQueryService;
        this.dailyAggregationMapper = dailyAggregationMapper;
        this.dailyAggregationService = dailyAggregationService;
        this.monthlyAggregationService = monthlyAggregationService;
        this.jobStatusService = jobStatusService;
    }

    public List<PositionDTO> getPositionsByDeviceIdAndDateRange(String deviceId, Instant dateFrom, Instant dateTo)
        throws JsonProcessingException {
        return getByDeviceIdAndDateRange(deviceId, dateFrom, dateTo)
            .stream()
            .map(DailyAggregationDTO::getPositions)
            .flatMap(List::stream)
            .collect(Collectors.toList());
    }

    public Map<String, SensorStatsDTO> getSensorsByDeviceIdAndDateRange(String deviceId, Instant dateFrom, Instant dateTo)
        throws JsonProcessingException {
        return getByDeviceIdAndDateRange(deviceId, dateFrom, dateTo)
            .stream()
            .map(DailyAggregationDTO::getSensors)
            .map(Map::entrySet)
            .flatMap(Set::stream)
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    Map.Entry::getValue,
                    (a, b) -> {
                        a.setValues(Stream.concat(a.getValues().stream(), b.getValues().stream()).distinct().collect(Collectors.toList()));
                        return a;
                    }
                )
            );
    }

    public List<DailyAggregationDTO> getByDeviceIdAndDateRange(String deviceId, Instant dateFrom, Instant dateTo)
        throws JsonProcessingException {
        List<LocalDate> days = DateUtils.getDaysBetweenIncludeStart(dateFrom, dateTo);
        if (days.size() == 0) {
            if (dateFrom.isAfter(LocalDate.now().atStartOfDay().toInstant(ZoneOffset.UTC))) {
                log.info("[{}] Using real time data", deviceId);
                return this.aggregateRealTime(deviceId, dateFrom, dateTo);
            } else {
                log.info("[{}] Using daily aggregation data filter by hours", deviceId);
                List<DailyAggregationDTO> dateData = dailyAggregationService.getByDeviceIdAndDateRange(
                    deviceId,
                    DateUtils.atStartOfDate(dateFrom),
                    dateTo
                );
                List<PositionDTO> positions = dateData
                    .stream()
                    .map(DailyAggregationDTO::getPositions)
                    .flatMap(List::stream)
                    .filter(positionDTO -> positionDTO.getTimestamp().isAfter(dateFrom) && positionDTO.getTimestamp().isBefore(dateTo))
                    .collect(Collectors.toList());
                return dateData
                    .stream()
                    .findFirst()
                    .map(da -> {
                        da.setPositions(positions);
                        return da;
                    })
                    .map(List::of)
                    .orElse(new ArrayList<>());
            }
        } else if (days.size() < MAX_DAY_TO_USE_DAILY) {
            log.info("[{}] Using daily aggregation data", deviceId);
            return dailyAggregationService.getByDeviceIdAndDateRange(deviceId, DateUtils.atStartOfDate(dateFrom), dateTo);
        } else {
            log.info("[{}] Using monthly aggregation data", deviceId);
            return monthlyAggregationService.getByDeviceIdAndDateRange(deviceId, DateUtils.atStartOfDate(dateFrom), dateTo);
        }
    }

    private List<DailyAggregationDTO> aggregateRealTime(String deviceId, Instant dateFrom, Instant dateTo) throws JsonProcessingException {
        List<TrackysatEventDTO> events = trackyEventQueryService.getByDeviceIdAndDateRange(deviceId, dateFrom, dateTo);
        return Stream
            .of(dailyAggregationService.buildDailyAggregation(deviceId, dateFrom, events))
            .map(dailyAggregationMapper::toDTO)
            .collect(Collectors.toList());
    }

    public Optional<DailyAggregationDTO> getOne() {
        return dailyAggregationService.getOne().map(dailyAggregationMapper::toDTO);
    }

    // == PROCESS ==

    public void dailyProcess(String deviceId) {
        Instant startDate = Instant.now();
        log.info("[{}] Started at " + startDate.toString(), deviceId);
        Optional<Instant> lastDay = jobStatusService.getLastDayProcessed(deviceId);
        if (lastDay.isEmpty()) {
            log.debug("[{}] Last day was not present, set to yesterday", deviceId);
        } else {
            log.debug("[{}] Last day processed: " + lastDay, deviceId);
        }
        List<LocalDate> days = DateUtils.getDaysBetween(lastDay.orElse(DateUtils.twoDaysAgo()), startDate);
        log.debug("[{}] Days to process: " + days.size(), deviceId);
        for (LocalDate d : days) {
            try {
                log.debug("[{}] Started processing day " + d.toString(), deviceId);
                trackyEventQueryService.processDay(deviceId, d);
                log.debug("[{}] Finished processing day " + d, deviceId);
                jobStatusService.setLastDayProcessed(deviceId, d, null);
            } catch (Exception e) {
                log.error("[{}] [{}] Error processing day. ERROR: {}", deviceId, d, e.getMessage());
            }
        }
        Instant endDate = Instant.now();
        log.info("[{}] Finished at {}, in {}ms", deviceId, endDate.toString(), endDate.toEpochMilli() - startDate.toEpochMilli());
    }

    public void monthlyProcess(String deviceId) {
        Instant startDate = Instant.now();
        log.info("[{}] Started at " + startDate.toString(), deviceId);
        Optional<Instant> lastDay = jobStatusService.getLastMonthProcessed(deviceId);
        if (lastDay.isEmpty()) {
            log.debug("[{}] Last month was not present, set to last month", deviceId);
        } else {
            log.debug("[{}] Last month processed: " + lastDay, deviceId);
        }
        List<LocalDate> months = DateUtils.getMonthsBetween(lastDay.orElse(DateUtils.twoMonthAgo()), startDate);
        log.debug("[{}] Months to process: " + months.size(), deviceId);
        for (LocalDate d : months) {
            try {
                log.debug("[{}] Started processing month " + d.getMonth().toString(), deviceId);
                trackyEventQueryService.processMonth(deviceId, d);
                log.debug("[{}] Finished processing month " + d.getMonth().toString(), deviceId);
                jobStatusService.setLastMonthProcessed(deviceId, d, null);
            } catch (Exception e) {
                log.error("[{}] [{}] Error processing month. ERROR: {}", deviceId, d.getMonth(), e.getMessage());
            }
        }
        Instant endDate = Instant.now();
        log.info("[{}] Finished at {}, in {}ms", deviceId, endDate.toString(), endDate.toEpochMilli() - startDate.toEpochMilli());
    }
}
