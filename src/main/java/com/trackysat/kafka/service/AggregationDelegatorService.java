package com.trackysat.kafka.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.trackysat.kafka.domain.aggregations.PositionDTO;
import com.trackysat.kafka.domain.aggregations.SensorStatsDTO;
import com.trackysat.kafka.domain.aggregations.SensorValDTO;
import com.trackysat.kafka.service.dto.DailyAggregationDTO;
import com.trackysat.kafka.service.dto.TrackysatEventDTO;
import com.trackysat.kafka.service.mapper.DailyAggregationMapper;
import com.trackysat.kafka.utils.DateUtils;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class AggregationDelegatorService {

    private static final int MAX_DAY_TO_USE_DAILY = 31;

    private final Logger log = LoggerFactory.getLogger(AggregationDelegatorService.class);

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
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, dailyAggregationService::mergeSensorMaps));
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
                List<DailyAggregationDTO> dateData = dailyAggregationService.getByDeviceIdAndDateRange(deviceId, dateFrom, dateTo);
                return dateData
                    .stream()
                    .findFirst()
                    .map(dateDataForDay -> filterHoursInDailyAggregation(dateDataForDay, dateFrom, dateTo))
                    .map(List::of)
                    .orElse(new ArrayList<>());
            }
        } /* else if (days.size() < MAX_DAY_TO_USE_DAILY) {
            log.info("[{}] Using daily aggregation data", deviceId);
            return dailyAggregationService
                .getByDeviceIdAndDateRange(deviceId, dateFrom, dateTo)
                .stream()
                .map(da -> filterHoursInDailyAggregation(da, dateFrom, dateTo))
                .collect(Collectors.toList());
        } */else {
            log.info("[{}] Using monthly aggregation data", deviceId);
            return monthlyAggregationService.getByDeviceIdAndDateRange(deviceId, DateUtils.atStartOfDate(dateFrom), dateTo);
        }
    }

    private DailyAggregationDTO filterHoursInDailyAggregation(DailyAggregationDTO dailyAggregationDTO, Instant dateFrom, Instant dateTo) {
        List<PositionDTO> filteredPositions = dailyAggregationDTO
            .getPositions()
            .stream()
            .filter(positionDTO -> positionDTO.getTimestamp().isAfter(dateFrom) && positionDTO.getTimestamp().isBefore(dateTo))
            .collect(Collectors.toList());
        dailyAggregationDTO.setPositions(filteredPositions);
        for (Map.Entry<String, SensorStatsDTO> sensor : dailyAggregationDTO.getSensors().entrySet()) {
            List<SensorValDTO> filteredValues = sensor
                .getValue()
                .getValues()
                .stream()
                .filter(v -> v.getCreationDate().isAfter(dateFrom) && v.getCreationDate().isBefore(dateTo))
                .collect(Collectors.toList());
            sensor.getValue().setValues(filteredValues);
            sensor.getValue().recalculate();
        }
        return dailyAggregationDTO;
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
        log.info("[{}] Started dailyProcess at " + startDate.toString(), deviceId);
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
        log.info("[{}] Finished dailyProcess at {}, in {}ms", deviceId, endDate.toString(), endDate.toEpochMilli() - startDate.toEpochMilli());
    }

    public void monthlyProcess(String deviceId) {
        Instant startDate = Instant.now();
        log.info("[{}] Started monthlyProcess at " + startDate.toString(), deviceId);
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

                Instant startOfFirstDay = d.withDayOfMonth(1).atStartOfDay().toInstant(ZoneOffset.UTC);
                Instant endOfLastDay = DateUtils.atEndOfDate(d.atStartOfDay().toInstant(ZoneOffset.UTC));

                trackyEventQueryService.processMonth(deviceId, d, startOfFirstDay, endOfLastDay);
                log.debug("[{}] Finished processing month " + d.getMonth().toString(), deviceId);
                jobStatusService.setLastMonthProcessed(deviceId, d, null);
            } catch (Exception e) {
                log.error("[{}] [{}] Error processing month. ERROR: {}", deviceId, d.getMonth(), e.getMessage());
            }
        }
        Instant endDate = Instant.now();
        log.info("[{}] Finished monthlyProcess at {}, in {}ms", deviceId, endDate.toString(), endDate.toEpochMilli() - startDate.toEpochMilli());
    }

    public List<SensorStatsDTO> getSensorsSummaryByDeviceIdAndDateRange(
        List<String> ids,
        Instant fromDate,
        Instant toDate,
        Optional<String> nameFilter
    ) {
        List<SensorStatsDTO> summary = new ArrayList<>();
        Map<String, List<SensorStatsDTO>> mapOfSensors = new HashMap<>();
        Map<String, List<SensorStatsDTO>> allDevicesSensors = ids
            .stream()
            .distinct()
            .collect(Collectors.toList())
            .parallelStream()
            .collect(
                Collectors.toMap(
                    Function.identity(),
                    id -> {
                        try {
                            Map<String, SensorStatsDTO> sensors = this.getSensorsByDeviceIdAndDateRange(id, fromDate, toDate);
                            List<SensorStatsDTO> sensorList = new ArrayList<>();
                            for (Map.Entry<String, SensorStatsDTO> s : sensors.entrySet()) {
                                if (nameFilter.isEmpty() || s.getKey().toLowerCase().contains(nameFilter.get())) {
                                    sensorList.add(s.getValue());
                                }
                            }
                            return sensorList;
                        } catch (JsonProcessingException e) {
                            return Collections.emptyList();
                        }
                    }
                )
            );
        for (Map.Entry<String, List<SensorStatsDTO>> sensorByDevice : allDevicesSensors.entrySet()) {
            List<SensorValDTO> filteredValues = sensorByDevice
                .getValue()
                .stream()
                .map(SensorStatsDTO::getValues)
                .flatMap(List::stream)
                .collect(Collectors.toList());
            sensorByDevice
                .getValue()
                .stream()
                .findFirst()
                .ifPresent(s -> {
                    s.setValues(filteredValues);
                    s.recalculate();
                    String key = String.format("%s_%s_%s_%s_%s", s.getSource(), s.getName(), s.getMeasureUnit(), s.getType(), s.getSid());
                    List<SensorStatsDTO> val = mapOfSensors.containsKey(key) ? mapOfSensors.get(key) : new ArrayList<>();
                    val.add(s);
                    mapOfSensors.put(key, val);
                });
        }
        for (Map.Entry<String, List<SensorStatsDTO>> sensorByName : mapOfSensors.entrySet()) {
            summary.add(sensorByName.getValue().stream().reduce(new SensorStatsDTO(), this::mergeSensors));
        }
        return summary;
    }

    public SensorStatsDTO mergeSensors(SensorStatsDTO a, SensorStatsDTO b) {
        try {
            if (b.getFirstValue().getCreationDate().getEpochSecond() < a.getFirstValue().getCreationDate().getEpochSecond()) {
                a.setFirstValue(b.getFirstValue());
            }
            if (b.getLastValue().getCreationDate().getEpochSecond() > a.getLastValue().getCreationDate().getEpochSecond()) {
                a.setLastValue(b.getLastValue());
            }
            a.setSum(a.getSum() + b.getSum());
            if (b.getMin() < a.getMin()) {
                a.setMin(b.getMin());
            }
            if (b.getMax() < a.getMax()) {
                a.setMax(b.getMax());
            }
            a.setAvg((b.getAvg() + a.getAvg()) / 2);
            a.setDiff(a.getDiff() + b.getDiff());
            return a;
        } catch (Exception e) {
            return b;
        }
    }
}
