package com.trackysat.kafka.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.trackysat.kafka.domain.aggregations.PositionDTO;
import com.trackysat.kafka.service.dto.DailyAggregationDTO;
import com.trackysat.kafka.service.dto.TrackysatEventDTO;
import com.trackysat.kafka.service.mapper.DailyAggregationMapper;
import com.trackysat.kafka.utils.DateUtils;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class AggregationDelegatorService {

    private static final int MAX_DAY_TO_USE_DAILY = 60;

    private final Logger log = LoggerFactory.getLogger(DailyAggregationService.class);

    private final TrackyEventQueryService trackyEventQueryService;

    private final DailyAggregationMapper dailyAggregationMapper;

    private final DailyAggregationService dailyAggregationService;

    private final MonthlyAggregationService monthlyAggregationService;

    public AggregationDelegatorService(
        TrackyEventQueryService trackyEventQueryService,
        DailyAggregationMapper dailyAggregationMapper,
        DailyAggregationService dailyAggregationService,
        MonthlyAggregationService monthlyAggregationService
    ) {
        this.trackyEventQueryService = trackyEventQueryService;
        this.dailyAggregationMapper = dailyAggregationMapper;
        this.dailyAggregationService = dailyAggregationService;
        this.monthlyAggregationService = monthlyAggregationService;
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
            return dailyAggregationService.getByDeviceIdAndDateRange(deviceId, dateFrom, dateTo);
        } else {
            log.info("[{}] Using monthly aggregation data", deviceId);
            return monthlyAggregationService.getByDeviceIdAndDateRange(deviceId, dateFrom, dateTo);
        }
    }

    private List<DailyAggregationDTO> aggregateRealTime(String deviceId, Instant dateFrom, Instant dateTo) throws JsonProcessingException {
        List<TrackysatEventDTO> events = trackyEventQueryService.getByDeviceIdAndDateRange(deviceId, dateFrom, dateTo);
        return List
            .of(dailyAggregationService.buildDailyAggregation(deviceId, dateFrom, events))
            .stream()
            .map(dailyAggregationMapper::toDTO)
            .collect(Collectors.toList());
    }

    public Optional<DailyAggregationDTO> getOne() {
        return dailyAggregationService.getOne().map(dailyAggregationMapper::toDTO);
    }
}
