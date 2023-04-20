package com.trackysat.kafka.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.trackysat.kafka.domain.TrackyEvent;
import com.trackysat.kafka.repository.TrackyEventRepository;
import com.trackysat.kafka.service.dto.DailyAggregationDTO;
import com.trackysat.kafka.service.dto.TrackysatEventDTO;
import com.trackysat.kafka.service.mapper.TrackysatEventMapper;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Service class for managing tracky events.
 */
@Service
public class TrackyEventQueryService {

    private final Logger log = LoggerFactory.getLogger(TrackyEventQueryService.class);

    private final TrackyEventRepository trackyEventRepository;

    private final TrackysatEventMapper trackysatEventMapper;

    private final DailyAggregationService dailyAggregationService;

    private final MonthlyAggregationService monthlyAggregationService;

    public TrackyEventQueryService(
        TrackyEventRepository trackyEventRepository,
        TrackysatEventMapper trackysatEventMapper,
        DeviceService deviceService,
        DailyAggregationService dailyAggregationService,
        MonthlyAggregationService monthlyAggregationService
    ) {
        this.trackyEventRepository = trackyEventRepository;
        this.trackysatEventMapper = trackysatEventMapper;
        this.dailyAggregationService = dailyAggregationService;
        this.monthlyAggregationService = monthlyAggregationService;
    }

    public Optional<TrackysatEventDTO> getOne() {
        return trackyEventRepository.findOne().map(trackysatEventMapper::toTrackysatEventDTO);
    }

    public List<TrackysatEventDTO> getByDeviceIdAndDateRange(String deviceId, Instant dateFrom, Instant dateTo) {
        return trackyEventRepository
            .findOneByDeviceIdAndDateRange(deviceId, dateFrom, dateTo)
            .stream()
            .map(trackysatEventMapper::toTrackysatEventDTO)
            .collect(Collectors.toList());
    }

    public void processDay(String deviceId, LocalDate day) throws JsonProcessingException {
        log.info("[{}] [{}] Start processing day", deviceId, day);
        Instant startOfDay = day.atStartOfDay().toInstant(ZoneOffset.UTC);
        Instant endOfDay = day.atStartOfDay().toInstant(ZoneOffset.UTC).plus(1, ChronoUnit.DAYS);
        List<TrackyEvent> events = trackyEventRepository.findOneByDeviceIdAndDateRange(deviceId, startOfDay, endOfDay);
        log.info("[{}] [{}] Events found: " + events.size(), deviceId, day);
        dailyAggregationService.process(deviceId, startOfDay, events);
        log.info("[{}] [{}] Finished processing day", deviceId, day);
    }

    public void processMonth(String deviceId, LocalDate month) throws JsonProcessingException {
        Instant startOfFirstDay = month.withDayOfMonth(1).atStartOfDay().toInstant(ZoneOffset.UTC);
        Instant endOfLastDay = month
            .withDayOfMonth(month.getMonth().length(month.isLeapYear()))
            .atStartOfDay()
            .toInstant(ZoneOffset.UTC)
            .plus(1, ChronoUnit.DAYS);
        List<DailyAggregationDTO> events = dailyAggregationService.getByDeviceIdAndDateRange(deviceId, startOfFirstDay, endOfLastDay);
        log.info("[{}] [{}] Events found: " + events.size(), deviceId, month.getMonth());
        monthlyAggregationService.process(deviceId, startOfFirstDay, events);
    }
}
