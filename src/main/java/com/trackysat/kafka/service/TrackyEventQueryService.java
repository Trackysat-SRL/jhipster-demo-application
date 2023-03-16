package com.trackysat.kafka.service;

import com.trackysat.kafka.repository.TrackyEventRepository;
import com.trackysat.kafka.service.dto.TrackysatEventDTO;
import com.trackysat.kafka.service.mapper.TrackysatEventMapper;
import java.time.Instant;
import java.time.LocalDate;
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

    public void processDay(String deviceId, LocalDate day) {
        log.info("Processing day " + day.toString());
    }

    public void processMonth(String deviceId, LocalDate day) {
        log.info("Processing month " + day.toString());
    }
}
