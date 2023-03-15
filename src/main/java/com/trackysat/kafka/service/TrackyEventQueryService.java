package com.trackysat.kafka.service;

import com.trackysat.kafka.repository.TrackyEventRepository;
import com.trackysat.kafka.service.dto.TrackysatEventDTO;
import com.trackysat.kafka.service.mapper.TrackysatEventMapper;
import java.time.Instant;
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

    public TrackyEventQueryService(TrackyEventRepository trackyEventRepository, TrackysatEventMapper trackysatEventMapper) {
        this.trackyEventRepository = trackyEventRepository;
        this.trackysatEventMapper = trackysatEventMapper;
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
}
