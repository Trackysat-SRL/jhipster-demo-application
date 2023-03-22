package com.trackysat.kafka.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.trackysat.kafka.domain.DailyAggregation;
import com.trackysat.kafka.domain.TrackyEvent;
import com.trackysat.kafka.domain.aggregations.PositionDTO;
import com.trackysat.kafka.repository.DailyAggregationRepository;
import com.trackysat.kafka.service.dto.TrackysatEventDTO;
import com.trackysat.kafka.service.mapper.TrackysatEventMapper;
import com.trackysat.kafka.utils.JSONUtils;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Service class for managing daily aggregations.
 */
@Service
public class DailyAggregationService {

    private final Logger log = LoggerFactory.getLogger(DailyAggregationService.class);

    private final DailyAggregationRepository dailyAggregationRepository;

    private final TrackysatEventMapper trackysatEventMapper;

    public DailyAggregationService(DailyAggregationRepository dailyAggregationRepository, TrackysatEventMapper trackysatEventMapper) {
        this.dailyAggregationRepository = dailyAggregationRepository;
        this.trackysatEventMapper = trackysatEventMapper;
    }

    public Optional<DailyAggregation> getOne(String id) {
        return dailyAggregationRepository.findById(id);
    }

    public List<DailyAggregation> getAll() {
        return dailyAggregationRepository.findAll();
    }

    public void process(String deviceId, Instant day, List<TrackyEvent> events) throws JsonProcessingException {
        List<TrackysatEventDTO> eventDTOS = events.stream().map(trackysatEventMapper::toTrackysatEventDTO).collect(Collectors.toList());
        DailyAggregation da = new DailyAggregation();
        da.setDeviceId(deviceId);
        da.setTimestamp(day);
        da.setPositions(processPositions(eventDTOS));
        this.dailyAggregationRepository.save(da);
    }

    private String processPositions(List<TrackysatEventDTO> events) throws JsonProcessingException {
        List<PositionDTO> positions = events
            .stream()
            .map(TrackysatEventDTO::getCon)
            .flatMap(List::stream)
            .map(e -> {
                PositionDTO p = new PositionDTO();
                p.setTimestamp(e.getEts().getTst());
                p.setAltitude(e.getSat().getAlt());
                p.setDirection(e.getSat().getDir());
                p.setLatitude(e.getSat().getLat());
                p.setLongitude(e.getSat().getLon());
                p.setFix(e.getSat().getFix());
                p.setNumberOfSatellites(e.getSat().getSnr());
                p.setSignal(e.getSat().getSig());
                p.setTyp(e.getSat().getTyp());
                p.setSpeed(e.getSat().getSpe());
                return p;
            })
            .distinct()
            .collect(Collectors.toList());
        log.debug("Total unique positions {}", positions.size());
        return JSONUtils.toString(positions);
    }
}
