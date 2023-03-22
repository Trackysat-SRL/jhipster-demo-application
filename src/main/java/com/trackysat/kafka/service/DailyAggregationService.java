package com.trackysat.kafka.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.trackysat.kafka.domain.DailyAggregation;
import com.trackysat.kafka.domain.TrackyEvent;
import com.trackysat.kafka.domain.aggregations.PositionDTO;
import com.trackysat.kafka.domain.aggregations.SensorStatsDTO;
import com.trackysat.kafka.domain.vmson.VmsonCon;
import com.trackysat.kafka.repository.DailyAggregationRepository;
import com.trackysat.kafka.service.dto.DailyAggregationDTO;
import com.trackysat.kafka.service.dto.TrackysatEventDTO;
import com.trackysat.kafka.service.mapper.DailyAggregationMapper;
import com.trackysat.kafka.service.mapper.TrackysatEventMapper;
import com.trackysat.kafka.utils.JSONUtils;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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

    private final DailyAggregationMapper dailyAggregationMapper;

    public DailyAggregationService(
        DailyAggregationRepository dailyAggregationRepository,
        TrackysatEventMapper trackysatEventMapper,
        DailyAggregationMapper dailyAggregationMapper
    ) {
        this.dailyAggregationRepository = dailyAggregationRepository;
        this.trackysatEventMapper = trackysatEventMapper;
        this.dailyAggregationMapper = dailyAggregationMapper;
    }

    public Optional<DailyAggregation> getOne() {
        return dailyAggregationRepository.findOne();
    }

    public List<DailyAggregation> getAll() {
        return dailyAggregationRepository.findAll();
    }

    public DailyAggregation buildDailyAggregation(String deviceId, Instant day, List<TrackysatEventDTO> events)
        throws JsonProcessingException {
        DailyAggregation da = new DailyAggregation();
        da.setDeviceId(deviceId);
        da.setAggregatedDate(day);
        da.setPositions(processPositions(events));
        da.setSensors(processSensors(events));
        return da;
    }

    public void process(String deviceId, Instant day, List<TrackyEvent> events) throws JsonProcessingException {
        List<TrackysatEventDTO> eventDTOS = events.stream().map(trackysatEventMapper::toTrackysatEventDTO).collect(Collectors.toList());
        this.dailyAggregationRepository.save(buildDailyAggregation(deviceId, day, eventDTOS));
    }

    private String processPositions(List<TrackysatEventDTO> events) throws JsonProcessingException {
        List<PositionDTO> positions = events
            .stream()
            .map(TrackysatEventDTO::getCon)
            .flatMap(List::stream)
            .map(dailyAggregationMapper::conToPosition)
            .distinct()
            .collect(Collectors.toList());
        log.debug("Total unique positions {}", positions.size());
        return JSONUtils.toString(positions);
    }

    private String processSensors(List<TrackysatEventDTO> events) throws JsonProcessingException {
        Map<String, SensorStatsDTO> sensors = new HashMap<>();
        for (TrackysatEventDTO event : events) {
            List<VmsonCon> con = event.getCon();
            for (VmsonCon vmsonCon : con) {
                Map<String, SensorStatsDTO> sensorIdDTOSensorValDTOMap = dailyAggregationMapper.conToSensorMap(vmsonCon);
                Set<Map.Entry<String, SensorStatsDTO>> entrySet = sensorIdDTOSensorValDTOMap.entrySet();
                for (Map.Entry<String, SensorStatsDTO> k : entrySet) {
                    sensors.merge(
                        k.getKey(),
                        k.getValue(),
                        (a, b) -> {
                            a.setValues(
                                Stream.concat(a.getValues().stream(), b.getValues().stream()).distinct().collect(Collectors.toList())
                            );
                            return a;
                        }
                    );
                }
            }
        }
        log.debug("Total sensors {}", sensors.size());
        return JSONUtils.toString(sensors);
    }

    public List<DailyAggregationDTO> getByDeviceIdAndDateRange(String deviceId, Instant dateFrom, Instant dateTo) {
        return dailyAggregationRepository
            .findOneByDeviceIdAndDateRange(deviceId, dateFrom, dateTo)
            .stream()
            .map(dailyAggregationMapper::toDTO)
            .collect(Collectors.toList());
    }
}
