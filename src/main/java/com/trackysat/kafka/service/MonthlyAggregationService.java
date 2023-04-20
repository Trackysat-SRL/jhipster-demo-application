package com.trackysat.kafka.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.trackysat.kafka.domain.MonthlyAggregation;
import com.trackysat.kafka.domain.aggregations.PositionDTO;
import com.trackysat.kafka.domain.aggregations.SensorStatsDTO;
import com.trackysat.kafka.domain.aggregations.SensorValDTO;
import com.trackysat.kafka.repository.MonthlyAggregationRepository;
import com.trackysat.kafka.service.dto.DailyAggregationDTO;
import com.trackysat.kafka.utils.JSONUtils;
import java.time.Instant;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Service class for managing monthly aggregations.
 */
@Service
public class MonthlyAggregationService {

    private final Logger log = LoggerFactory.getLogger(MonthlyAggregationService.class);

    private final MonthlyAggregationRepository monthlyAggregationRepository;

    public MonthlyAggregationService(MonthlyAggregationRepository monthlyAggregationRepository) {
        this.monthlyAggregationRepository = monthlyAggregationRepository;
    }

    public Optional<MonthlyAggregation> getOne() {
        return monthlyAggregationRepository.findOne();
    }

    public List<MonthlyAggregation> getAll() {
        return monthlyAggregationRepository.findAll();
    }

    public List<DailyAggregationDTO> getByDeviceIdAndDateRange(String deviceId, Instant dateFrom, Instant dateTo) {
        return null;
    }

    public void process(String deviceId, Instant day, List<DailyAggregationDTO> events) throws JsonProcessingException {
        this.monthlyAggregationRepository.save(buildMonthlyAggregation(deviceId, day, events));
    }

    private MonthlyAggregation buildMonthlyAggregation(String deviceId, Instant day, List<DailyAggregationDTO> events)
        throws JsonProcessingException {
        MonthlyAggregation da = new MonthlyAggregation();
        da.setDeviceId(deviceId);
        da.setAggregatedDate(day);
        da.setPositions(processPositions(events));
        da.setSensors(processSensors(events));
        return da;
    }

    private String processSensors(List<DailyAggregationDTO> events) throws JsonProcessingException {
        Map<String, SensorStatsDTO> sensors = new HashMap<>();
        for (DailyAggregationDTO event : events) {
            Map<String, SensorStatsDTO> sensorIdDTOSensorValDTOMap = event.getSensors();
            Set<Map.Entry<String, SensorStatsDTO>> entrySet = sensorIdDTOSensorValDTOMap.entrySet();
            for (Map.Entry<String, SensorStatsDTO> k : entrySet) {
                sensors.merge(k.getKey(), k.getValue(), this::mergeSensorMaps);
            }
        }
        log.debug("Total sensors {}", sensors.size());

        // TODO Cassandra save fails when over sized
        sensors
            .values()
            .forEach(stat -> {
                if (stat.getValues().size() > 500) {
                    stat.setValues(new ArrayList<>());
                }
            });

        return JSONUtils.toString(sensors);
    }

    private String processPositions(List<DailyAggregationDTO> events) throws JsonProcessingException {
        List<PositionDTO> positions = events
            .stream()
            .map(DailyAggregationDTO::getPositions)
            .flatMap(List::stream)
            .collect(Collectors.toList());
        log.debug("Total unique positions {}", positions.size());
        return JSONUtils.toString(positions);
    }

    public SensorStatsDTO mergeSensorMaps(SensorStatsDTO sensorStatsDTO, SensorStatsDTO sensorStatsDTO1) {
        List<SensorValDTO> allValues = Stream
            .concat(sensorStatsDTO.getValues().stream(), sensorStatsDTO1.getValues().stream())
            .collect(Collectors.toList());
        sensorStatsDTO.setValues(allValues);

        allValues.stream().min(Comparator.comparing(c -> c.getCreationDate().getEpochSecond())).ifPresent(sensorStatsDTO::setFirstValue);
        allValues.stream().max(Comparator.comparing(c -> c.getCreationDate().getEpochSecond())).ifPresent(sensorStatsDTO::setLastValue);

        if (Objects.equals(sensorStatsDTO.getType(), "TELLTALE") || Objects.equals(sensorStatsDTO.getType(), "BOOLEAN")) {
            sensorStatsDTO.setCount(
                allValues.stream().map(SensorValDTO::getValue).collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
            );
        } else {
            try {
                List<Double> doubleList = allValues
                    .stream()
                    .map(SensorValDTO::getValue)
                    .map(Double::parseDouble)
                    .collect(Collectors.toList());
                doubleList.stream().max(Comparator.naturalOrder()).ifPresent(sensorStatsDTO::setMax);
                doubleList.stream().min(Comparator.naturalOrder()).ifPresent(sensorStatsDTO::setMin);
                sensorStatsDTO.setAvg(doubleList.stream().reduce(0.0, Double::sum) / doubleList.size());
                sensorStatsDTO.setSum(doubleList.stream().reduce(0.0, Double::sum));
                sensorStatsDTO.setCount(Collections.singletonMap("total", (long) doubleList.size()));
            } catch (Exception e) {
                log.error("Cannot parse double form values. {}", sensorStatsDTO);
            }
        }

        return sensorStatsDTO;
    }
}
