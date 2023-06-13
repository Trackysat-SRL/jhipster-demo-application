package com.trackysat.kafka.service;

import com.datastax.oss.driver.internal.core.util.CollectionsUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.trackysat.kafka.config.Constants;
import com.trackysat.kafka.domain.MonthlyAggregation;
import com.trackysat.kafka.domain.aggregations.PositionDTO;
import com.trackysat.kafka.domain.aggregations.SensorStatsDTO;
import com.trackysat.kafka.domain.aggregations.SensorValDTO;
import com.trackysat.kafka.repository.MonthlyAggregationRepository;
import com.trackysat.kafka.service.dto.DailyAggregationDTO;
import com.trackysat.kafka.service.mapper.DailyAggregationMapper;
import com.trackysat.kafka.utils.DateUtils;
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

    private final DailyAggregationMapper dailyAggregationMapper;

    public MonthlyAggregationService(
        MonthlyAggregationRepository monthlyAggregationRepository,
        DailyAggregationMapper dailyAggregationMapper
    ) {
        this.monthlyAggregationRepository = monthlyAggregationRepository;
        this.dailyAggregationMapper = dailyAggregationMapper;
    }

    public Optional<MonthlyAggregation> getOne() {
        return monthlyAggregationRepository.findOne();
    }

    public List<MonthlyAggregation> getAll() {
        return monthlyAggregationRepository.findAll();
    }

    public List<DailyAggregationDTO> getByDeviceIdAndDateRange(String deviceId, Instant dateFrom, Instant dateTo) {
        return monthlyAggregationRepository
            .findOneByDeviceIdAndDateRange(deviceId, DateUtils.atStartOfDate(dateFrom), DateUtils.atEndOfDate(dateTo))
            .stream()
            .map(dailyAggregationMapper::monthlyToDTO)
            .collect(Collectors.toList());
    }

    public void process(String deviceId, Instant day, List<DailyAggregationDTO> events) throws JsonProcessingException {
        MonthlyAggregation mtAgg = buildMonthlyAggregation(deviceId, day, events);
        if (this.monthlyAggregationRepository.findById(deviceId, day).isPresent()) this.monthlyAggregationRepository.update(
                mtAgg
            ); else this.monthlyAggregationRepository.save(mtAgg);
        //this.monthlyAggregationRepository.save(buildMonthlyAggregation(deviceId, day, events));
    }

    private MonthlyAggregation buildMonthlyAggregation(String deviceId, Instant day, List<DailyAggregationDTO> events)
        throws JsonProcessingException {
        MonthlyAggregation da = new MonthlyAggregation();
        da.setDeviceId(deviceId);
        da.setAggregatedDate(day);
        //TODO per adesso commentate e settate a null, da capire se utile scriverle su un altra tabella
        da.setPositions(null);
        //da.setPositions(processPositions(events));
        da.setSensors(processSensors(events));
        return da;
    }

    private String processSensors(List<DailyAggregationDTO> events) throws JsonProcessingException {
        Map<String, SensorStatsDTO> sensors = new HashMap<>();
        for (DailyAggregationDTO event : events) {
            Map<String, SensorStatsDTO> sensorIdDTOSensorValDTOMap = event.getSensors();
            Set<Map.Entry<String, SensorStatsDTO>> entrySet = sensorIdDTOSensorValDTOMap.entrySet();
            for (Map.Entry<String, SensorStatsDTO> k : entrySet) {
                if (
                    k.getValue().getName().equals(Constants.SENSOR_TOT_VEHICLE_DIST) ||
                    k.getValue().getName().equals(Constants.SENSOR_TIME_ENGINE_LIFE) ||
                    k.getValue().getName().equals(Constants.SENSOR_TOT_FUEL)
                ) calculateDailyValue(k.getValue(), k.getValue().getName());

                sensors.merge(k.getKey(), k.getValue(), this::mergeSensorMaps);
            }
        }
        log.debug("Total sensors {}", sensors.size());

        // TODO Cassandra save fails when over sized
        sensors
            .values()
            .forEach(stat -> {
                if (
                    stat.getName().equals(Constants.SENSOR_TOT_VEHICLE_DIST) ||
                    stat.getName().equals(Constants.SENSOR_TIME_ENGINE_LIFE) ||
                    stat.getName().equals(Constants.SENSOR_TOT_FUEL)
                ) {
                    stat.setValues(stat.getLastDailyValues());
                    stat.setLastDailyValues(null);
                }
                if (stat.getValues().size() > 500) {
                    log.info("Size value > " + 500 + " sensor " + stat.getName());
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
        if (
            sensorStatsDTO.getName().equals((Constants.SENSOR_TOT_VEHICLE_DIST)) ||
            sensorStatsDTO.getName().equals((Constants.SENSOR_TIME_ENGINE_LIFE)) ||
            sensorStatsDTO.getName().equals((Constants.SENSOR_TOT_FUEL))
        ) {
            if (Objects.nonNull(sensorStatsDTO.getLastDailyValues()) && Objects.nonNull(sensorStatsDTO1.getLastDailyValues())) {
                List<SensorValDTO> dailyValues = Stream
                    .concat(sensorStatsDTO.getLastDailyValues().stream(), sensorStatsDTO1.getLastDailyValues().stream())
                    .collect(Collectors.toList());
                sensorStatsDTO.setLastDailyValues(dailyValues);
            }
        }

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
                sensorStatsDTO.setDiff(
                    Optional.ofNullable(sensorStatsDTO.getMax()).orElse(0.0) - Optional.ofNullable(sensorStatsDTO.getMin()).orElse(0.0)
                );
                sensorStatsDTO.setAvg(doubleList.stream().reduce(0.0, Double::sum) / doubleList.size());
                sensorStatsDTO.setSum(doubleList.stream().reduce(0.0, Double::sum));
                sensorStatsDTO.setCount(Collections.singletonMap("total", (long) doubleList.size()));
            } catch (Exception e) {
                log.error("Cannot parse double form values. {}", sensorStatsDTO);
            }
        }

        return sensorStatsDTO;
    }

    private static void calculateDailyValue(SensorStatsDTO sensorStatsDTO, String nameSensor) {
        if (sensorStatsDTO.getName().equals(nameSensor) && sensorStatsDTO.getValues().size() >= 1) {
            SensorValDTO firsValue = sensorStatsDTO.getValues().get(0);
            SensorValDTO lastValue = sensorStatsDTO.getValues().get(sensorStatsDTO.getValues().size() - 1);

            if (Objects.nonNull(lastValue)) {
                SensorValDTO diffValue = new SensorValDTO();
                double diff = Double.parseDouble(lastValue.getValue()) - Double.parseDouble(firsValue.getValue());
                diffValue.setValue(Double.toString(diff));
                diffValue.setCreationDate(lastValue.getCreationDate());
                if (Objects.isNull(sensorStatsDTO.getLastDailyValues())) {
                    sensorStatsDTO.setLastDailyValues(new ArrayList<>());
                }
                sensorStatsDTO.getLastDailyValues().add(diffValue);
            }
        }
    }
}
