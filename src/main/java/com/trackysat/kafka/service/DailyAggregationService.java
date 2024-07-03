package com.trackysat.kafka.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.trackysat.kafka.config.Constants;
import com.trackysat.kafka.domain.DailyAggregation;
import com.trackysat.kafka.domain.TrackyEvent;
import com.trackysat.kafka.domain.aggregations.PositionDTO;
import com.trackysat.kafka.domain.aggregations.SensorStatsDTO;
import com.trackysat.kafka.domain.aggregations.SensorValDTO;
import com.trackysat.kafka.domain.vmson.Sen;
import com.trackysat.kafka.domain.vmson.VmsonCon;
import com.trackysat.kafka.repository.DailyAggregationRepository;
import com.trackysat.kafka.service.dto.DailyAggregationDTO;
import com.trackysat.kafka.service.dto.TrackysatEventDTO;
import com.trackysat.kafka.service.mapper.DailyAggregationMapper;
import com.trackysat.kafka.service.mapper.TrackysatEventMapper;
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
        /*List<TrackysatEventDTO> eventDTOSTomorow = new ArrayList<>();
        eventDTOS.forEach(v -> {
            int createDay = v.getCreatedDate().atZone(ZoneId.systemDefault()).getDayOfWeek().getValue();
            if(createDay != v.getEventDate().atZone(ZoneId.systemDefault()).getDayOfWeek().getValue()){
                List<VmsonCon> con = v.getCon().stream().filter(c -> c.getEts().getTst().atZone(ZoneId.systemDefault()).getDayOfWeek().getValue() != createDay).collect(Collectors.toList());
                TrackysatEventDTO mapped = new TrackysatEventDTO();
                mapped.setDeviceId(v.getDeviceId());
                mapped.setEventDate(v.getEventDate());
                mapped.setCreatedDate(con.stream().findFirst().map(VmsonCon::getEts).map(Ets::getTst).orElse(v.getEts().getTst()));
                mapped.setUid(v.getUid());
                mapped.setVer(v.getVer());
                mapped.setCon(con);
                mapped.setDes(v.getDes());
                mapped.setEts(v.getEts());
                mapped.setOri(v.getOri());
                eventDTOSTomorow.add(mapped);
            }
        });*/
        this.dailyAggregationRepository.save(buildDailyAggregation(deviceId, day, eventDTOS));
        /*if(eventDTOSTomorow.size() > 0){
            this.dailyAggregationRepository.save(buildDailyAggregation(deviceId, day, eventDTOSTomorow));
        }*/
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
            List<VmsonCon> tmpVmson = con;
            tmpVmson.forEach(vj -> {
                List<Sen> listSens = vj
                    .getSen()
                    .stream()
                    .filter(s ->
                        s.getIid().contains("TotalVehicleDistance") ||
                        s.getIid().contains("TotalFuel") ||
                        s.getIid().contains("TimeEngineLife") ||
                        s.getIid().contains("ServiceDistance")
                    )
                    .collect(Collectors.toList());
                vj.getSen().clear();
                vj.setSen(listSens);
            });
            con = tmpVmson;
            for (VmsonCon vmsonCon : con) {
                Map<String, SensorStatsDTO> sensorIdDTOSensorValDTOMap = dailyAggregationMapper.conToSensorMap(vmsonCon);
                this.convertSensorValue(sensorIdDTOSensorValDTOMap);
                Set<Map.Entry<String, SensorStatsDTO>> entrySet = sensorIdDTOSensorValDTOMap.entrySet();
                for (Map.Entry<String, SensorStatsDTO> k : entrySet) {
                    sensors.merge(k.getKey(), k.getValue(), this::mergeSensorMaps);
                }
            }
        }

        //Se presente la CAN km tolgo dalla mappa le altre perchè non le devo salvare.
        if (Objects.nonNull(sensors.get(Constants.SENSOR_NAME_CAN_DISTANCE_KM))) {
            checkRemoveSensor(sensors, Constants.SENSOR_NAME_CAN_DISTANCE_M);
            checkRemoveSensor(sensors, Constants.SENSOR_NAME_DEVICE_DISTANCE_KM);
            checkRemoveSensor(sensors, Constants.SENSOR_NAME_DEVICE_DISTANCE_M);
        } else if (Objects.nonNull(sensors.get(Constants.SENSOR_NAME_DEVICE_DISTANCE_KM))) {
            //Se presente la DEVICE km tolgo dalla mappa quella in metri perchè non è da salvare.
            checkRemoveSensor(sensors, Constants.SENSOR_NAME_DEVICE_DISTANCE_M);
        }

        log.debug("Total sensors {}", sensors.size());

        // TODO Cassandra save fails when over sized
        sensors
            .values()
            .forEach(stat -> {
                if (stat.getValues().size() > 500) {
                    //salvo solo il primo valore e l'ultmo
                    List<SensorValDTO> copy = stat.getValues();
                    stat.setValues(new ArrayList<>());
                    stat.getValues().add(copy.get(0));
                    stat.getValues().add(copy.get(copy.size() - 1));
                }
            });

        return JSONUtils.toString(sensors);
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

    public List<DailyAggregationDTO> getByDeviceIdAndDateRange(String deviceId, Instant dateFrom, Instant dateTo) {
        log.debug("Getting DailyAggregations by device {} and range {} - {}", deviceId, dateFrom, dateTo);
        return dailyAggregationRepository
            .findOneByDeviceIdAndDateRange(deviceId, DateUtils.atStartOfDate(dateFrom), DateUtils.atEndOfDate(dateTo))
            .stream()
            .map(dailyAggregationMapper::toDTO)
            .collect(Collectors.toList());
    }

    public List<DailyAggregationDTO> getByDeviceIdAndSingleDay(String deviceId, Instant dateFrom) {
        log.debug("Getting DailyAggregations by device {} and day {} ", deviceId, dateFrom);
        return dailyAggregationRepository
            .findOneByDeviceIdAndSingleDay(deviceId, DateUtils.atStartOfDate(dateFrom))
            .stream()
            .map(dailyAggregationMapper::toDTO)
            .collect(Collectors.toList());
    }

    private void checkRemoveSensor(Map<String, SensorStatsDTO> sensorStatsDTOMap, String key) {
        if (Objects.nonNull(sensorStatsDTOMap.get(key))) {
            sensorStatsDTOMap.remove(key);
        }
    }

    private void convertSensorValue(Map<String, SensorStatsDTO> sensorIdDTOSensorValDTOMap) {
        //verifico se è presente il valore di TotalVehicleDinstance in km da CAN
        if (Objects.isNull(sensorIdDTOSensorValDTOMap.get(Constants.SENSOR_NAME_CAN_DISTANCE_KM))) {
            //in caso non presente vedere se è presente in m da CAN
            if (Objects.isNull(sensorIdDTOSensorValDTOMap.get(Constants.SENSOR_NAME_CAN_DISTANCE_M))) {
                //in caso non presente vedere se è presente il valore del DEVICE in km
                if (Objects.isNull(sensorIdDTOSensorValDTOMap.get(Constants.SENSOR_NAME_DEVICE_DISTANCE_KM))) {
                    //in caso non presente vedere se è presente il valore del DEVICE in m
                    if (!Objects.isNull(sensorIdDTOSensorValDTOMap.get(Constants.SENSOR_NAME_DEVICE_DISTANCE_M))) {
                        //converto in km
                        SensorStatsDTO sensorStatsDTO = SensorStatsDTO.copy(
                            sensorIdDTOSensorValDTOMap.get(Constants.SENSOR_NAME_DEVICE_DISTANCE_M)
                        );
                        sensorStatsDTO
                            .getValues()
                            .forEach(val -> {
                                String newValue = String.valueOf(Double.parseDouble(val.getValue()) / 1000);
                                val.setValue(newValue);
                            });
                        sensorStatsDTO.setMeasureUnit("km");
                        sensorIdDTOSensorValDTOMap.put(Constants.SENSOR_NAME_DEVICE_DISTANCE_KM, sensorStatsDTO);
                    }
                }
            } else {
                //converto in km
                SensorStatsDTO sensorStatsDTO = SensorStatsDTO.copy(sensorIdDTOSensorValDTOMap.get(Constants.SENSOR_NAME_CAN_DISTANCE_M));
                sensorStatsDTO
                    .getValues()
                    .forEach(val -> {
                        String newValue = String.valueOf(Double.parseDouble(val.getValue()) / 1000);
                        val.setValue(newValue);
                    });
                sensorStatsDTO.setMeasureUnit("km");
                sensorIdDTOSensorValDTOMap.put(Constants.SENSOR_NAME_CAN_DISTANCE_KM, sensorStatsDTO);
            }
        }
    }
}
