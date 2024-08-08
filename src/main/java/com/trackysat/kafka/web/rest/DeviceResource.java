package com.trackysat.kafka.web.rest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.trackysat.kafka.config.Constants;
import com.trackysat.kafka.domain.Device;
import com.trackysat.kafka.domain.aggregations.PositionDTO;
import com.trackysat.kafka.domain.aggregations.SensorStatsDTO;
import com.trackysat.kafka.service.AggregationDelegatorService;
import com.trackysat.kafka.service.DeviceService;
import com.trackysat.kafka.service.dto.DailyAggregationDTO;
import com.trackysat.kafka.utils.cache.AbstractCache;
import com.trackysat.kafka.web.rest.dto.BulkDeviceRequestDTO;
import com.trackysat.kafka.web.rest.dto.DeviceTimeZoneDTO;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springdoc.api.annotations.ParameterObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;
import tech.jhipster.web.util.PaginationUtil;
import tech.jhipster.web.util.ResponseUtil;

@RestController
@RequestMapping("/api/devices")
public class DeviceResource {

    private final Logger log = LoggerFactory.getLogger(DeviceResource.class);

    @Value("${jhipster.clientApp.name}")
    private String applicationName;

    private final DeviceService deviceService;

    private final AggregationDelegatorService aggregationDelegatorService;

    private final AbstractCache<String, List<SensorStatsDTO>> monthlyAggregationCache;

    public DeviceResource(
        DeviceService deviceService,
        AggregationDelegatorService aggregationDelegatorService,
        AbstractCache<String, List<SensorStatsDTO>> monthlyAggregationCache
    ) {
        this.deviceService = deviceService;
        this.aggregationDelegatorService = aggregationDelegatorService;
        this.monthlyAggregationCache = monthlyAggregationCache;
    }

    @GetMapping
    public ResponseEntity<List<Device>> getList(@org.springdoc.api.annotations.ParameterObject Pageable pageable) {
        log.debug("REST request to get a page of Device list");
        List<Device> devices = deviceService.getAll();

        final int start = (int) pageable.getOffset();
        final int end = Math.min((start + pageable.getPageSize()), devices.size());
        final Page<Device> page = new PageImpl<>(devices.subList(start, end), pageable, devices.size());

        HttpHeaders headers = PaginationUtil.generatePaginationHttpHeaders(ServletUriComponentsBuilder.fromCurrentRequest(), page);
        return ResponseEntity.ok().headers(headers).body(page.getContent());
    }

    @GetMapping("/{id}")
    public ResponseEntity<Device> getOne(@PathVariable String id) {
        log.debug("REST request to get a page of Device by deviceId: {}", id);
        return ResponseUtil.wrapOrNotFound(deviceService.getOne(id));
    }

    @PutMapping
    public ResponseEntity<List<String>> updateDevice(@RequestBody DeviceTimeZoneDTO deviceTimeZoneDTO) {
        log.debug("REST request to put device zone");
        List<String> deviceUpdated = new ArrayList<>();
        for (String key : deviceTimeZoneDTO.getDeviceZoneMap().keySet()) {
            Optional<Device> deviceToUpdated = deviceService.getOne(key);
            deviceToUpdated.ifPresent(device -> {
                device.setTimezone(deviceTimeZoneDTO.getDeviceZoneMap().get(key));
                deviceService.updateDevice(device);
                deviceUpdated.add(device.getUid());
            });
        }
        return ResponseEntity.ok().body(deviceUpdated);
    }

    @GetMapping("/{id}/data")
    public ResponseEntity<List<DailyAggregationDTO>> getListAggregation(
        @org.springdoc.api.annotations.ParameterObject Pageable pageable,
        @PathVariable String id,
        @RequestParam("from") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) String from,
        @RequestParam("to") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) String to
    ) {
        log.debug("REST request to get a page of DailyAggregationDTO by deviceId: {}, {}, {}", id, from, to);
        Instant fromDate = Instant.parse(from);
        Instant toDate = Instant.parse(to);
        try {
            List<DailyAggregationDTO> trackyEvents = aggregationDelegatorService.getByDeviceIdAndDateRange(id, fromDate, toDate);
            final int start = (int) pageable.getOffset();
            final int end = Math.min((start + pageable.getPageSize()), trackyEvents.size());
            final Page<DailyAggregationDTO> page = new PageImpl<>(trackyEvents.subList(start, end), pageable, trackyEvents.size());

            HttpHeaders headers = PaginationUtil.generatePaginationHttpHeaders(ServletUriComponentsBuilder.fromCurrentRequest(), page);
            return ResponseEntity.ok().headers(headers).body(page.getContent());
        } catch (JsonProcessingException e) {
            return ResponseEntity.unprocessableEntity().body(null);
        }
    }

    @GetMapping("/{id}/positions")
    public ResponseEntity<List<PositionDTO>> getPositionList(
        @org.springdoc.api.annotations.ParameterObject Pageable pageable,
        @PathVariable String id,
        @RequestParam("from") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) String from,
        @RequestParam("to") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) String to
    ) {
        log.debug("REST request to get a page of getPositionList by deviceId: {}, {}, {}", id, from, to);
        Instant fromDate = Instant.parse(from);
        Instant toDate = Instant.parse(to);
        try {
            List<PositionDTO> trackyEvents = aggregationDelegatorService.getPositionsByDeviceIdAndDateRange(id, fromDate, toDate);
            final int start = (int) pageable.getOffset();
            final int end = Math.min((start + pageable.getPageSize()), trackyEvents.size());
            final Page<PositionDTO> page = new PageImpl<>(trackyEvents.subList(start, end), pageable, trackyEvents.size());

            HttpHeaders headers = PaginationUtil.generatePaginationHttpHeaders(ServletUriComponentsBuilder.fromCurrentRequest(), page);
            return ResponseEntity.ok().headers(headers).body(page.getContent());
        } catch (JsonProcessingException e) {
            return ResponseEntity.unprocessableEntity().body(null);
        }
    }

    @GetMapping("/{id}/sensors")
    public ResponseEntity<List<SensorStatsDTO>> getSensorList(
        @PathVariable String id,
        @RequestParam("from") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) String from,
        @RequestParam("to") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) String to,
        @RequestParam(value = "values", required = false, defaultValue = "false") Boolean includeValues
    ) {
        log.debug("REST request to getSensorList by deviceId: {}, {}, {}, {}", id, from, to, includeValues);
        Instant fromDate = Instant.parse(from);
        Instant toDate = Instant.parse(to);
        try {
            Map<String, SensorStatsDTO> sensors = aggregationDelegatorService.getSensorsByDeviceIdAndDateRange(id, fromDate, toDate);
            if (!includeValues) {
                sensors.values().forEach(s -> s.setValues(null));
            }
            return ResponseEntity.ok().body(new ArrayList<>(sensors.values()));
        } catch (JsonProcessingException e) {
            return ResponseEntity.unprocessableEntity().body(null);
        }
    }

    @GetMapping("/{id}/sensors/{sensor}")
    public ResponseEntity<List<SensorStatsDTO>> getFilteredSensor(
        @PathVariable String id,
        @PathVariable String sensor,
        @RequestParam("from") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) String from,
        @RequestParam("to") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) String to,
        @RequestParam(value = "values", required = false, defaultValue = "false") Boolean includeValues
    ) {
        log.debug("REST request to getFilteredSensor by deviceId: {}, {}, {}, {}, {}", id, from, to, sensor, includeValues);
        Instant fromDate = Instant.parse(from);
        Instant toDate = Instant.parse(to);
        try {
            Map<String, SensorStatsDTO> sensors = aggregationDelegatorService.getSensorsByDeviceIdAndDateRange(id, fromDate, toDate);
            List<SensorStatsDTO> stats = sensors
                .entrySet()
                .stream()
                .filter(e -> e.getKey().toLowerCase().contains(sensor.toLowerCase()))
                .map(Map.Entry::getValue)
                .collect(Collectors.toList());
            if (!includeValues) {
                stats.forEach(s -> s.setValues(null));
            }
            return ResponseEntity.ok().body(stats);
        } catch (JsonProcessingException e) {
            return ResponseEntity.unprocessableEntity().body(null);
        }
    }

    @GetMapping("/bulk/sensors")
    public ResponseEntity<Map<String, List<SensorStatsDTO>>> getSensorListBulk(
        @RequestParam("from") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) String from,
        @RequestParam("to") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) String to,
        @RequestParam(value = "values", required = false, defaultValue = "false") Boolean includeValues,
        @Valid @RequestBody BulkDeviceRequestDTO devicesIds
    ) {
        log.debug("REST request to getSensorListBulk: {}, {}, {}, {}", devicesIds.getDevices(), from, to, includeValues);
        Instant fromDate = Instant.parse(from);
        Instant toDate = Instant.parse(to);
        Map<String, List<SensorStatsDTO>> allDevicesSensors = devicesIds
            .getDevices()
            .stream()
            .distinct()
            .collect(Collectors.toList())
            .parallelStream()
            .collect(
                Collectors.toMap(
                    Function.identity(),
                    id -> {
                        try {
                            Map<String, SensorStatsDTO> sensors = aggregationDelegatorService.getSensorsByDeviceIdAndDateRange(
                                id,
                                fromDate,
                                toDate
                            );
                            if (!includeValues) {
                                sensors.values().forEach(s -> s.setValues(null));
                            }
                            return new ArrayList<>(sensors.values());
                        } catch (JsonProcessingException e) {
                            return Collections.emptyList();
                        }
                    }
                )
            );
        return ResponseEntity.ok().body(allDevicesSensors);
    }

    @GetMapping("/bulk/sensors/{sensor}")
    public ResponseEntity<Map<String, List<SensorStatsDTO>>> getFilteredSensorBulk(
        @PathVariable String sensor,
        @RequestParam("from") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) String from,
        @RequestParam("to") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) String to,
        @RequestParam(value = "values", required = false, defaultValue = "false") Boolean includeValues,
        @Valid @RequestBody BulkDeviceRequestDTO devicesIds
    ) {
        log.debug("REST request to getFilteredSensorBulk: {}, {}, {}, {}", devicesIds.getDevices(), from, to, includeValues);
        Instant fromDate = Instant.parse(from);
        Instant toDate = Instant.parse(to);
        Map<String, List<SensorStatsDTO>> allDevicesSensors = devicesIds
            .getDevices()
            .stream()
            .distinct()
            .collect(Collectors.toList())
            .parallelStream()
            .collect(
                Collectors.toMap(
                    Function.identity(),
                    id -> {
                        try {
                            Map<String, SensorStatsDTO> sensors = aggregationDelegatorService.getSensorsByDeviceIdAndDateRange(
                                id,
                                fromDate,
                                toDate
                            );
                            List<SensorStatsDTO> stats = sensors
                                .entrySet()
                                .stream()
                                .filter(e -> e.getKey().toLowerCase().contains(sensor.toLowerCase()))
                                .map(Map.Entry::getValue)
                                .collect(Collectors.toList());
                            if (!includeValues) {
                                stats.forEach(s -> s.setValues(null));
                            }
                            return stats;
                        } catch (JsonProcessingException e) {
                            return Collections.emptyList();
                        }
                    }
                )
            );
        return ResponseEntity.ok().body(allDevicesSensors);
    }

    @GetMapping("/summary/sensors")
    public ResponseEntity<List<SensorStatsDTO>> getSensorListSummarized(
        @RequestParam("from") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) String from,
        @RequestParam("to") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) String to,
        @RequestParam(value = "values", required = false, defaultValue = "false") Boolean includeValues,
        @Valid @RequestBody BulkDeviceRequestDTO devicesIds
    ) {
        log.debug("REST request to getSensorListSummarized: {}, {}, {}, {}", devicesIds.getDevices(), from, to, includeValues);
        Instant fromDate = Instant.parse(from);
        Instant toDate = Instant.parse(to);
        List<String> ids = devicesIds.getDevices();
        List<SensorStatsDTO> summary = aggregationDelegatorService.getSensorsSummaryByDeviceIdAndDateRange(
            ids,
            fromDate,
            toDate,
            Optional.empty()
        );
        if (!includeValues) {
            summary.forEach(s -> s.setValues(null));
        }
        return ResponseEntity.ok().body(summary);
    }

    @GetMapping("/summary/sensors/{sensor}")
    public ResponseEntity<List<SensorStatsDTO>> getFilteredSensorSummarized(
        @PathVariable String sensor,
        @RequestParam("from") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) String from,
        @RequestParam("to") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) String to,
        @RequestParam(value = "values", required = false, defaultValue = "false") Boolean includeValues,
        @Valid @RequestBody BulkDeviceRequestDTO devicesIds
    ) {
        log.debug("REST request to getFilteredSensorSummarized: {}, {}, {}, {}", devicesIds.getDevices(), from, to, includeValues);
        Instant fromDate = Instant.parse(from);
        Instant toDate = Instant.parse(to);
        List<String> ids = devicesIds.getDevices();
        List<SensorStatsDTO> summary = aggregationDelegatorService.getSensorsSummaryByDeviceIdAndDateRange(
            ids,
            fromDate,
            toDate,
            Optional.ofNullable(sensor)
        );
        if (!includeValues) {
            summary.forEach(s -> s.setValues(null));
        }
        return ResponseEntity.ok().body(summary);
    }

    @PostMapping("/sensorsTotalVehicleDistance/{unitMisure}")
    public ResponseEntity<Map<String, List<SensorStatsDTO>>> getSensorTotalVehicleDistance(
        @PathVariable String unitMisure,
        @RequestParam("from") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) String from,
        @RequestParam("to") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) String to,
        @RequestParam(value = "values", required = false, defaultValue = "true") Boolean includeValues,
        @Valid @RequestBody BulkDeviceRequestDTO devicesIds
    ) {
        log.debug("REST request to getSensorTotalVehicleDistance: {}, {}, {}, {}", devicesIds.getDevices(), from, to, includeValues);
        Instant fromDate = Instant.parse(from);
        Instant toDate = Instant.parse(to);
        Duration duration = Duration.between(fromDate, toDate);
        long days = duration.toDays();
        Map<String, List<SensorStatsDTO>> allDevicesSensors = devicesIds
            .getDevices()
            .stream()
            .distinct()
            .collect(Collectors.toList())
            .parallelStream()
            .collect(
                Collectors.toMap(
                    Function.identity(),
                    id ->
                        monthlyAggregationCache.getOrElse(
                            id,
                            key -> {
                                try {
                                    Map<String, SensorStatsDTO> sensors = aggregationDelegatorService.getSensorsByDeviceIdAndDateRange(
                                        key,
                                        fromDate,
                                        toDate
                                    );
                                    List<SensorStatsDTO> stats = sensors
                                        .entrySet()
                                        .stream()
                                        .filter(e ->
                                            e.getKey().toLowerCase().contains(Constants.SENSOR_TOT_VEHICLE_DIST.toLowerCase()) &&
                                            unitMisure.toLowerCase().equals(e.getValue().getMeasureUnit())
                                        )
                                        .map(Map.Entry::getValue)
                                        .collect(Collectors.toList());

                                    if (!includeValues) {
                                        stats.forEach(s -> s.setValues(null));
                                    }
                                    return stats;
                                } catch (JsonProcessingException e) {
                                    return Collections.emptyList();
                                }
                            },
                            false
                        )
                )
            );
        return ResponseEntity.ok().body(allDevicesSensors);
    }

    @PostMapping("/sensorsTotalFuel")
    public ResponseEntity<Map<String, List<SensorStatsDTO>>> getSensorTotalFuel(
        @RequestParam("from") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) String from,
        @RequestParam("to") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) String to,
        @RequestParam(value = "values", required = false, defaultValue = "true") Boolean includeValues,
        @Valid @RequestBody BulkDeviceRequestDTO devicesIds
    ) {
        log.debug("REST request to sensorsTotalFuel: {}, {}, {}, {}", devicesIds.getDevices(), from, to, includeValues);
        Instant fromDate = Instant.parse(from);
        Instant toDate = Instant.parse(to);
        Duration duration = Duration.between(fromDate, toDate);
        long days = duration.toDays();
        Map<String, List<SensorStatsDTO>> allDevicesSensors = devicesIds
            .getDevices()
            .stream()
            .distinct()
            .collect(Collectors.toList())
            .parallelStream()
            .collect(
                Collectors.toMap(
                    Function.identity(),
                    id ->
                        monthlyAggregationCache.getOrElse(
                            id,
                            key -> {
                                try {
                                    Map<String, SensorStatsDTO> sensors = aggregationDelegatorService.getSensorsByDeviceIdAndDateRange(
                                        key,
                                        fromDate,
                                        toDate
                                    );
                                    List<SensorStatsDTO> stats = sensors
                                        .entrySet()
                                        .stream()
                                        .filter(e -> e.getKey().toLowerCase().contains(Constants.SENSOR_TOT_FUEL.toLowerCase()))
                                        .map(Map.Entry::getValue)
                                        .collect(Collectors.toList());

                                    if (!includeValues) {
                                        stats.forEach(s -> s.setValues(null));
                                    }
                                    return stats;
                                } catch (JsonProcessingException e) {
                                    return Collections.emptyList();
                                }
                            },
                            false
                        )
                )
            );
        return ResponseEntity.ok().body(allDevicesSensors);
    }

    @PostMapping("/serviceDistance")
    public ResponseEntity<Map<String, List<SensorStatsDTO>>> getServiceDistance(
        @RequestParam("day") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) String day,
        @RequestParam(value = "values", required = false, defaultValue = "false") Boolean includeValues,
        @Valid @RequestBody BulkDeviceRequestDTO devicesIds
    ) {
        log.debug("REST request to getServiceDistance: {}, {}, {}", devicesIds.getDevices(), day, includeValues);

        Instant fromDate = Instant.parse(day);
        Instant toDate = fromDate.plus(1, ChronoUnit.DAYS);

        Map<String, List<SensorStatsDTO>> allDevicesSensors = devicesIds
            .getDevices()
            .stream()
            .distinct()
            .collect(Collectors.toList())
            .parallelStream()
            .collect(
                Collectors.toMap(
                    Function.identity(),
                    id -> {
                        try {
                            Map<String, SensorStatsDTO> sensors = aggregationDelegatorService.getLastValueSensorsByDeviceIdAndDateRange(
                                id,
                                fromDate,
                                toDate
                            );
                            List<SensorStatsDTO> stats = sensors
                                .entrySet()
                                .stream()
                                .filter(e -> e.getKey().toLowerCase().contains(Constants.SENSOR_SERVICE_DISTANCE.toLowerCase()))
                                .map(Map.Entry::getValue)
                                .filter(v ->
                                    Objects.nonNull(v.getLastValue()) &&
                                    Objects.nonNull(v.getLastValue().getValue()) &&
                                    Double.parseDouble(v.getLastValue().getValue()) < 160000.0 &&
                                    Double.parseDouble(v.getLastValue().getValue()) > -160000.0
                                )
                                .collect(Collectors.toList());

                            if (!includeValues) {
                                stats.forEach(s -> s.setValues(null));
                            }
                            return stats;
                        } catch (JsonProcessingException e) {
                            return Collections.emptyList();
                        }
                    }
                )
            );
        return ResponseEntity.ok().body(allDevicesSensors);
    }
    /*    @GetMapping("/summaryAllDevices/sensors/{sensor}")
    public ResponseEntity<List<SensorStatsDTO>> summaryAllDevices(
        @PathVariable String sensor,
        @RequestParam("from") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) String from,
        @RequestParam("to") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) String to,
        @RequestParam(value = "values", required = false, defaultValue = "false") Boolean includeValues
    ) {


       log.debug("REST request to get a page of Device list");
        BulkDeviceRequestDTO bulkDeviceRequestDTO = new BulkDeviceRequestDTO();

        List<String> devicesStringUid = new  ArrayList<String>();

        List<Device> allDevices = deviceService.getAll();
        log.info("reprocessing {} allDevices", allDevices.size());

        allDevices.forEach(dev -> {
            try {
            	devicesStringUid.add(dev.getUid());

            } catch (Exception e) {
                log.error("error processing device with uid: {}. Exception: {}", devicesStringUid, e);
            }
        });

        bulkDeviceRequestDTO.setDevices(devicesStringUid);

        log.debug("REST request to getFilteredSensorSummarized: {}, {}, {}, {}", devicesStringUid, from, to, includeValues);
        Instant fromDate = Instant.parse(from);
        Instant toDate = Instant.parse(to);
        Map<String, List<SensorStatsDTO>> allDevicesSensorsDto = devicesStringUid
            .stream()
            .distinct()
            .collect(Collectors.toList())
            .parallelStream()
            .collect(
                Collectors.toMap(
                    Function.identity(),
                    id -> {
                        try {
                            Map<String, SensorStatsDTO> sensors = aggregationDelegatorService.getSensorsByDeviceIdAndDateRange(
                                id,
                                fromDate,
                                toDate
                            );
                            List<SensorStatsDTO> stats = sensors
                                .entrySet()
                                .stream()
                                .filter(e -> e.getKey().toLowerCase().contains(sensor.toLowerCase()))
                                .map(Map.Entry::getValue)
                                .collect(Collectors.toList());
                            if (!includeValues) {
                                stats.forEach(s -> s.setValues(null));
                            }
                            return stats;
                        } catch (JsonProcessingException e) {
                            return Collections.emptyList();
                        }
                    }
                )
            );
        List<SensorStatsDTO> summary = List.of(
        		allDevicesSensorsDto
                .values()
                .stream()
                .flatMap(List::stream)
                .reduce(new SensorStatsDTO(), aggregationDelegatorService::mergeSensors)
        );
        return ResponseEntity.ok().body(summary);
    }




    @GetMapping("/bulkAllDevices/sensors/{sensor}")
    public ResponseEntity<Map<String, List<SensorStatsDTO>>> getFilteredSensorAllDevicesBulk(
        @PathVariable String sensor,
        @RequestParam("from") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) String from,
        @RequestParam("to") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) String to,
        @RequestParam(value = "values", required = false, defaultValue = "false") Boolean includeValues
    ) {

    	  log.debug("REST request to get a page of Device list");
          BulkDeviceRequestDTO bulkDeviceRequestDTO = new BulkDeviceRequestDTO();

          List<String> devicesStringUid = new  ArrayList<String>();

          List<Device> allDevices = deviceService.getAll();
          log.info("reprocessing {} allDevices", allDevices.size());

          allDevices.forEach(dev -> {
              try {
              	devicesStringUid.add(dev.getUid());

              } catch (Exception e) {
                  log.error("error processing device with uid: {}. Exception: {}", devicesStringUid, e);
              }
          });

          bulkDeviceRequestDTO.setDevices(devicesStringUid);


        log.debug("REST request to getFilteredSensorAllDevicesBulk: {}, {}, {}, {}", devicesStringUid, from, to, includeValues);
        Instant fromDate = Instant.parse(from);
        Instant toDate = Instant.parse(to);
        Map<String, List<SensorStatsDTO>> allDevicesSensorsDto = devicesStringUid
            .stream()
            .distinct()
            .collect(Collectors.toList())
            .parallelStream()
            .collect(
                Collectors.toMap(
                    Function.identity(),
                    id -> {
                        try {
                            Map<String, SensorStatsDTO> sensors = aggregationDelegatorService.getSensorsByDeviceIdAndDateRange(
                                id,
                                fromDate,
                                toDate
                            );
                            List<SensorStatsDTO> stats = sensors
                                .entrySet()
                                .stream()
                                .filter(e -> e.getKey().toLowerCase().contains(sensor.toLowerCase()))
                                .map(Map.Entry::getValue)
                                .collect(Collectors.toList());
                            if (!includeValues) {
                                stats.forEach(s -> s.setValues(null));
                            }
                            return stats;
                        } catch (JsonProcessingException e) {
                            return Collections.emptyList();
                        }
                    }
                )
            );
        return ResponseEntity.ok().body(allDevicesSensorsDto);
    }*/
}
