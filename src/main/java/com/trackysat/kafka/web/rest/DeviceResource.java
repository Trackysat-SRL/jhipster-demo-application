package com.trackysat.kafka.web.rest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.trackysat.kafka.domain.Device;
import com.trackysat.kafka.domain.aggregations.PositionDTO;
import com.trackysat.kafka.domain.aggregations.SensorStatsDTO;
import com.trackysat.kafka.service.AggregationDelegatorService;
import com.trackysat.kafka.service.DeviceService;
import com.trackysat.kafka.service.dto.DailyAggregationDTO;
import com.trackysat.kafka.web.rest.dto.BulkDeviceRequestDTO;
import java.time.Instant;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    public DeviceResource(DeviceService deviceService, AggregationDelegatorService aggregationDelegatorService) {
        this.deviceService = deviceService;
        this.aggregationDelegatorService = aggregationDelegatorService;
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
}
