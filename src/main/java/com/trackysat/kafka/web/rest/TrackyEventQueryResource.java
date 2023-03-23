package com.trackysat.kafka.web.rest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.trackysat.kafka.domain.aggregations.SensorStatsDTO;
import com.trackysat.kafka.service.AggregationDelegatorService;
import com.trackysat.kafka.service.TrackyEventQueryService;
import com.trackysat.kafka.service.dto.DailyAggregationDTO;
import com.trackysat.kafka.service.dto.TrackysatEventDTO;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
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
@RequestMapping("/api")
public class TrackyEventQueryResource {

    private final Logger log = LoggerFactory.getLogger(TrackyEventQueryResource.class);

    @Value("${jhipster.clientApp.name}")
    private String applicationName;

    private final TrackyEventQueryService trackyEventQueryService;

    private final AggregationDelegatorService aggregationDelegatorService;

    public TrackyEventQueryResource(
        TrackyEventQueryService trackyEventQueryService,
        AggregationDelegatorService aggregationDelegatorService
    ) {
        this.trackyEventQueryService = trackyEventQueryService;
        this.aggregationDelegatorService = aggregationDelegatorService;
    }

    @GetMapping("/events")
    public ResponseEntity<TrackysatEventDTO> getOne() {
        log.debug("REST request to get one TrackysatEventDTO random");
        return ResponseUtil.wrapOrNotFound(trackyEventQueryService.getOne());
    }

    @GetMapping("/events/{id}")
    public ResponseEntity<List<TrackysatEventDTO>> getList(
        @org.springdoc.api.annotations.ParameterObject Pageable pageable,
        @PathVariable String id,
        @RequestParam("from") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) String from,
        @RequestParam("to") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) String to
    ) {
        log.debug("REST request to get a page of TrackysatEventDTO by deviceId: {}, {}, {}", id, from, to);
        Instant fromDate = Instant.parse(from);
        Instant toDate = Instant.parse(to);
        List<TrackysatEventDTO> trackyEvents = trackyEventQueryService.getByDeviceIdAndDateRange(id, fromDate, toDate);

        final int start = (int) pageable.getOffset();
        final int end = Math.min((start + pageable.getPageSize()), trackyEvents.size());
        final Page<TrackysatEventDTO> page = new PageImpl<>(trackyEvents.subList(start, end), pageable, trackyEvents.size());

        HttpHeaders headers = PaginationUtil.generatePaginationHttpHeaders(ServletUriComponentsBuilder.fromCurrentRequest(), page);
        return ResponseEntity.ok().headers(headers).body(page.getContent());
    }

    @GetMapping("/data")
    public ResponseEntity<DailyAggregationDTO> getOneAggregation() {
        log.debug("REST request to get one DailyAggregationDTO random");
        return ResponseUtil.wrapOrNotFound(aggregationDelegatorService.getOne());
    }

    @GetMapping("/data/{id}")
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

    @GetMapping("/distance/{id}")
    public ResponseEntity<Map<String, Double>> getTotalDistance(
        @PathVariable String id,
        @RequestParam("from") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) String from,
        @RequestParam("to") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) String to
    ) {
        log.debug("REST request to get total distance by deviceId: {}, {}, {}", id, from, to);
        Instant fromDate = Instant.parse(from);
        Instant toDate = Instant.parse(to);
        try {
            Map<String, SensorStatsDTO> sensors = aggregationDelegatorService.getSensorsByDeviceIdAndDateRange(id, fromDate, toDate);
            Map<String, Double> distance = sensors
                .entrySet()
                .stream()
                .filter(e -> e.getKey().contains("TotalVehicleDistance"))
                .collect(Collectors.toMap(Map.Entry::getKey, v -> v.getValue().getMax() - v.getValue().getMin()));
            return ResponseEntity.ok().body(distance);
        } catch (JsonProcessingException e) {
            return ResponseEntity.unprocessableEntity().body(null);
        }
    }

    @GetMapping("/data/{id}/sensors")
    public ResponseEntity<List<SensorStatsDTO>> getSensorList(
        @PathVariable String id,
        @RequestParam("from") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) String from,
        @RequestParam("to") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) String to
    ) {
        log.debug("REST request to getSensorList by deviceId: {}, {}, {}", id, from, to);
        Instant fromDate = Instant.parse(from);
        Instant toDate = Instant.parse(to);
        try {
            Map<String, SensorStatsDTO> sensors = aggregationDelegatorService.getSensorsByDeviceIdAndDateRange(id, fromDate, toDate);
            return ResponseEntity.ok().body(new ArrayList<>(sensors.values()));
        } catch (JsonProcessingException e) {
            return ResponseEntity.unprocessableEntity().body(null);
        }
    }

    @GetMapping("/data/{id}/sensors/{sensor}")
    public ResponseEntity<Map<String, SensorStatsDTO>> getFilteredSensor(
        @PathVariable String id,
        @PathVariable String sensor,
        @RequestParam("from") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) String from,
        @RequestParam("to") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) String to
    ) {
        log.debug("REST request to getFilteredSensor by deviceId: {}, {}, {}, {}", id, from, to, sensor);
        Instant fromDate = Instant.parse(from);
        Instant toDate = Instant.parse(to);
        try {
            Map<String, SensorStatsDTO> sensors = aggregationDelegatorService.getSensorsByDeviceIdAndDateRange(id, fromDate, toDate);
            Map<String, SensorStatsDTO> stats = sensors
                .entrySet()
                .stream()
                .filter(e -> e.getKey().toLowerCase().contains(sensor.toLowerCase()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            return ResponseEntity.ok().body(stats);
        } catch (JsonProcessingException e) {
            return ResponseEntity.unprocessableEntity().body(null);
        }
    }
}
