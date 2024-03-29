package com.trackysat.kafka.web.rest;

import com.trackysat.kafka.service.TrackyEventQueryService;
import com.trackysat.kafka.service.dto.TrackysatEventDTO;
import java.time.Instant;
import java.util.List;
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

    public TrackyEventQueryResource(TrackyEventQueryService trackyEventQueryService) {
        this.trackyEventQueryService = trackyEventQueryService;
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
}
