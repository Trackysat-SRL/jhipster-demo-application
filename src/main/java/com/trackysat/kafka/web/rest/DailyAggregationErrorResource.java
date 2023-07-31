package com.trackysat.kafka.web.rest;

import com.trackysat.kafka.domain.DailyAggregationError;
import com.trackysat.kafka.service.AggregationDelegatorService;
import com.trackysat.kafka.service.DailyAggregationErrorService;
import java.time.Instant;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;
import tech.jhipster.web.util.HeaderUtil;
import tech.jhipster.web.util.PaginationUtil;

@RestController
@RequestMapping("/api")
public class DailyAggregationErrorResource {

    private final Logger log = LoggerFactory.getLogger(DailyAggregationErrorResource.class);

    private final DailyAggregationErrorService dailyAggregationErrorService;

    private final AggregationDelegatorService aggregationDelegatorService;

    public DailyAggregationErrorResource(
        DailyAggregationErrorService dailyAggregationErrorService,
        AggregationDelegatorService aggregationDelegatorService
    ) {
        this.dailyAggregationErrorService = dailyAggregationErrorService;
        this.aggregationDelegatorService = aggregationDelegatorService;
    }

    @GetMapping("/errors")
    public ResponseEntity<List<DailyAggregationError>> getList(@org.springdoc.api.annotations.ParameterObject Pageable pageable) {
        log.debug("REST request to get a page of Daily Error list");
        List<DailyAggregationError> error = dailyAggregationErrorService.getAll();

        final int start = (int) pageable.getOffset();
        final int end = Math.min((start + pageable.getPageSize()), error.size());
        final Page<DailyAggregationError> page = new PageImpl<>(error.subList(start, end), pageable, error.size());

        HttpHeaders headers = PaginationUtil.generatePaginationHttpHeaders(ServletUriComponentsBuilder.fromCurrentRequest(), page);
        return ResponseEntity.ok().headers(headers).body(page.getContent());
    }

    @GetMapping("/errors/{idDevice}")
    public ResponseEntity<Boolean> recoveryError(
        @PathVariable String idDevice,
        @RequestParam("date") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) String date
    ) {
        log.debug("REST request to recovery error by device : {} and {}", idDevice, date);
        boolean result = aggregationDelegatorService.recoveryDailyError(idDevice, Instant.parse(date));
        return ResponseEntity.ok().body(result);
    }

    @DeleteMapping("/errors/{idDevice}")
    public ResponseEntity<Void> deleteOne(
        @PathVariable String idDevice,
        @RequestParam("date") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) String date
    ) {
        log.debug("REST request to delete error by idDevice: {} and date {} ", idDevice, date);
        DailyAggregationError error = new DailyAggregationError();
        error.setDeviceId(idDevice);
        error.setAggregatedDate(Instant.parse(date));

        dailyAggregationErrorService.delete(error);

        return ResponseEntity.noContent().build();
    }
}
