package com.trackysat.kafka.web.rest;

import com.trackysat.kafka.domain.JobStatus;
import com.trackysat.kafka.service.JobStatusService;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;
import tech.jhipster.web.util.PaginationUtil;
import tech.jhipster.web.util.ResponseUtil;

@RestController
@RequestMapping("/api")
public class JobStatusResource {

    private final Logger log = LoggerFactory.getLogger(JobStatusResource.class);

    @Value("${jhipster.clientApp.name}")
    private String applicationName;

    private final JobStatusService jobStatusService;

    public JobStatusResource(JobStatusService jobStatusService) {
        this.jobStatusService = jobStatusService;
    }

    @GetMapping("/jobs")
    public ResponseEntity<List<JobStatus>> getList(@org.springdoc.api.annotations.ParameterObject Pageable pageable) {
        log.debug("REST request to get a page of JobStatus list");
        List<JobStatus> devices = jobStatusService.getAll();

        final int start = (int) pageable.getOffset();
        final int end = Math.min((start + pageable.getPageSize()), devices.size());
        final Page<JobStatus> page = new PageImpl<>(devices.subList(start, end), pageable, devices.size());

        HttpHeaders headers = PaginationUtil.generatePaginationHttpHeaders(ServletUriComponentsBuilder.fromCurrentRequest(), page);
        return ResponseEntity.ok().headers(headers).body(page.getContent());
    }

    @GetMapping("/jobs/{id}")
    public ResponseEntity<JobStatus> getOne(@PathVariable String id) {
        log.debug("REST request to get a JobStatus by id: {}", id);
        return ResponseUtil.wrapOrNotFound(jobStatusService.getOne(id));
    }

    @DeleteMapping("/jobs/{id}")
    public ResponseEntity<Boolean> deleteOne(@PathVariable String id) {
        log.debug("REST request to delete JobStatus by id: {}", id);
        return ResponseEntity.ok().body(jobStatusService.deleteOne(id));
    }
}
