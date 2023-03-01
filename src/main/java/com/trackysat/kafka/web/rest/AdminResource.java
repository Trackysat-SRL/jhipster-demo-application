package com.trackysat.kafka.web.rest;

import com.trackysat.kafka.config.Constants;
import com.trackysat.kafka.domain.User;
import com.trackysat.kafka.repository.UserRepository;
import com.trackysat.kafka.security.AuthoritiesConstants;
import com.trackysat.kafka.service.CassandraConsumerService;
import com.trackysat.kafka.service.MailService;
import com.trackysat.kafka.service.UserService;
import com.trackysat.kafka.service.dto.AdminUserDTO;
import com.trackysat.kafka.service.dto.StatusDTO;
import com.trackysat.kafka.web.rest.errors.BadRequestAlertException;
import com.trackysat.kafka.web.rest.errors.EmailAlreadyUsedException;
import com.trackysat.kafka.web.rest.errors.LoginAlreadyUsedException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import javax.validation.Valid;
import javax.validation.constraints.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;
import tech.jhipster.web.util.HeaderUtil;
import tech.jhipster.web.util.ResponseUtil;

@RestController
@RequestMapping("/api/admin")
public class AdminResource {

    private final Logger log = LoggerFactory.getLogger(AdminResource.class);

    @Value("${jhipster.clientApp.name}")
    private String applicationName;

    private final CassandraConsumerService cassandraConsumerService;

    public AdminResource(CassandraConsumerService cassandraConsumerService) {
        this.cassandraConsumerService = cassandraConsumerService;
    }

    @GetMapping("/status")
    public ResponseEntity<StatusDTO> getStatus() {
        log.debug("REST request to get status");
        return ResponseUtil.wrapOrNotFound(cassandraConsumerService.getStatus());
    }

    @PutMapping("/reset")
    public ResponseEntity<Boolean> reset() {
        log.debug("REST request to reset consumer");
        return new ResponseEntity<Boolean>(cassandraConsumerService.reset(), new HttpHeaders(), HttpStatus.OK);
    }

    @PutMapping("/stop")
    public ResponseEntity<StatusDTO> stop() {
        log.debug("REST request to stop consumer");
        cassandraConsumerService.setIsEnabled(false);
        return ResponseUtil.wrapOrNotFound(cassandraConsumerService.getStatus());
    }
}
