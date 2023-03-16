package com.trackysat.kafka.tasks;

import com.trackysat.kafka.service.JobStatusService;
import com.trackysat.kafka.service.TrackyEventQueryService;
import com.trackysat.kafka.utils.DateUtils;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class DailyScheduleExecutor {

    private static final Logger log = LoggerFactory.getLogger(DailyScheduleExecutor.class);

    private final TrackyEventQueryService trackyEventQueryService;

    private final JobStatusService jobStatusService;

    public DailyScheduleExecutor(TrackyEventQueryService trackyEventQueryService, JobStatusService jobStatusService) {
        this.trackyEventQueryService = trackyEventQueryService;
        this.jobStatusService = jobStatusService;
    }

    @Scheduled(cron = "0 1 * * * *")
    public void dailyProcess() {
        Instant startDate = Instant.now();
        log.info("Started at " + startDate.toString());
        Optional<Instant> lastDay = jobStatusService.getLastDayProcessed();
        if (lastDay.isEmpty()) {
            log.debug("Last day was not present, set to yesterday");
        } else {
            log.debug("Last day processed: " + lastDay.toString());
        }
        DateUtils
            .getDatesBetween(lastDay.orElse(DateUtils.yesterday()), startDate)
            .forEach(d -> {
                log.debug("Started processing day " + d.toString());
                trackyEventQueryService.processDay(d);
                log.debug("Finished processing day " + d.toString());
                jobStatusService.setLastDayProcessed(d, null);
            });
        Instant endDate = Instant.now();
        log.info("Finished at {}, in {}ms", endDate.toString(), endDate.toEpochMilli() - startDate.toEpochMilli());
    }
}
