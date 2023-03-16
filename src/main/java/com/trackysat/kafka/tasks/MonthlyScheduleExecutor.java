package com.trackysat.kafka.tasks;

import com.trackysat.kafka.service.JobStatusService;
import com.trackysat.kafka.service.TrackyEventQueryService;
import com.trackysat.kafka.utils.DateUtils;
import java.time.Instant;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class MonthlyScheduleExecutor {

    private static final Logger log = LoggerFactory.getLogger(MonthlyScheduleExecutor.class);

    private final TrackyEventQueryService trackyEventQueryService;

    private final JobStatusService jobStatusService;

    public MonthlyScheduleExecutor(TrackyEventQueryService trackyEventQueryService, JobStatusService jobStatusService) {
        this.trackyEventQueryService = trackyEventQueryService;
        this.jobStatusService = jobStatusService;
    }

    @Scheduled(cron = "0 1 * * * *")
    public void monthlyProcess() {
        Instant startDate = Instant.now();
        log.info("Started at " + startDate.toString());
        Optional<Instant> lastDay = jobStatusService.getLastMonthProcessed();
        if (lastDay.isEmpty()) {
            log.debug("Last day was not present, set to yesterday");
        } else {
            log.debug("Last day processed: " + lastDay.toString());
        }
        DateUtils
            .getDatesBetween(lastDay.orElse(DateUtils.yesterday()), startDate)
            .forEach(d -> {
                log.debug("Started processing month " + d.getMonth().toString());
                trackyEventQueryService.processMonth(d);
                log.debug("Finished processing month " + d.getMonth().toString());
                jobStatusService.setLastMonthProcessed(d, null);
            });
        Instant endDate = Instant.now();
        log.info("Finished at {}, in {}ms", endDate.toString(), endDate.toEpochMilli() - startDate.toEpochMilli());
    }
}
