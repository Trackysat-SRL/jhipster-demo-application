package com.trackysat.kafka.tasks;

import com.trackysat.kafka.domain.Device;
import com.trackysat.kafka.service.DeviceService;
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

    private final DeviceService deviceService;

    public MonthlyScheduleExecutor(
        TrackyEventQueryService trackyEventQueryService,
        JobStatusService jobStatusService,
        DeviceService deviceService
    ) {
        this.trackyEventQueryService = trackyEventQueryService;
        this.jobStatusService = jobStatusService;
        this.deviceService = deviceService;
    }

    @Scheduled(cron = "0 1 * * * *")
    public void processAllDevices() {
        deviceService.getAll().forEach(this::monthlyProcess);
    }

    public void monthlyProcess(Device device) {
        Instant startDate = Instant.now();
        log.info("[{}] Started at " + startDate.toString(), device.getUid());
        Optional<Instant> lastDay = jobStatusService.getLastMonthProcessed(device.getUid());
        if (lastDay.isEmpty()) {
            log.debug("[{}] Last month was not present, set to yesterday", device.getUid());
        } else {
            log.debug("[{}] Last month processed: " + lastDay, device.getUid());
        }
        DateUtils
            .getMonthsBetween(lastDay.orElse(DateUtils.lastMonth()), startDate)
            .forEach(d -> {
                try {
                    log.debug("[{}] Started processing month " + d.getMonth().toString(), device.getUid());
                    trackyEventQueryService.processMonth(device.getUid(), d);
                    log.debug("[{}] Finished processing month " + d.getMonth().toString(), device.getUid());
                    jobStatusService.setLastMonthProcessed(device.getUid(), d, null);
                } catch (Exception e) {
                    log.error("[{}] Error processing month " + d.getMonth().toString(), device.getUid());
                }
            });
        Instant endDate = Instant.now();
        log.info("[{}] Finished at {}, in {}ms", device.getUid(), endDate.toString(), endDate.toEpochMilli() - startDate.toEpochMilli());
    }
}
