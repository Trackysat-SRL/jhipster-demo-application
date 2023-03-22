package com.trackysat.kafka.tasks;

import com.trackysat.kafka.domain.Device;
import com.trackysat.kafka.service.DeviceService;
import com.trackysat.kafka.service.JobStatusService;
import com.trackysat.kafka.service.TrackyEventQueryService;
import com.trackysat.kafka.utils.DateUtils;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
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

    private final DeviceService deviceService;

    public DailyScheduleExecutor(
        TrackyEventQueryService trackyEventQueryService,
        JobStatusService jobStatusService,
        DeviceService deviceService
    ) {
        this.trackyEventQueryService = trackyEventQueryService;
        this.jobStatusService = jobStatusService;
        this.deviceService = deviceService;
    }

    @Scheduled(cron = "0 */3 * * * *")
    public void processAllDevices() {
        deviceService.getAll().forEach(this::dailyProcess);
    }

    public void dailyProcess(Device device) {
        Instant startDate = Instant.now();
        log.info("[{}] Started at " + startDate.toString(), device.getUid());
        Optional<Instant> lastDay = jobStatusService.getLastDayProcessed(device.getUid());
        if (lastDay.isEmpty()) {
            log.debug("[{}] Last day was not present, set to yesterday", device.getUid());
        } else {
            log.debug("[{}] Last day processed: " + lastDay, device.getUid());
        }
        List<LocalDate> days = DateUtils.getDaysBetween(lastDay.orElse(DateUtils.twoDaysAgo()), startDate);
        log.debug("[{}] Days to process: " + days.size(), device.getUid());
        for (LocalDate d : days) {
            try {
                log.debug("[{}] Started processing day " + d.toString(), device.getUid());
                trackyEventQueryService.processDay(device.getUid(), d);
                log.debug("[{}] Finished processing day " + d, device.getUid());
                jobStatusService.setLastDayProcessed(device.getUid(), d, null);
            } catch (Exception e) {
                log.error("[{}] [{}] Error processing day. ERROR: {}", device.getUid(), d, e.getMessage());
            }
        }
        Instant endDate = Instant.now();
        log.info("[{}] Finished at {}, in {}ms", device.getUid(), endDate.toString(), endDate.toEpochMilli() - startDate.toEpochMilli());
    }
}
