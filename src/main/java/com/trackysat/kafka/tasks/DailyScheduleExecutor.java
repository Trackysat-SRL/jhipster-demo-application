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

    @Scheduled(cron = "*/2 * * * * *")
    public void processAllDevices() {
        //        deviceService.getAll().forEach(this::dailyProcess);
        List.of(deviceService.getOne("359632104827701").orElseThrow()).forEach(this::dailyProcess);
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
        for (LocalDate d : DateUtils.getDaysBetween(lastDay.orElse(DateUtils.yesterday()), startDate)) {
            try {
                log.debug("[{}] Started processing day " + d.toString(), device.getUid());
                trackyEventQueryService.processDay(device.getUid(), d);
                log.debug("[{}] Finished processing day " + d.toString(), device.getUid());
                jobStatusService.setLastDayProcessed(device.getUid(), d, null);
            } catch (Exception e) {
                log.error("[{}] Error processing day " + d.toString(), device.getUid());
            }
        }
        Instant endDate = Instant.now();
        log.info("[{}] Finished at {}, in {}ms", device.getUid(), endDate.toString(), endDate.toEpochMilli() - startDate.toEpochMilli());
    }
}
