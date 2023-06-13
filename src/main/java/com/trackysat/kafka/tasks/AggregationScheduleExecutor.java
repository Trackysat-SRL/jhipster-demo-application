package com.trackysat.kafka.tasks;

import com.trackysat.kafka.domain.Device;
import com.trackysat.kafka.service.AggregationDelegatorService;
import com.trackysat.kafka.service.DeadLetterQueueService;
import com.trackysat.kafka.service.DeviceService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class AggregationScheduleExecutor {

    private static final Logger log = LoggerFactory.getLogger(AggregationScheduleExecutor.class);

    private final AggregationDelegatorService aggregationDelegatorService;

    private final DeviceService deviceService;

    private final DeadLetterQueueService deadLetterQueueService;

    public AggregationScheduleExecutor(
        AggregationDelegatorService aggregationDelegatorService,
        DeviceService deviceService,
        DeadLetterQueueService deadLetterQueueService
    ) {
        this.aggregationDelegatorService = aggregationDelegatorService;
        this.deviceService = deviceService;
        this.deadLetterQueueService = deadLetterQueueService;
    }

    @Scheduled(cron = "0 0 */3 * * *")
    public void processAllDevicesDaily() {
        deviceService.getAll().stream().map(Device::getUid).forEach(aggregationDelegatorService::dailyProcess);
    }

    @Scheduled(cron = "0 0 1 * * *")
    public void processAllDevicesMonthly() {
        deviceService.getAll().stream().map(Device::getUid).forEach(aggregationDelegatorService::monthlyProcess);
    }

    @Scheduled(cron = "0 0 */1 * * *")
    public void reprocessDLQ() {
        deadLetterQueueService.reprocess();
    }
}
