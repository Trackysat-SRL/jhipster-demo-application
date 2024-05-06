package com.trackysat.kafka.tasks;

import com.trackysat.kafka.domain.Device;
import com.trackysat.kafka.service.AggregationDelegatorService;
import com.trackysat.kafka.service.DailyAggregationErrorService;
import com.trackysat.kafka.service.DeadLetterQueueService;
import com.trackysat.kafka.service.DeviceService;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class AggregationScheduleExecutor {

    private static final Logger log = LoggerFactory.getLogger(AggregationScheduleExecutor.class);

    private final AggregationDelegatorService aggregationDelegatorService;

    private final DeviceService deviceService;

    private final DeadLetterQueueService deadLetterQueueService;

    private final DailyAggregationErrorService dailyAggregationErrorService;

    @Value(value = "${kafka.aggregation.enabled}")
    private boolean enabled;

    public AggregationScheduleExecutor(
        AggregationDelegatorService aggregationDelegatorService,
        DeviceService deviceService,
        DeadLetterQueueService deadLetterQueueService,
        DailyAggregationErrorService dailyAggregationErrorService
    ) {
        this.aggregationDelegatorService = aggregationDelegatorService;
        this.deviceService = deviceService;
        this.deadLetterQueueService = deadLetterQueueService;
        this.dailyAggregationErrorService = dailyAggregationErrorService;
    }

    @Scheduled(cron = "0 0 */3 * * *")
    public void processAllDevicesDaily() {
        if (enabled) {
            AtomicInteger totDevice = new AtomicInteger(1);
            List<Device> listDev = deviceService
                .getAll()
                .stream()
                .filter(d -> Objects.nonNull(d.getCompanyname()) && d.getCompanyname().equals("CGT"))
                .collect(Collectors.toList());
            listDev.forEach(d -> {
                aggregationDelegatorService.dailyProcess(d.getUid());
                log.info("[{}] Elaborated {} of {}", d.getUid(), totDevice.getAndIncrement(), listDev.size());
            });
        }
    }

    @Scheduled(cron = "0 0 1 * * *")
    public void processAllDevicesMonthly() {
        if (enabled) {
            deviceService.getAll().stream().map(Device::getUid).forEach(aggregationDelegatorService::monthlyProcess);
        }
    }

    @Scheduled(cron = "0 0 */1 * * *")
    public void reprocessDLQ() {
        if (enabled) {
            deadLetterQueueService.reprocess();
        }
    }

    @Scheduled(cron = "0 0 4 * * *")
    public void recoveryAllDailyError() {
        if (enabled) {
            dailyAggregationErrorService
                .getAll()
                .forEach(d -> {
                    aggregationDelegatorService.recoveryDailyError(d.getDeviceId(), d.getAggregatedDate());
                });
        }
    }
}
