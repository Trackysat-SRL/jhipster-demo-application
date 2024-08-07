package com.trackysat.kafka.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.trackysat.kafka.domain.DailyAggregationError;
import com.trackysat.kafka.domain.Device;
import com.trackysat.kafka.domain.aggregations.PositionDTO;
import com.trackysat.kafka.domain.aggregations.SensorStatsDTO;
import com.trackysat.kafka.domain.aggregations.SensorValDTO;
import com.trackysat.kafka.service.dto.DailyAggregationDTO;
import com.trackysat.kafka.service.dto.TrackysatEventDTO;
import com.trackysat.kafka.service.mapper.DailyAggregationMapper;
import com.trackysat.kafka.utils.DateUtils;
import com.trackysat.kafka.web.rest.dto.AggregationErrorResponseDTO;
import java.time.*;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class AggregationDelegatorService {

    private static final int MAX_DAY_TO_USE_DAILY = 31;

    private final Logger log = LoggerFactory.getLogger(AggregationDelegatorService.class);

    private final DailyAggregationMapper dailyAggregationMapper;

    private final TrackyEventQueryService trackyEventQueryService;

    private final DailyAggregationService dailyAggregationService;

    private final MonthlyAggregationService monthlyAggregationService;

    private final JobStatusService jobStatusService;

    private final DailyAggregationErrorService dailyAggregationErrorService;

    private final DeviceService deviceService;

    public AggregationDelegatorService(
        TrackyEventQueryService trackyEventQueryService,
        DailyAggregationMapper dailyAggregationMapper,
        DailyAggregationService dailyAggregationService,
        MonthlyAggregationService monthlyAggregationService,
        JobStatusService jobStatusService,
        DailyAggregationErrorService dailyAggregationErrorService,
        DeviceService deviceService
    ) {
        this.trackyEventQueryService = trackyEventQueryService;
        this.dailyAggregationMapper = dailyAggregationMapper;
        this.dailyAggregationService = dailyAggregationService;
        this.monthlyAggregationService = monthlyAggregationService;
        this.jobStatusService = jobStatusService;
        this.dailyAggregationErrorService = dailyAggregationErrorService;
        this.deviceService = deviceService;
    }

    public List<PositionDTO> getPositionsByDeviceIdAndDateRange(String deviceId, Instant dateFrom, Instant dateTo)
        throws JsonProcessingException {
        return getByDeviceIdAndDateRange(deviceId, dateFrom, dateTo)
            .stream()
            .map(DailyAggregationDTO::getPositions)
            .flatMap(List::stream)
            .collect(Collectors.toList());
    }

    public Map<String, SensorStatsDTO> getSensorsByDeviceIdAndDateRange(String deviceId, Instant dateFrom, Instant dateTo)
        throws JsonProcessingException {
        return getByDeviceIdAndDateRange(deviceId, dateFrom, dateTo)
            .stream()
            .map(DailyAggregationDTO::getSensors)
            .map(Map::entrySet)
            .flatMap(Set::stream)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, dailyAggregationService::mergeSensorMaps));
    }

    public List<DailyAggregationDTO> getByDeviceIdAndDateRange(String deviceId, Instant dateFrom, Instant dateTo)
        throws JsonProcessingException {
        List<LocalDate> days = DateUtils.getDaysBetweenIncludeStart(dateFrom, dateTo);
        if (days.size() == 0) {
            if (dateFrom.isAfter(LocalDate.now().atStartOfDay().toInstant(ZoneOffset.UTC))) {
                log.info("[{}] Using real time data", deviceId);
                return this.aggregateRealTime(deviceId, dateFrom, dateTo);
            } else {
                log.info("[{}] Using daily aggregation data filter by hours", deviceId);
                List<DailyAggregationDTO> dateData = dailyAggregationService.getByDeviceIdAndDateRange(deviceId, dateFrom, dateTo);
                return dateData
                    .stream()
                    .findFirst()
                    .map(dateDataForDay -> filterHoursInDailyAggregation(dateDataForDay, dateFrom, dateTo))
                    .map(List::of)
                    .orElse(new ArrayList<>());
            }
        } /* else if (days.size() < MAX_DAY_TO_USE_DAILY) {
            log.info("[{}] Using daily aggregation data", deviceId);
            return dailyAggregationService
                .getByDeviceIdAndDateRange(deviceId, dateFrom, dateTo)
                .stream()
                .map(da -> filterHoursInDailyAggregation(da, dateFrom, dateTo))
                .collect(Collectors.toList());
        } */else {
            log.info("[{}] Using monthly aggregation data", deviceId);
            Instant dayStartMonth = DateUtils.atStartOfMonth(dateFrom);
            return monthlyAggregationService
                .getByDeviceIdAndDateRange(deviceId, DateUtils.atStartOfDate(dayStartMonth), dateTo)
                .stream()
                .map(da -> filterHoursInDailyAggregation(da, dateFrom, dateTo))
                .collect(Collectors.toList());
        }
    }

    /**
     * @param deviceId
     * @param dateFrom
     * @param dateTo
     * @return
     * @throws JsonProcessingException
     */
    //Servizio che va ad interrogare solo la tabella dailyAggregation
    public Map<String, SensorStatsDTO> getSensorsByDeviceIdAndSingleDay(String deviceId, Instant dateFrom, Instant dateTo)
        throws JsonProcessingException {
        return getByDeviceIdFromDailyAggr(deviceId, dateFrom, dateTo)
            .stream()
            .map(DailyAggregationDTO::getSensors)
            .map(Map::entrySet)
            .flatMap(Set::stream)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, dailyAggregationService::mergeSensorMaps));
    }

    /**
     * @param deviceId
     * @param dateFrom
     * @param dateTo
     * @return List<DailyAggregationDTO>
     * @throws JsonProcessingException
     */
    public List<DailyAggregationDTO> getByDeviceIdFromDailyAggr(String deviceId, Instant dateFrom, Instant dateTo)
        throws JsonProcessingException {
        return dailyAggregationService
            .getByDeviceIdAndSingleDay(deviceId, dateFrom)
            .stream()
            .map(da -> filterHoursInDailyAggregation(da, dateFrom, dateTo))
            .collect(Collectors.toList());
    }

    private DailyAggregationDTO filterHoursInDailyAggregation(DailyAggregationDTO dailyAggregationDTO, Instant dateFrom, Instant dateTo) {
        Duration duration = Duration.between(dateFrom, dateTo);
        long days = duration.toDays();
        List<PositionDTO> filteredPositions = dailyAggregationDTO
            .getPositions()
            .stream()
            .filter(positionDTO -> positionDTO.getTimestamp().isAfter(dateFrom) && positionDTO.getTimestamp().isBefore(dateTo))
            .collect(Collectors.toList());
        dailyAggregationDTO.setPositions(filteredPositions);
        for (Map.Entry<String, SensorStatsDTO> sensor : dailyAggregationDTO.getSensors().entrySet()) {
            List<SensorValDTO> filteredValues = sensor
                .getValue()
                .getValues()
                .stream()
                .filter(v -> v.getCreationDate().isAfter(dateFrom) && v.getCreationDate().isBefore(dateTo))
                .collect(Collectors.toList());
            sensor.getValue().setValues(filteredValues);
            sensor.getValue().recalculate((int) days); // passo la differenza tra i giorni in modo che il metodo utilizzi per i calcoli i giorni corretti e non solo
            // i giorni dei valori trovati
        }
        return dailyAggregationDTO;
    }

    private List<DailyAggregationDTO> aggregateRealTime(String deviceId, Instant dateFrom, Instant dateTo) throws JsonProcessingException {
        List<TrackysatEventDTO> events = trackyEventQueryService.getByDeviceIdAndDateRange(deviceId, dateFrom, dateTo);
        return Stream
            .of(dailyAggregationService.buildDailyAggregation(deviceId, dateFrom, events))
            .map(dailyAggregationMapper::toDTO)
            .collect(Collectors.toList());
    }

    public Optional<DailyAggregationDTO> getOne() {
        return dailyAggregationService.getOne().map(dailyAggregationMapper::toDTO);
    }

    public Map<String, SensorStatsDTO> getLastValueSensorsByDeviceIdAndDateRange(String deviceId, Instant dateFrom, Instant dateTo)
        throws JsonProcessingException {
        log.info("[{}] getLastValueSensorsByDeviceIdAndDateRange {} {}", deviceId, dateFrom, dateTo);
        Instant dayStartMonth = DateUtils.atStartOfMonth(dateFrom);
        return monthlyAggregationService
            .getByDeviceIdAndDateRange(deviceId, DateUtils.atStartOfDate(dayStartMonth), dateTo)
            .stream()
            .map(DailyAggregationDTO::getSensors)
            .map(Map::entrySet)
            .flatMap(Set::stream)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, dailyAggregationService::mergeSensorMaps));
    }

    // == PROCESS ==

    public void dailyProcess(String deviceId, String timezone) {
        Instant startDate = Instant.now();
        log.info("[{}] Started dailyProcess at " + startDate.toString(), deviceId);

        Optional<Instant> lastDay = jobStatusService.getLastDayProcessed(deviceId);
        if (lastDay.isEmpty()) {
            log.debug("[{}] Last day was not present, set to yesterday", deviceId);
        } else {
            log.debug("[{}] Last day processed: " + lastDay, deviceId);
        }
        List<LocalDate> days = DateUtils.getDaysBetween(lastDay.orElse(DateUtils.twoDaysAgo()), startDate);
        log.debug("[{}] Days to process: " + days.size(), deviceId);
        for (LocalDate d : days) {
            try {
                log.debug("[{}] Started processing day " + d.toString(), deviceId);
                trackyEventQueryService.processDay(deviceId, d, Objects.isNull(timezone) || timezone.isEmpty() ? "UTC" : timezone);
                log.debug("[{}] Finished processing day " + d, deviceId);
                jobStatusService.setLastDayProcessed(deviceId, d, null);
            } catch (Exception e) {
                //save error in daily_aggregation_error
                DailyAggregationError error = new DailyAggregationError(
                    deviceId,
                    d.atStartOfDay().toInstant(ZoneOffset.UTC),
                    e.getMessage()
                );
                this.dailyAggregationErrorService.save(error);
                log.error("[{}] [{}] Error processing day. ERROR: {}", deviceId, d, e.getMessage());
            }
        }
        Instant endDate = Instant.now();
        log.info(
            "[{}] Finished dailyProcess at {}, in {}ms",
            deviceId,
            endDate.toString(),
            endDate.toEpochMilli() - startDate.toEpochMilli()
        );
    }

    public AggregationErrorResponseDTO retryAggregateProcessForDevices(List<String> deviceIds, Instant from, Instant to) {
        var devices = deviceService.getAllByIdIn(deviceIds);
        List<LocalDate> days = DateUtils.getDaysBetween(from, to);
        List<LocalDate> months = DateUtils.getMonthsBetween(from, to);
        var errors = new HashMap<String, List<String>>();
        BiConsumer<String, String> addErrorMessage = (key, errorMessage) ->
            errors.compute(
                key,
                (k, v) -> {
                    var value = v == null ? new ArrayList<String>() : v;
                    value.add(errorMessage);
                    return value;
                }
            );

        log.debug("{} Day/s and {} month/s to process for devices: {}", days.size(), months.size(), deviceIds);

        devices.forEach(device -> {
            var deviceId = device.getUid();
            var timezone = device.getTimezone();

            days.forEach(day -> {
                try {
                    trackyEventQueryService.processDay(deviceId, day, Objects.isNull(timezone) || timezone.isEmpty() ? "UTC" : timezone);
                } catch (Exception e) {
                    DailyAggregationError error = new DailyAggregationError(
                        deviceId,
                        day.atStartOfDay().toInstant(ZoneOffset.UTC),
                        e.getMessage()
                    );
                    this.dailyAggregationErrorService.save(error);
                    addErrorMessage.accept(deviceId, String.format("ERROR DAILY PROCESS [%s]: %s", day, e.getMessage()));
                }
            });

            months.forEach(month -> {
                try {
                    var firstDayOfMonth = month.withDayOfMonth(1).atStartOfDay();
                    var lastDayOfMonth = YearMonth.from(firstDayOfMonth).atEndOfMonth().atStartOfDay().toInstant(ZoneOffset.UTC);
                    trackyEventQueryService.processMonth(deviceId, month, firstDayOfMonth.toInstant(ZoneOffset.UTC), lastDayOfMonth);
                } catch (Exception e) {
                    addErrorMessage.accept(deviceId, String.format("ERROR MONTHLY PROCESS [%s]: %s", month, e.getMessage()));
                }
            });
        });

        return new AggregationErrorResponseDTO(errors);
    }

    public boolean recoveryDailyError(String deviceId, Instant date) {
        Instant startDate = Instant.now();
        boolean recovery = false;
        log.info("[{}] Started recoveryDailyError at " + startDate.toString(), deviceId);
        Optional<Instant> lastDay = jobStatusService.getLastDayProcessed(deviceId);
        LocalDate d = LocalDate.ofInstant(date, ZoneId.systemDefault());
        Optional<Device> device = deviceService.getOne(deviceId);
        String timezone = device.isPresent() ? device.get().getTimezone() : "UTC";
        try {
            log.info("[{}] Started processing day " + d.toString(), deviceId);
            trackyEventQueryService.processDay(deviceId, d, timezone);
            log.info("[{}] Finished processing day " + d, deviceId);
            if (lastDay.isEmpty() || lastDay.get().isBefore(date)) {
                log.info("[{}] Updated job status " + d, deviceId);
                jobStatusService.setLastDayProcessed(deviceId, d, null);
            }
            DailyAggregationError error = new DailyAggregationError();
            error.setAggregatedDate(date);
            error.setDeviceId(deviceId);
            this.dailyAggregationErrorService.delete(error);
            recovery = true;
        } catch (Exception e) {
            //save error in daily_aggregation_error
            DailyAggregationError error = new DailyAggregationError(deviceId, d.atStartOfDay().toInstant(ZoneOffset.UTC), e.getMessage());
            this.dailyAggregationErrorService.save(error);
            log.error("[{}] [{}] Error processing day. ERROR: {}", deviceId, d, e.getMessage());
        }
        Instant endDate = Instant.now();
        log.info(
            "[{}] Finished recoveryDailyError at {}, in {}ms",
            deviceId,
            endDate.toString(),
            endDate.toEpochMilli() - startDate.toEpochMilli()
        );

        return recovery;
    }

    public void monthlyProcess(String deviceId) {
        Instant startDate = Instant.now();
        log.info("[{}] Started monthlyProcess at " + startDate.toString(), deviceId);
        Optional<Instant> lastDay = jobStatusService.getLastMonthProcessed(deviceId);
        if (lastDay.isEmpty()) {
            log.debug("[{}] Last month was not present, set to last month", deviceId);
        } else {
            log.debug("[{}] Last month processed: " + lastDay, deviceId);
        }
        List<LocalDate> months = DateUtils.getMonthsBetween(lastDay.orElse(DateUtils.twoMonthAgo()), startDate);
        log.debug("[{}] Months to process: " + months.size(), deviceId);
        for (LocalDate d : months) {
            try {
                log.debug("[{}] Started processing month " + d.getMonth().toString(), deviceId);

                Instant startOfFirstDay = d.withDayOfMonth(1).atStartOfDay().toInstant(ZoneOffset.UTC);
                Instant endOfLastDay = DateUtils.atEndOfDate(d.atStartOfDay().toInstant(ZoneOffset.UTC));

                trackyEventQueryService.processMonth(deviceId, d, startOfFirstDay, endOfLastDay);
                log.debug("[{}] Finished processing month " + d.getMonth().toString(), deviceId);
                jobStatusService.setLastMonthProcessed(deviceId, d, null);
            } catch (Exception e) {
                log.error("[{}] [{}] Error processing month. ERROR: {}", deviceId, d.getMonth(), e.getMessage());
            }
        }
        Instant endDate = Instant.now();
        log.info(
            "[{}] Finished monthlyProcess at {}, in {}ms",
            deviceId,
            endDate.toString(),
            endDate.toEpochMilli() - startDate.toEpochMilli()
        );
    }

    public List<SensorStatsDTO> getSensorsSummaryByDeviceIdAndDateRange(
        List<String> ids,
        Instant fromDate,
        Instant toDate,
        Optional<String> nameFilter
    ) {
        List<SensorStatsDTO> summary = new ArrayList<>();
        Map<String, List<SensorStatsDTO>> mapOfSensors = new HashMap<>();
        Map<String, List<SensorStatsDTO>> allDevicesSensors = ids
            .stream()
            .distinct()
            .collect(Collectors.toList())
            .parallelStream()
            .collect(
                Collectors.toMap(
                    Function.identity(),
                    id -> {
                        try {
                            Map<String, SensorStatsDTO> sensors = this.getSensorsByDeviceIdAndDateRange(id, fromDate, toDate);
                            List<SensorStatsDTO> sensorList = new ArrayList<>();
                            for (Map.Entry<String, SensorStatsDTO> s : sensors.entrySet()) {
                                if (nameFilter.isEmpty() || s.getKey().toLowerCase().contains(nameFilter.get())) {
                                    sensorList.add(s.getValue());
                                }
                            }
                            return sensorList;
                        } catch (JsonProcessingException e) {
                            return Collections.emptyList();
                        }
                    }
                )
            );
        for (Map.Entry<String, List<SensorStatsDTO>> sensorByDevice : allDevicesSensors.entrySet()) {
            List<SensorValDTO> filteredValues = sensorByDevice
                .getValue()
                .stream()
                .map(SensorStatsDTO::getValues)
                .flatMap(List::stream)
                .collect(Collectors.toList());
            sensorByDevice
                .getValue()
                .stream()
                .findFirst()
                .ifPresent(s -> {
                    s.setValues(filteredValues);
                    s.recalculate(-1); //passo un valore negativo in modo che il metodo utilizzi solo il count dei valori trovati
                    String key = String.format("%s_%s_%s_%s_%s", s.getSource(), s.getName(), s.getMeasureUnit(), s.getType(), s.getSid());
                    List<SensorStatsDTO> val = mapOfSensors.containsKey(key) ? mapOfSensors.get(key) : new ArrayList<>();
                    val.add(s);
                    mapOfSensors.put(key, val);
                });
        }
        for (Map.Entry<String, List<SensorStatsDTO>> sensorByName : mapOfSensors.entrySet()) {
            summary.add(sensorByName.getValue().stream().reduce(new SensorStatsDTO(), this::mergeSensors));
        }
        return summary;
    }

    public SensorStatsDTO mergeSensors(SensorStatsDTO a, SensorStatsDTO b) {
        try {
            if (b.getFirstValue().getCreationDate().getEpochSecond() < a.getFirstValue().getCreationDate().getEpochSecond()) {
                a.setFirstValue(b.getFirstValue());
            }
            if (b.getLastValue().getCreationDate().getEpochSecond() > a.getLastValue().getCreationDate().getEpochSecond()) {
                a.setLastValue(b.getLastValue());
            }
            a.setSum(a.getSum() + b.getSum());
            if (b.getMin() < a.getMin()) {
                a.setMin(b.getMin());
            }
            if (b.getMax() < a.getMax()) {
                a.setMax(b.getMax());
            }
            a.setAvg((b.getAvg() + a.getAvg()) / 2);
            a.setDiff(a.getDiff() + b.getDiff());
            return a;
        } catch (Exception e) {
            return b;
        }
    }
}
