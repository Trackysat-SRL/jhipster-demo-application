package com.trackysat.kafka.utils;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.stream.Collectors;

public class DateUtils {

    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");

    public static List<LocalDate> getDaysBetweenIncludeStart(Instant startDate, Instant endDate) {
        return LocalDate
            .ofInstant(startDate, ZoneId.systemDefault())
            .datesUntil(LocalDate.ofInstant(endDate, ZoneId.systemDefault()))
            .collect(Collectors.toList());
    }

    public static List<LocalDate> getDaysBetween(Instant startDate, Instant endDate) {
        // make startDate exclusive with 1 DAY added to startDate
        Instant startDateFixed = startDate.plus(1, ChronoUnit.DAYS);
        return LocalDate
            .ofInstant(startDateFixed, ZoneId.systemDefault())
            .datesUntil(LocalDate.ofInstant(endDate, ZoneId.systemDefault()))
            .collect(Collectors.toList());
    }

    public static List<LocalDate> getMonthsBetween(Instant startDate, Instant endDate) {
        return LocalDate
            .ofInstant(startDate.plus(1, ChronoUnit.DAYS), ZoneId.systemDefault())
            .datesUntil(LocalDate.ofInstant(endDate, ZoneId.systemDefault()))
            .collect(Collectors.toList());
    }

    public static Instant twoDaysAgo() {
        Instant now = Instant.now();
        return now.minus(7, ChronoUnit.DAYS);
    }

    public static Instant twoMonthAgo() {
        Instant now = Instant.now();
        return now.minus(2, ChronoUnit.MONTHS);
    }

    public static Instant atStartOfDate(Instant dateFrom) {
        return LocalDate.ofInstant(dateFrom, ZoneOffset.UTC).atStartOfDay().toInstant(ZoneOffset.UTC);
    }
}
