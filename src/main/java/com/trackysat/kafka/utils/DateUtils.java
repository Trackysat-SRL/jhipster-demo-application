package com.trackysat.kafka.utils;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.stream.Collectors;

public class DateUtils {

    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");

    // make startDate exclusive with 1 DAY added to startDate
    public static List<LocalDate> getDatesBetween(Instant startDate, Instant endDate) {
        return LocalDate
            .ofInstant(startDate.plus(1, ChronoUnit.DAYS), ZoneId.systemDefault())
            .datesUntil(LocalDate.ofInstant(endDate, ZoneId.systemDefault()))
            .collect(Collectors.toList());
    }

    public static Instant yesterday() {
        Instant now = Instant.now();
        return now.minus(1, ChronoUnit.DAYS);
    }
}
