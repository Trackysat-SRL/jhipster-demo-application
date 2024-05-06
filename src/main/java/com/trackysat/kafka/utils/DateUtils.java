package com.trackysat.kafka.utils;

import java.text.SimpleDateFormat;
import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.*;
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
        Map<Integer, LocalDate> months = new TreeMap<>();
        LocalDate now = LocalDate.now();
        LocalDate
            .ofInstant(startDate.plus(1, ChronoUnit.DAYS), ZoneId.systemDefault())
            .datesUntil(LocalDate.ofInstant(endDate, ZoneId.systemDefault()))
            .forEach(d -> {
                if (d.getMonth() != now.getMonth()) {
                    //Essendo un mese diverso la data che viene passata è quella del giorno 1
                    months.put(d.getMonthValue(), d);
                } else {
                    months.put(d.getMonthValue(), d);
                }
            });
        return new ArrayList<>(months.values());
    }

    public static Instant twoDaysAgo() {
        Instant now = Instant.now();
        return now.minus(7, ChronoUnit.DAYS);
    }

    public static Instant twoMonthAgo() {
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.MONTH, -2);
        return cal.getTime().toInstant();
    }

    public static Instant atStartOfDate(Instant dateFrom) {
        return LocalDate.ofInstant(dateFrom, ZoneOffset.UTC).atStartOfDay().toInstant(ZoneOffset.UTC);
    }

    public static Instant atEndOfDate(Instant dateFrom) {
        return LocalDate.ofInstant(dateFrom, ZoneOffset.UTC).atTime(23, 59).toInstant(ZoneOffset.UTC);
    }

    public static Instant atStartOfMonth(Instant date) {
        String month = String.valueOf(LocalDate.ofInstant(date, ZoneOffset.UTC).getMonthValue());
        if (!month.equals(10) || !month.equals(11) || !month.equals(12)) month = "0".concat(month);
        int year = LocalDate.ofInstant(date, ZoneOffset.UTC).getYear();
        String firstDayMonth = String.valueOf(year).concat("-").concat(month).concat("-").concat("01").concat("T00:00:00Z");
        return Instant.parse(firstDayMonth);
    }
}
