package com.trackysat.kafka.config;

/**
 * Application constants.
 */
public final class Constants {

    // Regex for acceptable logins
    public static final String LOGIN_REGEX = "^(?>[a-zA-Z0-9!$&*+=?^_`{|}~.-]+@[a-zA-Z0-9-]+(?:\\.[a-zA-Z0-9-]+)*)|(?>[_.@A-Za-z0-9-]+)$";

    public static final String SYSTEM = "system";
    public static final String DEFAULT_LANGUAGE = "en";
    public static final String SENSOR_TOT_VEHICLE_DIST = "TotalVehicleDistance";
    public static final String SENSOR_TIME_ENGINE_LIFE = "TimeEngineLife";
    public static final String SENSOR_TOT_FUEL = "TotalFuel";

    public static final String SENSOR_SERVICE_DISTANCE = "ServiceDistance";

    private Constants() {}
}
