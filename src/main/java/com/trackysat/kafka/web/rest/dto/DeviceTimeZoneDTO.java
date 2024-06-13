package com.trackysat.kafka.web.rest.dto;

import java.util.Map;

public class DeviceTimeZoneDTO {

    private Map<String, String> deviceZoneMap;

    public Map<String, String> getDeviceZoneMap() {
        return deviceZoneMap;
    }

    public void setDeviceZoneMap(Map<String, String> deviceZoneMap) {
        this.deviceZoneMap = deviceZoneMap;
    }
}
