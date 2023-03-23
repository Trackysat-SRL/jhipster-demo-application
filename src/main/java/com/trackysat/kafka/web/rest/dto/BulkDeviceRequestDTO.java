package com.trackysat.kafka.web.rest.dto;

import java.util.List;

public class BulkDeviceRequestDTO {

    List<String> devices;

    public List<String> getDevices() {
        return devices;
    }

    public void setDevices(List<String> devices) {
        this.devices = devices;
    }
}
