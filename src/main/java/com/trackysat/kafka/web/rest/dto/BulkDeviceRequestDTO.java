package com.trackysat.kafka.web.rest.dto;

import java.util.List;
import javax.validation.constraints.NotEmpty;

public class BulkDeviceRequestDTO {

    @NotEmpty(message = "The list of devices cannot be empty")
    List<String> devices;

    public List<String> getDevices() {
        return devices;
    }

    public void setDevices(List<String> devices) {
        this.devices = devices;
    }
}
