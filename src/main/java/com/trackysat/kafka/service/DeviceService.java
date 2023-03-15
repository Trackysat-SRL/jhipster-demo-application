package com.trackysat.kafka.service;

import com.trackysat.kafka.domain.Device;
import com.trackysat.kafka.repository.DeviceRepository;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Service class for managing devices.
 */
@Service
public class DeviceService {

    private final Logger log = LoggerFactory.getLogger(DeviceService.class);

    private final DeviceRepository deviceRepository;

    public DeviceService(DeviceRepository deviceRepository) {
        this.deviceRepository = deviceRepository;
    }

    public Optional<Device> getOne(String id) {
        return deviceRepository.findById(id);
    }

    public List<Device> getAll() {
        return deviceRepository.findAll();
    }
}
