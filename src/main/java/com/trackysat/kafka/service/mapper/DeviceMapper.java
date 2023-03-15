package com.trackysat.kafka.service.mapper;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.trackysat.kafka.domain.Device;
import com.trackysat.kafka.domain.TrackyEvent;
import com.trackysat.kafka.domain.Vmson;
import com.trackysat.kafka.domain.vmson.*;
import com.trackysat.kafka.service.dto.TrackysatEventDTO;
import com.trackysat.kafka.utils.JSONUtils;
import java.time.Instant;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class DeviceMapper {

    private final Logger log = LoggerFactory.getLogger(DeviceMapper.class);

    public Device fromVmson(Vmson event) throws JsonProcessingException {
        Device e = new Device();
        Ori o = event.getVmson().getOri();
        e.setCreatedDate(Instant.now());
        e.setUid(o.getUid());
        e.setCla(o.getCla());
        e.setVer(o.getVer());
        e.setTyp(o.getTyp());
        return e;
    }
}
