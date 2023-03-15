package com.trackysat.kafka.service.mapper;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
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
public class TrackysatEventMapper {

    private final Logger log = LoggerFactory.getLogger(TrackysatEventMapper.class);

    public TrackyEvent fromVmson(Vmson event) throws JsonProcessingException {
        TrackyEvent e = new TrackyEvent();
        VmsonBody body = event.getVmson();

        e.setDeviceId(body.getOri().getUid());

        Instant trackingTime = body.getCon().stream().findFirst().map(VmsonCon::getEts).map(Ets::getTst).orElse(body.getEts().getTst());
        e.setCreatedDate(trackingTime);

        e.setEventDate(body.getEts().getTst());
        e.setUid(body.getUid());
        e.setVer(body.getVer());
        e.setDes(JSONUtils.toString(body.getDes()));
        e.setEts(JSONUtils.toString(body.getEts()));
        e.setOri(JSONUtils.toString(body.getOri()));
        e.setCon(JSONUtils.toString(body.getCon()));
        return e;
    }

    public TrackysatEventDTO toTrackysatEventDTO(TrackyEvent trackyEvent) {
        TrackysatEventDTO mapped = new TrackysatEventDTO();
        mapped.setCreatedDate(trackyEvent.getCreatedDate());
        mapped.setDeviceId(trackyEvent.getDeviceId());
        mapped.setEventDate(trackyEvent.getEventDate());
        mapped.setUid(trackyEvent.getUid());
        mapped.setVer(trackyEvent.getVer());
        try {
            mapped.setCon(JSONUtils.toJson(trackyEvent.getCon(), new TypeReference<List<VmsonCon>>() {}));
            mapped.setDes(JSONUtils.toJson(trackyEvent.getDes(), Des.class));
            mapped.setEts(JSONUtils.toJson(trackyEvent.getEts(), Ets.class));
            mapped.setOri(JSONUtils.toJson(trackyEvent.getOri(), Ori.class));
        } catch (Exception e) {
            log.error("cannot parse");
        }
        return mapped;
    }
}
