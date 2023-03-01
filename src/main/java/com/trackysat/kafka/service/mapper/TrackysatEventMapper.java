package com.trackysat.kafka.service.mapper;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.trackysat.kafka.domain.TrackysatEvent;
import com.trackysat.kafka.domain.Vmson;
import com.trackysat.kafka.domain.vmson.Ets;
import com.trackysat.kafka.domain.vmson.VmsonBody;
import com.trackysat.kafka.domain.vmson.VmsonCon;
import com.trackysat.kafka.utils.JSONUtils;
import java.time.Instant;
import org.springframework.stereotype.Service;

@Service
public class TrackysatEventMapper {

    public TrackysatEvent fromVmson(Vmson event) throws JsonProcessingException {
        TrackysatEvent e = new TrackysatEvent();
        VmsonBody body = event.getVmson();

        String id = JSONUtils.toString(body.getOri());
        e.setDeviceId(id);

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
}
