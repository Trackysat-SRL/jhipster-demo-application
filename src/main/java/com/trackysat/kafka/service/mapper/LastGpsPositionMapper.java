package com.trackysat.kafka.service.mapper;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.trackysat.kafka.domain.LastGpsPosition;
import com.trackysat.kafka.domain.Vmson;
import com.trackysat.kafka.utils.JSONUtils;
import java.util.Collections;
import java.util.Comparator;
import org.springframework.stereotype.Service;

@Service
public class LastGpsPositionMapper {

    public LastGpsPosition fromVmson(Vmson event) throws JsonProcessingException {
        var conList = event.getVmson().getCon();
        if (conList.isEmpty()) return null;
        conList.sort(Collections.reverseOrder(Comparator.comparing(a -> a.getEts().getTst())));
        var mostRecentPosition = conList.stream().findFirst().orElseThrow();
        var position = new LastGpsPosition();
        position.setGpsPosition(
            "{ \"ets\" :" +
            JSONUtils.toString(mostRecentPosition.getEts()) +
            ", \"sat\" :" +
            JSONUtils.toString(mostRecentPosition.getSat()) +
            " }"
        );
        position.setEventPositionDate(mostRecentPosition.getEts().getTst());
        position.setDeviceId(event.getVmson().getOri().getUid());
        return position;
    }
}
