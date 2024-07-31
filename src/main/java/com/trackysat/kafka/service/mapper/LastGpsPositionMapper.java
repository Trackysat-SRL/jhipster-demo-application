package com.trackysat.kafka.service.mapper;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.trackysat.kafka.domain.LastGpsPosition;
import com.trackysat.kafka.domain.Vmson;
import com.trackysat.kafka.service.dto.LastGpsPositionDTO;
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
        var optionalVmsonCon = conList.stream().findFirst();
        if (optionalVmsonCon.isEmpty()) return null;
        var vmsonCon = optionalVmsonCon.get();
        var position = new LastGpsPosition();
        position.setGpsPosition(JSONUtils.toString(vmsonCon));
        position.setEventPositionDate(vmsonCon.getEts().getTst());
        position.setDeviceId(event.getVmson().getOri().getUid());
        return position;
    }

    public LastGpsPositionDTO toDto(LastGpsPosition entity) {
        return new LastGpsPositionDTO(entity.getDeviceId(), entity.getEventPositionDate(), entity.getGpsPosition());
    }
}
