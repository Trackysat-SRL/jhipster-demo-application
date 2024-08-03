package com.trackysat.kafka.service.mapper;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.trackysat.kafka.domain.LastGpsPosition;
import com.trackysat.kafka.domain.LastTellTaleInfo;
import com.trackysat.kafka.domain.vmson.VmsonCon;
import com.trackysat.kafka.service.dto.LastTellTaleInfoDTO;
import com.trackysat.kafka.utils.JSONUtils;
import java.util.List;
import java.util.stream.Collectors;
import org.springframework.stereotype.Service;

@Service
public class LastTellTaleInfoMapper {

    public static String SRC_CAN = "CAN";
    public static String TYPE_TELLTALE = "TELLTALE";

    public List<LastTellTaleInfo> getLastTellTaleInfoListFromLastPosition(LastGpsPosition lastGpsPosition) throws JsonProcessingException {
        VmsonCon con = JSONUtils.toJson(lastGpsPosition.getGpsPosition(), VmsonCon.class);
        return con
            .getSen()
            .stream()
            .filter(sen -> sen.getSrc().equals(SRC_CAN) && sen.getTyp().equals(TYPE_TELLTALE))
            .map(sen -> {
                var lastTellTaleInfo = new LastTellTaleInfo();
                lastTellTaleInfo.setDeviceId(lastGpsPosition.getDeviceId());
                lastTellTaleInfo.setIid(sen.getIid());
                lastTellTaleInfo.setState(sen.getVal());
                lastTellTaleInfo.setEventCreatedDate(con.getEts().getTst());
                lastTellTaleInfo.setLat(lastGpsPosition.getLat());
                lastTellTaleInfo.setLon(lastGpsPosition.getLon());
                return lastTellTaleInfo;
            })
            .collect(Collectors.toList());
    }

    public LastTellTaleInfoDTO toDto(LastTellTaleInfo entity) {
        return new LastTellTaleInfoDTO(
            entity.getDeviceId(),
            entity.getIid(),
            entity.getState(),
            entity.getEventCreatedDate(),
            entity.getLon(),
            entity.getLat()
        );
    }

    public List<LastTellTaleInfoDTO> toDto(List<LastTellTaleInfo> entities) {
        return entities.stream().map(this::toDto).collect(Collectors.toList());
    }
}
