package com.trackysat.kafka.service.mapper;

import com.trackysat.kafka.domain.TellTaleInfo;
import com.trackysat.kafka.domain.Vmson;
import com.trackysat.kafka.service.dto.TellTaleInfoDTO;
import com.trackysat.kafka.utils.JSONUtils;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.springframework.stereotype.Service;

@Service
public class TellTaleInfoMapper {

    public List<TellTaleInfo> fromVmson(Vmson record) {
        var conList = record.getVmson().getCon();
        var deviceId = record.getVmson().getOri().getUid();
        if (conList.isEmpty()) return new ArrayList<>();

        return conList
            .parallelStream()
            .flatMap(con ->
                con
                    .getSen()
                    .parallelStream()
                    .filter(sen ->
                        sen.getSrc().equals(LastTellTaleInfoMapper.SRC_CAN) && sen.getTyp().equals(LastTellTaleInfoMapper.TYPE_TELLTALE)
                    )
                    .map(sen ->
                        propagate(() -> {
                            var tellTaleInfo = new TellTaleInfo();
                            tellTaleInfo.setIid(sen.getIid());
                            tellTaleInfo.setState(sen.getVal());
                            tellTaleInfo.setCreatedDate(Instant.now());
                            tellTaleInfo.setEventCreatedDate(con.getEts().getTst());
                            tellTaleInfo.setEts(JSONUtils.toString(con.getEts()));
                            tellTaleInfo.setSen(JSONUtils.toString(sen));
                            tellTaleInfo.setDeviceId(deviceId);
                            return tellTaleInfo;
                        })
                    )
            )
            .collect(Collectors.toList());
    }

    public TellTaleInfoDTO toDto(TellTaleInfo entity) {
        return new TellTaleInfoDTO(
            entity.getDeviceId(),
            entity.getIid(),
            entity.getEts(),
            entity.getSen(),
            entity.getCreatedDate(),
            entity.getEventCreatedDate()
        );
    }

    private <R> R propagate(SupplierWithException<R> f) {
        try {
            return f.get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @FunctionalInterface
    interface SupplierWithException<R> {
        R get() throws Exception;
    }
}
