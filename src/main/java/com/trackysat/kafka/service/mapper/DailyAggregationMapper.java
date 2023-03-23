package com.trackysat.kafka.service.mapper;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.trackysat.kafka.domain.DailyAggregation;
import com.trackysat.kafka.domain.aggregations.PositionDTO;
import com.trackysat.kafka.domain.aggregations.SensorStatsDTO;
import com.trackysat.kafka.domain.aggregations.SensorValDTO;
import com.trackysat.kafka.domain.vmson.VmsonCon;
import com.trackysat.kafka.service.dto.DailyAggregationDTO;
import com.trackysat.kafka.service.dto.TrackysatEventDTO;
import com.trackysat.kafka.utils.JSONUtils;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class DailyAggregationMapper {

    private final Logger log = LoggerFactory.getLogger(DailyAggregationMapper.class);

    public PositionDTO conToPosition(VmsonCon e) {
        PositionDTO p = new PositionDTO();
        p.setTimestamp(e.getEts().getTst());
        p.setAltitude(Optional.ofNullable(e.getSat().getAlt()).orElse(-1));
        p.setSpeed(Optional.ofNullable(e.getSat().getSpe()).orElse(-1));
        p.setLatitude(Optional.ofNullable(e.getSat().getLat()).orElse(-1.0));
        p.setLongitude(Optional.ofNullable(e.getSat().getLon()).orElse(-1.0));
        p.setDirection(e.getSat().getDir());
        p.setFix(e.getSat().getFix());
        p.setNumberOfSatellites(e.getSat().getSnr());
        p.setSignal(e.getSat().getSig());
        p.setTyp(e.getSat().getTyp());
        return p;
    }

    public Map<String, SensorStatsDTO> conToSensorMap(VmsonCon e) {
        return e
            .getSen()
            .stream()
            .collect(
                Collectors.toMap(
                    sen -> String.format("%s_%s_%s_%s_%s", sen.getSrc(), sen.getIid(), sen.getMis(), sen.getTyp(), sen.getSid()),
                    sen -> {
                        SensorStatsDTO id = new SensorStatsDTO();
                        id.setName(sen.getIid());
                        id.setSource(sen.getSrc());
                        id.setMeasureUnit(sen.getMis());
                        id.setType(sen.getTyp());
                        id.setSid(sen.getSid());

                        SensorValDTO val = new SensorValDTO();
                        val.setCreationDate(e.getEts().getTst());
                        val.setValue(sen.getVal());
                        id.setValues(Collections.singletonList(val));
                        return id;
                    }
                )
            );
    }

    public DailyAggregationDTO fromTrackysatEventDTO(TrackysatEventDTO trackysatEventDTO) {
        DailyAggregationDTO d = new DailyAggregationDTO();
        d.setDeviceId(trackysatEventDTO.getDeviceId());
        d.setAggregatedDate(trackysatEventDTO.getCreatedDate());
        d.setPositions(trackysatEventDTO.getCon().stream().map(this::conToPosition).collect(Collectors.toList()));
        return d;
    }

    public DailyAggregationDTO toDTO(DailyAggregation dailyAggregation) {
        DailyAggregationDTO d = new DailyAggregationDTO();
        d.setDeviceId(dailyAggregation.getDeviceId());
        d.setAggregatedDate(dailyAggregation.getAggregatedDate());
        List<PositionDTO> positions = new ArrayList<>();
        Map<String, SensorStatsDTO> sensorMap = new HashMap<>();
        try {
            positions =
                !Strings.isEmpty(dailyAggregation.getPositions())
                    ? JSONUtils.toJson(dailyAggregation.getPositions(), new TypeReference<List<PositionDTO>>() {})
                    : positions;
            sensorMap =
                !Strings.isEmpty(dailyAggregation.getSensors())
                    ? JSONUtils.toJson(dailyAggregation.getSensors(), new TypeReference<Map<String, SensorStatsDTO>>() {})
                    : sensorMap;
        } catch (JsonProcessingException e) {
            log.error("Couldnt parse from database. {}", e.getMessage());
        }
        d.setPositions(positions);
        d.setSensors(sensorMap);
        return d;
    }
}
