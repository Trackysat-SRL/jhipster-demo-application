package com.trackysat.kafka.service.mapper;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.trackysat.kafka.domain.DailyAggregation;
import com.trackysat.kafka.domain.aggregations.PositionDTO;
import com.trackysat.kafka.domain.vmson.VmsonCon;
import com.trackysat.kafka.service.dto.DailyAggregationDTO;
import com.trackysat.kafka.service.dto.TrackysatEventDTO;
import com.trackysat.kafka.utils.JSONUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class DailyAggregationMapper {

    private final Logger log = LoggerFactory.getLogger(DailyAggregationMapper.class);

    public PositionDTO conToPosition(VmsonCon e) {
        PositionDTO p = new PositionDTO();
        p.setTimestamp(e.getEts().getTst());
        p.setAltitude(e.getSat().getAlt());
        p.setDirection(e.getSat().getDir());
        p.setLatitude(e.getSat().getLat());
        p.setLongitude(e.getSat().getLon());
        p.setFix(e.getSat().getFix());
        p.setNumberOfSatellites(e.getSat().getSnr());
        p.setSignal(e.getSat().getSig());
        p.setTyp(e.getSat().getTyp());
        p.setSpeed(e.getSat().getSpe());
        return p;
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
        try {
            positions = JSONUtils.toJson(dailyAggregation.getPositions(), new TypeReference<List<PositionDTO>>() {});
        } catch (JsonProcessingException e) {
            log.error("Couldnt parse positions. {}", e.getMessage());
        }
        d.setPositions(positions);
        return d;
    }
}
