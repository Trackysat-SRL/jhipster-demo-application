package com.trackysat.kafka.service;

import com.trackysat.kafka.repository.LastTellTaleInfoRepository;
import com.trackysat.kafka.repository.TellTaleInfoRepository;
import com.trackysat.kafka.service.dto.LastTellTaleInfoDTO;
import com.trackysat.kafka.service.dto.TellTaleInfoDTO;
import com.trackysat.kafka.service.mapper.LastTellTaleInfoMapper;
import com.trackysat.kafka.service.mapper.TellTaleInfoMapper;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class TellTaleInfoService {

    private static final Logger log = LoggerFactory.getLogger(TellTaleInfoService.class);

    private final TellTaleInfoRepository tellTaleInfoRepository;
    private final TellTaleInfoMapper tellTaleInfoMapper;
    private final LastTellTaleInfoRepository lastTellTaleInfoRepository;
    private final LastTellTaleInfoMapper lastTellTaleInfoMapper;

    public TellTaleInfoService(
        TellTaleInfoRepository tellTaleInfoRepository,
        TellTaleInfoMapper tellTaleInfoMapper,
        LastTellTaleInfoRepository lastTellTaleInfoRepository,
        LastTellTaleInfoMapper lastTellTaleInfoMapper
    ) {
        this.tellTaleInfoRepository = tellTaleInfoRepository;
        this.tellTaleInfoMapper = tellTaleInfoMapper;
        this.lastTellTaleInfoRepository = lastTellTaleInfoRepository;
        this.lastTellTaleInfoMapper = lastTellTaleInfoMapper;
    }

    public List<TellTaleInfoDTO> getTellTaleInfoListByDeviceIdAndRangeDate(
        String deviceId,
        Instant from,
        Instant to,
        boolean filterOffValues
    ) {
        log.debug("getTellTaleInfoListByDeviceIdAndRangeDate - deviceId: {}, from: {}, to: {}", deviceId, from, to);
        return tellTaleInfoRepository
            .findByDeviceIdAndRangeDate(deviceId, from, to)
            .stream()
            .filter(info -> !filterOffValues || !info.getState().equals("OFF"))
            .map(this.tellTaleInfoMapper::toDto)
            .collect(Collectors.toList());
    }

    public List<LastTellTaleInfoDTO> getLastTellTaleInfoByDeviceId(String deviceId, boolean filterOffValues) {
        log.debug("getLastTellTaleInfoByDeviceId - deviceId: {}", deviceId);
        return lastTellTaleInfoMapper.toDto(
            lastTellTaleInfoRepository
                .findByDeviceId(deviceId)
                .stream()
                .filter(info -> !filterOffValues || !info.getState().equals("OFF"))
                .collect(Collectors.toList())
        );
    }

    public Optional<LastTellTaleInfoDTO> getLastTellTaleByDeviceIdAndIid(String deviceId, String iid) {
        return lastTellTaleInfoRepository.findById(deviceId, iid).map(lastTellTaleInfoMapper::toDto);
    }

    public Map<String, List<LastTellTaleInfoDTO>> getLastTellTaleInfoListByDevices(List<String> devices, boolean filterOffValues) {
        log.debug("getLastTellTaleInfoListByDevices - devices size {}", devices.size());
        Map<String, List<LastTellTaleInfoDTO>> map = lastTellTaleInfoRepository
            .findByDeviceIdIn(devices)
            .stream()
            .filter(info -> !filterOffValues || !info.getState().equals("OFF"))
            .map(lastTellTaleInfoMapper::toDto)
            .collect(
                Collectors.toMap(
                    LastTellTaleInfoDTO::getDeviceId,
                    v -> {
                        var list = new ArrayList<LastTellTaleInfoDTO>();
                        list.add(v);
                        return list;
                    },
                    (a, b) -> {
                        a.addAll(b);
                        return a;
                    }
                )
            );

        devices.forEach(key -> {
            if (!map.containsKey(key)) map.put(key, new ArrayList<>());
        });

        return map;
    }
}
