package com.trackysat.kafka.service;

import com.trackysat.kafka.domain.LastGpsPosition;
import com.trackysat.kafka.repository.LastGpsPositionRepository;
import com.trackysat.kafka.service.dto.LastGpsPositionDTO;
import com.trackysat.kafka.service.mapper.LastGpsPositionMapper;
import com.trackysat.kafka.utils.cache.AbstractCache;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

@Service
public class LastGpsPositionService {

    private final Logger log = LoggerFactory.getLogger(LastGpsPositionService.class);
    private final AbstractCache<String, LastGpsPosition> lastGpsPositionAbstractCache;
    private final LastGpsPositionRepository lastGpsPositionRepository;
    private final LastGpsPositionMapper lastGpsPositionMapper;

    public LastGpsPositionService(
        AbstractCache<String, LastGpsPosition> lastGpsPositionAbstractCache,
        LastGpsPositionRepository lastGpsPositionRepository,
        LastGpsPositionMapper lastGpsPositionMapper
    ) {
        this.lastGpsPositionAbstractCache = lastGpsPositionAbstractCache;
        this.lastGpsPositionRepository = lastGpsPositionRepository;
        this.lastGpsPositionMapper = lastGpsPositionMapper;
    }

    public LastGpsPositionDTO getLastGpsPositionByDeviceId(String deviceId) {
        var position = lastGpsPositionAbstractCache.getOrElse(deviceId, key -> lastGpsPositionRepository.findById(key).orElse(null));
        if (position == null) throw new ResponseStatusException(HttpStatus.NOT_FOUND, "No position found for device " + deviceId);
        return lastGpsPositionMapper.toDto(position);
    }

    public List<LastGpsPositionDTO> getLastGpsPositionsByDeviceIds(List<String> deviceIds) {
        return lastGpsPositionAbstractCache
            .getListByKeys(deviceIds, lastGpsPositionRepository::findByIdIn, LastGpsPosition::getDeviceId)
            .stream()
            .map(lastGpsPositionMapper::toDto)
            .collect(Collectors.toList());
    }
}
