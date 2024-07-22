package com.trackysat.kafka.web.rest;

import com.trackysat.kafka.service.LastGpsPositionService;
import com.trackysat.kafka.service.dto.LastGpsPositionDTO;
import com.trackysat.kafka.web.rest.dto.BulkDeviceRequestDTO;
import java.util.List;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api")
public class LastGpsPositionResource {

    private final LastGpsPositionService lastGpsPositionService;

    public LastGpsPositionResource(LastGpsPositionService lastGpsPositionService) {
        this.lastGpsPositionService = lastGpsPositionService;
    }

    @GetMapping("/lastGpsPosition/{deviceId}")
    public ResponseEntity<LastGpsPositionDTO> getPositionByDeviceId(@PathVariable String deviceId) {
        return ResponseEntity.ok(lastGpsPositionService.getLastGpsPositionByDeviceId(deviceId));
    }

    @PostMapping("/lastGpsPosition")
    public ResponseEntity<List<LastGpsPositionDTO>> getPositionsByDeviceIds(@RequestBody BulkDeviceRequestDTO request) {
        return ResponseEntity.ok(lastGpsPositionService.getLastGpsPositionsByDeviceIds(request.getDevices()));
    }
}
