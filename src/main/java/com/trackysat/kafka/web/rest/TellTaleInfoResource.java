package com.trackysat.kafka.web.rest;

import com.trackysat.kafka.service.TellTaleInfoService;
import com.trackysat.kafka.service.dto.LastTellTaleInfoDTO;
import com.trackysat.kafka.service.dto.TellTaleInfoDTO;
import com.trackysat.kafka.web.rest.dto.BulkDeviceRequestDTO;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import tech.jhipster.web.util.ResponseUtil;

@RestController
@RequestMapping("/api")
public class TellTaleInfoResource {

    private final TellTaleInfoService tellTaleInfoService;

    public TellTaleInfoResource(TellTaleInfoService tellTaleInfoService) {
        this.tellTaleInfoService = tellTaleInfoService;
    }

    @GetMapping("/tellTaleInfo/{deviceId}")
    public ResponseEntity<List<TellTaleInfoDTO>> getTellTaleInfoByRangeDate(
        @RequestParam("from") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) String from,
        @RequestParam("to") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) String to,
        @RequestParam(value = "filterOffValues", defaultValue = "true") boolean filterOffValues,
        @PathVariable String deviceId
    ) {
        return ResponseEntity.ok(
            tellTaleInfoService.getTellTaleInfoListByDeviceIdAndRangeDate(deviceId, Instant.parse(from), Instant.parse(to), filterOffValues)
        );
    }

    @GetMapping("/lastTellTaleInfo/{deviceId}")
    public ResponseEntity<List<LastTellTaleInfoDTO>> getLastTellTaleInfoByDeviceId(
        @PathVariable String deviceId,
        @RequestParam(value = "filterOffValues", defaultValue = "true") boolean filterOffValues
    ) {
        return ResponseEntity.ok(tellTaleInfoService.getLastTellTaleInfoByDeviceId(deviceId, filterOffValues));
    }

    @GetMapping("/lastTellTaleInfo")
    public ResponseEntity<LastTellTaleInfoDTO> getLastTellTaleInfoByDeviceIdAndIid(
        @RequestParam @NotEmpty String deviceId,
        @RequestParam @NotEmpty String iid
    ) {
        return ResponseUtil.wrapOrNotFound(tellTaleInfoService.getLastTellTaleByDeviceIdAndIid(deviceId, iid));
    }

    @PostMapping("/lastTellTaleInfo")
    public ResponseEntity<Map<String, List<LastTellTaleInfoDTO>>> getLastTellTaleInfoByDevices(
        @RequestParam(value = "filterOffValues", defaultValue = "true") boolean filterOffValues,
        @RequestBody @Valid BulkDeviceRequestDTO request
    ) {
        return ResponseEntity.ok(tellTaleInfoService.getLastTellTaleInfoListByDevices(request.getDevices(), filterOffValues));
    }
}
