package com.trackysat.kafka.service;

import com.trackysat.kafka.domain.MonthlyAggregation;
import com.trackysat.kafka.repository.MonthlyAggregationRepository;
import com.trackysat.kafka.service.dto.DailyAggregationDTO;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Service class for managing monthly aggregations.
 */
@Service
public class MonthlyAggregationService {

    private final Logger log = LoggerFactory.getLogger(MonthlyAggregationService.class);

    private final MonthlyAggregationRepository monthlyAggregationRepository;

    public MonthlyAggregationService(MonthlyAggregationRepository monthlyAggregationRepository) {
        this.monthlyAggregationRepository = monthlyAggregationRepository;
    }

    public Optional<MonthlyAggregation> getOne() {
        return monthlyAggregationRepository.findOne();
    }

    public List<MonthlyAggregation> getAll() {
        return monthlyAggregationRepository.findAll();
    }

    public List<DailyAggregationDTO> getByDeviceIdAndDateRange(String deviceId, Instant dateFrom, Instant dateTo) {
        return null;
    }
}
