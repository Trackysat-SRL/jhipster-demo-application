package com.trackysat.kafka.service;

import com.trackysat.kafka.domain.DailyAggregation;
import com.trackysat.kafka.repository.DailyAggregationRepository;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Service class for managing daily aggregations.
 */
@Service
public class DailyAggregationService {

    private final Logger log = LoggerFactory.getLogger(DailyAggregationService.class);

    private final DailyAggregationRepository dailyAggregationRepository;

    public DailyAggregationService(DailyAggregationRepository dailyAggregationRepository) {
        this.dailyAggregationRepository = dailyAggregationRepository;
    }

    public Optional<DailyAggregation> getOne(String id) {
        return dailyAggregationRepository.findById(id);
    }

    public List<DailyAggregation> getAll() {
        return dailyAggregationRepository.findAll();
    }
}
