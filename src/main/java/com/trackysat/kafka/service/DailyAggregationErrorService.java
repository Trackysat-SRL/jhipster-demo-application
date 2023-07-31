package com.trackysat.kafka.service;

import com.trackysat.kafka.domain.DailyAggregationError;
import com.trackysat.kafka.repository.DailyAggregationErrorRepository;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class DailyAggregationErrorService {

    private final Logger log = LoggerFactory.getLogger(DailyAggregationErrorService.class);

    private final DailyAggregationErrorRepository dailyAggregationErrorRepository;

    public DailyAggregationErrorService(DailyAggregationErrorRepository dailyAggregationErrorRepository) {
        this.dailyAggregationErrorRepository = dailyAggregationErrorRepository;
    }

    public Optional<DailyAggregationError> getOne() {
        return dailyAggregationErrorRepository.findOne();
    }

    public List<DailyAggregationError> getAll() {
        return dailyAggregationErrorRepository.findAll();
    }

    public DailyAggregationError save(DailyAggregationError error) {
        return this.dailyAggregationErrorRepository.save(error);
    }

    public void delete(DailyAggregationError error) {
        this.dailyAggregationErrorRepository.delete(error);
    }
}
