package com.trackysat.kafka.config.cache;

import com.trackysat.kafka.domain.LastGpsPosition;
import com.trackysat.kafka.domain.aggregations.SensorStatsDTO;
import com.trackysat.kafka.repository.LastGpsPositionRepository;
import com.trackysat.kafka.utils.cache.AbstractCache;
import com.trackysat.kafka.utils.cache.BackOffStrategy;
import com.trackysat.kafka.utils.cache.GenericCacheManager;
import com.trackysat.kafka.utils.cache.builder.impl.InMemoryCacheBuilder;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class InternalCacheConfiguration {

    private final Logger logger = LoggerFactory.getLogger(InternalCacheConfiguration.class);
    public static final String LAST_GPS_POSITION_CACHE_NAME = "last_gps_position_cache";
    public static final String MONTHLY_SENSORS_AGGREGATION_CACHE_NAME = "monthly_sensors_aggregation_cache";

    @Bean
    public AbstractCache<String, List<SensorStatsDTO>> monthlySensorsAggregationCache() {
        logger.debug("Creating cache {}", MONTHLY_SENSORS_AGGREGATION_CACHE_NAME);
        return GenericCacheManager.getCacheOrCreateNew(
            MONTHLY_SENSORS_AGGREGATION_CACHE_NAME,
            name ->
                new InMemoryCacheBuilder<String, List<SensorStatsDTO>>()
                    .cacheName(name)
                    .checkExpirationTimeInMillis(3000)
                    .recordTtl(3_600_000)
                    .recordIdleTime(3_600_000)
                    .build()
        );
    }

    @Bean
    public AbstractCache<String, LastGpsPosition> lastGpsPositionAbstractCache(LastGpsPositionRepository lastGpsPositionRepository) {
        logger.debug("Creating cache {}", LAST_GPS_POSITION_CACHE_NAME);
        return GenericCacheManager.getCacheOrCreateNew(
            LAST_GPS_POSITION_CACHE_NAME,
            name ->
                new InMemoryCacheBuilder<String, LastGpsPosition>()
                    .cacheName(name)
                    .checkExpirationTimeInMillis(1000)
                    .recordTtl(200_000)
                    .recordIdleTime(180_000)
                    .processOnlyUpdatedOrNewRecords(true)
                    .backOffStrategy(new BackOffStrategy(3, 10_000, 50_000, 3000))
                    .onRecordExpiration(lastGpsPositionRepository::save)
                    .build()
        );
    }
}
