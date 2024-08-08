package com.trackysat.kafka.config.cache;

import com.trackysat.kafka.domain.MonthlyAggregation;
import java.time.Duration;
import org.springframework.boot.autoconfigure.cache.RedisCacheManagerBuilderCustomizer;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.cache.RedisCacheConfiguration;
import org.springframework.data.redis.cache.RedisCacheManager;
import org.springframework.data.redis.connection.RedisConnectionFactory;

@Configuration
@EnableCaching
public class CacheManagerConfiguration {

    public static final String MONTHLY_AGGREGATION_CACHE = "monthly_aggregation";

    @Bean
    public CacheManager cacheManager(RedisConnectionFactory connectionFactory) {
        return RedisCacheManager.RedisCacheManagerBuilder
            .fromConnectionFactory(connectionFactory)
            .withCacheConfiguration(
                MONTHLY_AGGREGATION_CACHE,
                RedisCacheConfiguration
                    .defaultCacheConfig(Thread.currentThread().getContextClassLoader())
                    .entryTtl(Duration.ofMinutes(30))
                    .disableCachingNullValues()
            )
            .build();
    }

    @Bean
    public RedisCacheManagerBuilderCustomizer myRedisCacheManagerBuilderCustomizer() {
        return builder ->
            builder.withCacheConfiguration(
                MONTHLY_AGGREGATION_CACHE,
                RedisCacheConfiguration.defaultCacheConfig().entryTtl(Duration.ofMinutes(30)).disableCachingNullValues()
            );
    }
}
