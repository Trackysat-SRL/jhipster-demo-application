package com.trackysat.kafka.config.cache;

import com.trackysat.kafka.utils.cache.GenericCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class InternalCacheConfiguration {

    @Bean(destroyMethod = "destroy")
    public GenericCacheManager genericCacheManager() {
        return new GenericCacheManager(15);
    }
}
