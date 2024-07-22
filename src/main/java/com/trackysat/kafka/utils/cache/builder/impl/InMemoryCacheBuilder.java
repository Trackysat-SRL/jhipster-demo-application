package com.trackysat.kafka.utils.cache.builder.impl;

import com.trackysat.kafka.utils.cache.AbstractCache;
import com.trackysat.kafka.utils.cache.builder.AbstractCacheBuilder;
import com.trackysat.kafka.utils.cache.impl.InMemoryCache;

public class InMemoryCacheBuilder<KEY, T> extends AbstractCacheBuilder<KEY, T> {

    @Override
    public AbstractCache<KEY, T> build() {
        return new InMemoryCache<>(
            this.onExpiration,
            this.checkExpirationTimeInMillis,
            this.recordIdleTime,
            this.recordTtl,
            this.backOffStrategy,
            this.processOnlyUpdatedOrNewRecords
        );
    }
}
