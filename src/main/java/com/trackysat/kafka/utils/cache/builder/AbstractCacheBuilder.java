package com.trackysat.kafka.utils.cache.builder;

import com.trackysat.kafka.utils.cache.AbstractCache;
import java.util.function.Consumer;

public abstract class AbstractCacheBuilder<KEY, T> {

    protected Consumer<T> onExpiration;
    protected long checkExpirationTimeInMillis;
    protected long recordTtl = 10000;
    protected long recordIdleTime = 10000;

    public AbstractCacheBuilder<KEY, T> recordTtl(long recordTtl) {
        this.recordTtl = recordTtl;
        return this;
    }

    public AbstractCacheBuilder<KEY, T> recordIdleTime(long recordIdleTime) {
        this.recordIdleTime = recordIdleTime;
        return this;
    }

    public AbstractCacheBuilder<KEY, T> onRecordExpiration(Consumer<T> expFun) {
        this.onExpiration = expFun;
        return this;
    }

    public AbstractCacheBuilder<KEY, T> checkExpirationTimeInMillis(long ms) {
        this.checkExpirationTimeInMillis = ms;
        return this;
    }

    public abstract AbstractCache<KEY, T> build();
}
