package com.trackysat.kafka.utils.cache.builder;

import com.trackysat.kafka.utils.cache.AbstractCache;
import com.trackysat.kafka.utils.cache.BackOffStrategy;
import java.util.function.Consumer;

public abstract class AbstractCacheBuilder<KEY, T> {

    protected Consumer<T> onExpiration;
    protected long checkExpirationTimeInMillis = 1000;
    protected long recordTtl = 10000;
    protected long recordIdleTime = 10000;
    protected BackOffStrategy backOffStrategy;
    protected boolean processOnlyUpdatedOrNewRecords = false;
    protected String cacheName;

    public AbstractCacheBuilder<KEY, T> recordTtl(long recordTtl) {
        this.recordTtl = recordTtl;
        return this;
    }

    public AbstractCacheBuilder<KEY, T> cacheName(String cacheName) {
        this.cacheName = cacheName;
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

    public AbstractCacheBuilder<KEY, T> backOffStrategy(BackOffStrategy strategy) {
        this.backOffStrategy = strategy;
        return this;
    }

    public AbstractCacheBuilder<KEY, T> processOnlyUpdatedOrNewRecords(boolean flag) {
        this.processOnlyUpdatedOrNewRecords = flag;
        return this;
    }

    public abstract AbstractCache<KEY, T> build();
}
