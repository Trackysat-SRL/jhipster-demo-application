package com.trackysat.kafka.utils.cache.impl;

import com.trackysat.kafka.utils.cache.AbstractCacheRecord;

public class InMemoryCacheRecord<T> extends AbstractCacheRecord<T> {

    public InMemoryCacheRecord(long recordIdleTime, long recordTtl, T obj) {
        super(recordIdleTime, recordTtl, obj);
    }
}
