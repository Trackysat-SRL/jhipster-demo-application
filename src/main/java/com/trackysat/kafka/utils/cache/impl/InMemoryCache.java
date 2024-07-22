package com.trackysat.kafka.utils.cache.impl;

import com.trackysat.kafka.utils.cache.AbstractCache;
import com.trackysat.kafka.utils.cache.BackOffStrategy;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InMemoryCache<KEY, T> extends AbstractCache<KEY, T> {

    private final Logger logger = LoggerFactory.getLogger(InMemoryCache.class);
    private final ConcurrentHashMap<KEY, InMemoryCacheRecord<T>> internalCache = new ConcurrentHashMap<>();

    public InMemoryCache(
        Consumer<T> onExpiration,
        long checkExpirationTimeInMillis,
        long recordIdleTime,
        long recordTtl,
        BackOffStrategy backOffStrategy
    ) {
        super(onExpiration, checkExpirationTimeInMillis, recordIdleTime, recordTtl, backOffStrategy);
    }

    @Override
    protected void flush() {
        if (internalCache.isEmpty() || onExpiration == null) return;
        var start = System.currentTimeMillis();
        logger.debug("Deleting cache [{}] - START ", cacheName);
        internalCache.values().parallelStream().forEach(record -> onExpiration.accept(record.getValue()));
        System.out.printf("Deleting cache [%s] - END (%dms)%n", cacheName, System.currentTimeMillis() - start);
        internalCache.clear();
        logger.debug("Cache [{}] deleted.", cacheName);
    }

    @Override
    public Optional<T> get(KEY key) {
        var record = internalCache.getOrDefault(key, null);
        if (record == null) return Optional.empty();
        return Optional.of(record.getValue());
    }

    @Override
    public List<T> getValues() {
        return this.internalCache.values().stream().map(InMemoryCacheRecord::getValue).collect(Collectors.toList());
    }

    @Override
    public T put(KEY key, T obj) {
        internalCache.compute(
            key,
            (k, v) -> {
                if (v == null) {
                    return new InMemoryCacheRecord<>(this.recordIdleTime, this.recordTtl, obj);
                }

                v.setValue(obj);
                return v;
            }
        );
        return obj;
    }

    @Override
    protected void checkExpiredRecords() {
        if (internalCache.isEmpty()) return;
        logger.debug("[{}] - checkExpiredRecords", cacheName);
        List<Runnable> operations = new ArrayList<>();
        this.internalCache.forEach((k, v) -> operations.add(() -> checkRecordExpiration(k, v)));
        /* to execute in parallel */
        operations.parallelStream().forEach(Runnable::run);
    }

    @Override
    protected void remove(KEY k) {
        this.internalCache.remove(k);
    }
}
