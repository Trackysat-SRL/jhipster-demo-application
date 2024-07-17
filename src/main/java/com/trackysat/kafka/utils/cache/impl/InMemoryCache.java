package com.trackysat.kafka.utils.cache.impl;

import com.trackysat.kafka.utils.cache.AbstractCache;
import com.trackysat.kafka.utils.cache.AbstractCacheRecord;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InMemoryCache<KEY, T> extends AbstractCache<KEY, T> {

    private final Logger logger = LoggerFactory.getLogger(InMemoryCache.class);
    private final ConcurrentHashMap<Object, InMemoryCacheRecord<T>> internalCache = new ConcurrentHashMap<>();
    private final ReentrantLock lock = new ReentrantLock();

    public InMemoryCache(Consumer<T> onExpiration, long checkExpirationTimeInMillis, long recordIdleTime, long recordTtl) {
        super(onExpiration, checkExpirationTimeInMillis, recordIdleTime, recordTtl);
    }

    private void withLock(Runnable f) {
        try {
            while (!lock.tryLock()) {
                Thread.yield();
            }
            f.run();
        } finally {
            lock.unlock();
        }
    }

    @Override
    protected void flush() {
        logger.debug("Deleting cache [{}] - START", cacheName);
        if (internalCache.isEmpty() || onExpiration == null) return;
        withLock(() -> this.internalCache.values().stream().map(InMemoryCacheRecord::getValue).forEach(onExpiration));
        internalCache.clear();
        logger.debug("Deleting cache [{}] - END", cacheName);
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
        withLock(() -> this.internalCache.forEach(this::checkRecordExpiration));
    }

    @Override
    protected void checkRecordExpiration(Object key, AbstractCacheRecord<T> record) {
        if (record.isExpired()) {
            logger.debug("[{}] - record with key [{}] is expired - removing from cache", cacheName, key);
            this.onExpiration.accept(record.getValue());
            this.internalCache.remove(key);
            return;
        }
        //this method should be executed every 'checkExpirationTimeInMillis' ms
        record.updateIdleCounter(checkExpirationTimeInMillis);
    }

    @Override
    public T putIf(KEY key, T obj, Function<T, Boolean> predicate) {
        var record = internalCache.getOrDefault(key, null);
        if (record == null || predicate.apply(record.getValue())) {
            put(key, obj);
            return obj;
        }

        return record.getValue();
    }

    @Override
    public List<T> get(Function<T, Boolean> predicate) {
        return internalCache.values().stream().map(InMemoryCacheRecord::getValue).filter(predicate::apply).collect(Collectors.toList());
    }

    @Override
    public List<T> getListByKeys(List<KEY> keys) {
        return keys.stream().map(this::get).filter(Optional::isPresent).map(Optional::get).collect(Collectors.toList());
    }

    @Override
    public List<T> getListByKeys(List<KEY> keys, Function<List<KEY>, List<T>> supplier, Function<T, KEY> keySupplier) {
        if (supplier == null) return getListByKeys(keys);
        var notFoundList = new ArrayList<KEY>();
        var objectList = keys
            .stream()
            .map(k -> {
                var record = get(k);
                if (record.isEmpty()) notFoundList.add(k);
                return record;
            })
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toList());

        var newObjectsToCache = supplier.apply(notFoundList);

        newObjectsToCache.stream().collect(Collectors.toMap(keySupplier, Function.identity())).forEach(this::put);

        objectList.addAll(newObjectsToCache);
        return objectList;
    }
}
