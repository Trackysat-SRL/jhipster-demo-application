package com.trackysat.kafka.utils.cache;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractCache<KEY, T> {

    protected final Consumer<T> onExpiration;
    protected final long checkExpirationTimeInMillis;
    protected final long recordTtl;
    protected final long recordIdleTime;
    protected final ScheduledFuture<?> onExpirationScheduledTask;
    protected String cacheName;
    protected final Executor backOffExecutor;
    private final Logger logger = LoggerFactory.getLogger(AbstractCache.class);

    public AbstractCache(
        Consumer<T> onExpiration,
        long checkExpirationTimeInMillis,
        long recordIdleTime,
        long recordTtl,
        BackOffStrategy backOffStrategy
    ) {
        this.onExpiration = onExpiration;
        this.checkExpirationTimeInMillis = checkExpirationTimeInMillis;
        this.recordTtl = recordTtl;
        this.recordIdleTime = recordIdleTime;
        this.onExpirationScheduledTask =
            GenericCacheManager
                .getExecutor()
                .scheduleWithFixedDelay(
                    this::checkExpiredRecords,
                    checkExpirationTimeInMillis,
                    checkExpirationTimeInMillis,
                    TimeUnit.MILLISECONDS
                );

        this.backOffExecutor =
            command ->
                new Thread(() -> {
                    BackOffStrategy s = backOffStrategy.copy();
                    while (s.getAttempts() < s.getMaxAttempts()) {
                        logger.debug("Attempt number [{}]", s.getAttempts() + 1);
                        try {
                            Thread.sleep(s.getInitialDelayInMillis());
                            command.run();
                            return;
                        } catch (Exception e) {
                            s.setAttempts(s.getAttempts() + 1);
                            s.setInitialDelayInMillis(Math.min(s.getInitialDelayInMillis() + s.getDelay(), s.getMaxDelayInMillis()));
                        }
                    }
                    logger.debug("Exhausted all attempts");
                })
                    .start();
    }

    protected void setCacheName(String cacheName) {
        this.cacheName = cacheName;
    }

    public String getCacheName() {
        return cacheName;
    }

    protected abstract void flush();

    protected void stopScheduledTask() {
        if (onExpirationScheduledTask == null) return;
        onExpirationScheduledTask.cancel(true);
    }

    /**
     * Get object or else use {@code newObjectProvider} as provider for the new object
     * which will be stored in cache if is not null.
     *
     * @param key               the key
     * @param newObjectProvider the object provider if record is not found.
     * @return the object stored with the provided key.
     */
    public T getOrElse(KEY key, Function<KEY, T> newObjectProvider) {
        var optional = get(key);
        if (optional.isPresent()) return optional.get();
        var newObj = newObjectProvider.apply(key);
        if (newObj == null) return null;
        return put(key, newObj);
    }

    /**
     * Get an {@link Optional} for the object stored in cache by the specified key.
     *
     * @param key the key.
     * @return an {@link Optional} for the object.
     */
    public abstract Optional<T> get(KEY key);

    /**
     * Get a list of object stored in cache matching the {@code predicate}.
     *
     * @param predicate the predicate.
     * @return a list of object.
     */
    public List<T> get(Function<T, Boolean> predicate) {
        return getValues().stream().filter(predicate::apply).collect(Collectors.toList());
    }

    /**
     * Get only the list of records matching the key
     *
     * @param keys the list of keys
     * @return the list of objects found by keys
     */
    public List<T> getListByKeys(List<KEY> keys) {
        return keys.stream().map(this::get).filter(Optional::isPresent).map(Optional::get).collect(Collectors.toList());
    }

    /**
     * Similar to {@link #getListByKeys(List)} but a {@code supplier} function can be specified in order
     * to get the objects which keys are not stored in the cache. The list that is returned from
     * the {@code supplier} is then stored inside the cache for future interrogations, while each key is created
     * via {@code keySupplier}.
     *
     * @param keys        the list of keys
     * @param supplier    the supplier function
     * @param keySupplier the key generator for each new object
     * @return the list of objects found by keys
     */
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

    /**
     * Put the new object to cache only if there is no object stored
     * with the specified key, or the predicate for the previous
     * object is true. Inside the function predicate, the old object is guaranteed to be not null.
     *
     * @param key       unique key for the object
     * @param obj       the new object to store
     * @param predicate the predicate that takes the previously stored object if any
     * @return the object stored for the specified key
     */
    public T putIf(KEY key, T obj, BiFunction<T, T, Boolean> predicate) {
        var record = get(key).orElse(null);
        if (record == null || predicate.apply(record, obj)) {
            put(key, obj);
            return obj;
        }

        return record;
    }

    /**
     * Same as {@link #put(Object, Object)} but this functions uses {@link #getOrElse(Object, Function)} to
     * retrieve the old record.
     *
     * @param key       the key of the record
     * @param obj       the new object
     * @param supplier  the supplier used for {@link #getOrElse(Object, Function)}
     * @param predicate the predicate
     * @return the object stored inside the cache for the specified key
     */

    public T putIf(KEY key, T obj, Function<KEY, T> supplier, BiFunction<T, T, Boolean> predicate) {
        var record = getOrElse(key, supplier);
        if (record == null || predicate.apply(record, obj)) {
            put(key, obj);
            return obj;
        }

        return record;
    }

    /**
     * Get all the records stored inside the cache
     *
     * @return the list of records
     */
    public abstract List<T> getValues();

    /**
     * Store a new object inside the cache using {@code key}.
     *
     * @param key the key
     * @param obj the object
     * @return the object stored.
     */
    public abstract T put(KEY key, T obj);

    /**
     * Routine that checks if there are some expired records inside the cache, and execute
     * {@link #checkRecordExpiration(Object, AbstractCacheRecord)} to each record found in this way.
     */
    protected abstract void checkExpiredRecords();

    /**
     * Remove from cache the object stored with the specified key
     *
     * @param k the key
     */
    protected abstract void remove(KEY k);

    /**
     * This function execute some business logic to an expired record
     *
     * @param key    the key
     * @param record the expired record
     */

    protected void checkRecordExpiration(KEY key, AbstractCacheRecord<T> record) {
        if (record.isExpired()) {
            try {
                remove(key);
                this.onExpiration.accept(record.getValue());
            } catch (Exception e) {
                logger.error("Exception thrown while checking expiration for record id {}: {}", key, e.getMessage());
                if (backOffExecutor != null) {
                    backOffExecutor.execute(() -> {
                        logger.debug("Retrying executing expiration command for record {}", key);
                        this.onExpiration.accept(record.getValue());
                    });
                }
            }
            return;
        }
        //this method should be executed every 'checkExpirationTimeInMillis' ms
        record.updateIdleCounter(checkExpirationTimeInMillis);
    }
}
