package com.trackysat.kafka.utils.cache;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

public abstract class AbstractCache<KEY, T> {

    protected Consumer<T> onExpiration;
    protected long checkExpirationTimeInMillis;
    protected long recordTtl;
    protected long recordIdleTime;
    protected ScheduledFuture<?> onExpirationScheduledTask;
    protected GenericCacheManager manager;
    protected String cacheName;

    public AbstractCache(Consumer<T> onExpiration, long checkExpirationTimeInMillis, long recordIdleTime, long recordTtl) {
        this.onExpiration = onExpiration;
        this.checkExpirationTimeInMillis = checkExpirationTimeInMillis;
        this.recordTtl = recordTtl;
        this.recordIdleTime = recordIdleTime;
    }

    protected void setManager(GenericCacheManager manager) {
        this.manager = manager;
        this.onExpirationScheduledTask =
            manager
                .getExecutor()
                .scheduleWithFixedDelay(
                    this::checkExpiredRecords,
                    checkExpirationTimeInMillis,
                    checkExpirationTimeInMillis,
                    TimeUnit.MILLISECONDS
                );
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
    public abstract List<T> get(Function<T, Boolean> predicate);

    /**
     * Get only the list of records matching the key
     *
     * @param keys the list of keys
     * @return the list of objects found by keys
     */
    public abstract List<T> getListByKeys(List<KEY> keys);

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
    public abstract List<T> getListByKeys(List<KEY> keys, Function<List<KEY>, List<T>> supplier, Function<T, KEY> keySupplier);

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
    public abstract T putIf(KEY key, T obj, Function<T, Boolean> predicate);

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
     * This function execute some business logic to an expired record
     *
     * @param key    the key
     * @param record the expired record
     */
    protected abstract void checkRecordExpiration(Object key, AbstractCacheRecord<T> record);
}
