package com.trackysat.kafka.utils.cache;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("rawtypes, unchecked")
public final class GenericCacheManager {

    private static final Logger logger = LoggerFactory.getLogger(GenericCacheManager.class);
    private static final ConcurrentHashMap<String, AbstractCache> caches = new ConcurrentHashMap<>();
    private static final ScheduledExecutorService executor = Executors.newScheduledThreadPool(15);

    private GenericCacheManager() {}

    static ScheduledExecutorService getExecutor() {
        return executor;
    }

    /**
     * Get cache by {@code cacheName} or {@code null} if not present.
     *
     * @param cacheName the name of the cache
     * @param <KEY>     Type of the cache key
     * @param <T>       Type of cached objects
     * @return the cache or {@code null} if not found
     */
    public static <KEY, T> AbstractCache<KEY, T> getCache(String cacheName) {
        if (!caches.containsKey(cacheName)) return null;
        return (AbstractCache<KEY, T>) caches.get(cacheName);
    }

    /**
     * Get or create a new cache with the specified key. A supplier has been provided instead
     * of a new created object, in order to avoid to create eagerly a potentially unused object.
     *
     * @param cacheName       the cacheName
     * @param lazyFunSupplier supplier function that creates a new cache if not found
     * @param <KEY>           Type for cache key
     * @param <T>             Type for cached objects
     * @return the cache
     */
    public static <KEY, T> AbstractCache<KEY, T> getCacheOrCreateNew(String cacheName, Supplier<AbstractCache<KEY, T>> lazyFunSupplier) {
        AbstractCache<KEY, T> c = getCache(cacheName);
        if (c == null) c = lazyFunSupplier.get();
        c.setCacheName(cacheName);
        caches.put(cacheName, c);
        return c;
    }

    /**
     * Remove the cache from the internal map of running caches and flushes it.
     * @param cache the cache to remove
     * @param stopScheduledTask a flag for interrupting the cache scheduled task after delete
     */
    public static void deleteCache(AbstractCache cache, boolean stopScheduledTask) {
        if (cache == null || cache.getCacheName() == null) return;
        caches.remove(cache.getCacheName());
        cache.flush();
        if (stopScheduledTask) cache.stopScheduledTask();
    }

    public static void destroy() {
        logger.debug("Saving all cached records before shutdown");
        caches.values().forEach(cache -> deleteCache(cache, true));
        logger.debug("done");
    }
}
