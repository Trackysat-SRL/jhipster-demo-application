package com.trackysat.kafka.utils.cache;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import javax.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GenericCacheManager {

    private final Logger logger = LoggerFactory.getLogger(GenericCacheManager.class);
    private final ConcurrentHashMap<String, AbstractCache<?, ?>> caches = new ConcurrentHashMap<>();
    private final long poolSize;
    private final ScheduledExecutorService executor;

    public GenericCacheManager(int poolSize) {
        this.poolSize = poolSize;
        this.executor = Executors.newScheduledThreadPool(poolSize);
    }

    protected ScheduledExecutorService getExecutor() {
        return executor;
    }

    public long getPoolSize() {
        return poolSize;
    }

    public Optional<AbstractCache<?, ?>> getCache(String cacheName) {
        if (!caches.containsKey(cacheName)) return Optional.empty();
        var cache = caches.get(cacheName);
        return Optional.of(cache);
    }

    @SuppressWarnings("unchecked")
    public <KEY, T> AbstractCache<KEY, T> getCacheOrCreateNew(String cacheName, @NotNull AbstractCache<KEY, T> newCache) {
        AbstractCache<KEY, T> c = (AbstractCache<KEY, T>) getCache(cacheName).orElse(newCache);
        c.setManager(this);
        c.setCacheName(cacheName);
        caches.put(cacheName, c);
        return c;
    }

    public void deleteCache(String cacheName, boolean stopScheduledTask) {
        var cacheOptional = getCache(cacheName);
        if (cacheOptional.isEmpty()) return;
        deleteCache(cacheOptional.get(), stopScheduledTask);
    }

    public void deleteCache(AbstractCache<?, ?> cache, boolean stopScheduledTask) {
        if (cache == null || cache.getCacheName() == null) return;
        this.caches.remove(cache.getCacheName());
        cache.flush();
        if (stopScheduledTask) cache.stopScheduledTask();
    }

    public void destroy() {
        logger.debug("Saving all cached records before shutdown");
        caches.values().forEach(cache -> deleteCache(cache, true));
        logger.debug("done");
    }
}
