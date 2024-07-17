package com.trackysat.kafka.utils.cache;

import java.util.concurrent.atomic.AtomicLong;

public abstract class AbstractCacheRecord<T> {

    protected long idt;
    protected long ttl;
    protected AtomicLong idleTimeCounter;
    protected long insertedAt;
    private T value;

    public AbstractCacheRecord(long idt, long ttl, T value) {
        this.idt = idt;
        this.ttl = ttl;
        this.idleTimeCounter = new AtomicLong(0);
        this.insertedAt = System.currentTimeMillis();
        this.value = value;
    }

    private void resetIdleCounter() {
        this.idleTimeCounter.set(0);
    }

    public void updateIdleCounter(long delta) {
        this.idleTimeCounter.addAndGet(delta);
    }

    public boolean isExpired() {
        return this.idleTimeCounter.get() >= idt || System.currentTimeMillis() - insertedAt >= ttl;
    }

    public T getValue() {
        resetIdleCounter();
        return value;
    }

    public void setValue(T value) {
        resetIdleCounter();
        this.value = value;
    }
}
