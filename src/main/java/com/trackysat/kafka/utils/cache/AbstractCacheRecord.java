package com.trackysat.kafka.utils.cache;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public abstract class AbstractCacheRecord<T> {

    protected long idt;
    protected long ttl;
    protected AtomicLong idleTimeCounter;
    protected AtomicBoolean needToBeProcessed;
    protected long insertedAt;
    private T value;

    public AbstractCacheRecord(long idt, long ttl, T value, boolean needToBeProcessed) {
        this.idt = idt;
        this.ttl = ttl;
        this.idleTimeCounter = new AtomicLong(0);
        this.insertedAt = System.currentTimeMillis();
        this.value = value;
        this.needToBeProcessed = new AtomicBoolean(needToBeProcessed);
    }

    private void resetIdleCounter() {
        this.idleTimeCounter.set(0);
    }

    public boolean needToBeProcessed() {
        return needToBeProcessed.get();
    }

    public void needToBeProcessed(boolean flag) {
        this.needToBeProcessed.set(flag);
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
        needToBeProcessed(true);
        this.value = value;
    }
}
