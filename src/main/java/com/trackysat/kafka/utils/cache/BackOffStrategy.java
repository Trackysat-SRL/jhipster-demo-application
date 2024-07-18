package com.trackysat.kafka.utils.cache;

public class BackOffStrategy {

    private long attempts = 0;
    private long maxAttempts;
    private long initialDelayInMillis;
    private long maxDelayInMillis;
    private long delay;

    public BackOffStrategy(long maxAttempts, long initialDelayInMillis, long maxDelayInMillis, long delay) {
        this.maxAttempts = maxAttempts;
        this.initialDelayInMillis = initialDelayInMillis;
        this.maxDelayInMillis = maxDelayInMillis;
        this.delay = delay;
    }

    public long getAttempts() {
        return attempts;
    }

    public void setAttempts(long attempts) {
        this.attempts = attempts;
    }

    public long getMaxAttempts() {
        return maxAttempts;
    }

    public void setMaxAttempts(long maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    public long getInitialDelayInMillis() {
        return initialDelayInMillis;
    }

    public void setInitialDelayInMillis(long initialDelayInMillis) {
        this.initialDelayInMillis = initialDelayInMillis;
    }

    public long getMaxDelayInMillis() {
        return maxDelayInMillis;
    }

    public void setMaxDelayInMillis(long maxDelayInMillis) {
        this.maxDelayInMillis = maxDelayInMillis;
    }

    public long getDelay() {
        return delay;
    }

    public void setDelay(long delay) {
        this.delay = delay;
    }

    public BackOffStrategy copy() {
        return new BackOffStrategy(this.maxAttempts, this.initialDelayInMillis, this.maxDelayInMillis, this.delay);
    }
}
