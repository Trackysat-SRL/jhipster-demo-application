package com.trackysat.kafka.service.dto;

import java.time.Instant;

public class StatusDTO {

    long runningTime;
    boolean isEnabled;
    int eventCounter;
    int errorCounter;

    public long getRunningTime() {
        return runningTime;
    }

    public void setRunningTime(long runningTime) {
        this.runningTime = runningTime;
    }

    public boolean isEnabled() {
        return isEnabled;
    }

    public void setEnabled(boolean enabled) {
        isEnabled = enabled;
    }

    public int getEventCounter() {
        return eventCounter;
    }

    public void setEventCounter(int eventCounter) {
        this.eventCounter = eventCounter;
    }

    public int getErrorCounter() {
        return errorCounter;
    }

    public void setErrorCounter(int errorCounter) {
        this.errorCounter = errorCounter;
    }
}
