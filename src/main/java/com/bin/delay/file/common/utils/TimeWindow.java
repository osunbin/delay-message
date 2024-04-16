package com.bin.delay.file.common.utils;

public class TimeWindow {

    long startTimeMs; // >=
    long endTimeMs; // <
    long minuteTime;

    public long getStartTimeMs() {
        return startTimeMs;
    }

    public void setStartTimeMs(long startTimeMs) {
        this.startTimeMs = startTimeMs;
    }

    public long getEndTimeMs() {
        return endTimeMs;
    }

    public void setEndTimeMs(long endTimeMs) {
        this.endTimeMs = endTimeMs;
    }

    public long getMinuteTime() {
        return minuteTime;
    }

    public void setMinuteTime(long minuteTime) {
        this.minuteTime = minuteTime;
    }

    @Override
    public String toString() {
        return "TimeSpan [startTimeMs=" + startTimeMs + ", endTimeMs=" + endTimeMs + ", minuteTime=" + minuteTime
                + "]";
    }
}
