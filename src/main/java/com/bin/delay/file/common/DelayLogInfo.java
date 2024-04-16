package com.bin.delay.file.common;

public class DelayLogInfo {
    private long logOffset;

    private long offset;

    public DelayLogInfo(long logOffset, long offset) {
        this.logOffset = logOffset;
        this.offset = offset;
    }

    public long getLogOffset() {
        return logOffset;
    }

    public void setLogOffset(long logOffset) {
        this.logOffset = logOffset;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }
}
