package com.bin.delay.file.log;

import com.bin.delay.file.common.utils.TimeWindow;

public class LoadJob {
    private TimeWindow timeWindow;
    private long minuteTime;
    private long baseOffset;
    private long offset;
    private JobStatus status;



    public LoadJob(long minuteTime, long baseOffset, long offset, TimeWindow timeWindow){
        this.minuteTime = minuteTime;
        this.baseOffset = baseOffset;
        this.offset = offset;
        this.timeWindow = timeWindow;
        this.status = JobStatus.Doing;
    }


    public TimeWindow getTimeSpan() {
        return timeWindow;
    }

    public void setTimeSpan(TimeWindow timeWindow) {
        this.timeWindow = timeWindow;
    }

    public long getMinuteTime() {
        return minuteTime;
    }

    public void setMinuteTime(long minuteTime) {
        this.minuteTime = minuteTime;
    }

    public long getBaseOffset() {
        return baseOffset;
    }

    public void setBaseOffset(long baseOffset) {
        this.baseOffset = baseOffset;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public JobStatus getStatus() {
        return status;
    }

    public boolean isError() {
      return LoadJob.JobStatus.Error.equals(status);
    }

    public void setStatus(JobStatus status) {
        this.status = status;
    }

    public void setHaveDoneSuccess(){
        this.status = JobStatus.DoneSuccess;
    }

    public void setErrorStatus(){
        this.status = JobStatus.Error;
    }


    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("LoadJob{");
        sb.append("timeSpan=").append(timeWindow);
        sb.append(", minuteTime=").append(minuteTime);
        sb.append(", baseOffset=").append(baseOffset);
        sb.append(", offset=").append(offset);
        sb.append(", status=").append(status);
        sb.append('}');
        return sb.toString();
    }

    public static enum JobStatus {
        Doing,DoneSuccess,Error
    }

}
