package com.bin.delay.file.common;

import java.nio.ByteBuffer;

public class DelayRequest {

    private final String topic;
    private final int partition;
    private final ByteBuffer msgByteBuffer;
    private final long delayTime;


    public DelayRequest(String topic,int partition,
                        ByteBuffer msgByteBuffer,
                        long delayTime) {
        this.topic = topic;
        this.partition = partition;
        this.msgByteBuffer = msgByteBuffer;
        this.delayTime = delayTime;
    }


    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    public ByteBuffer getMsgByteBuffer() {
        return msgByteBuffer;
    }

    public long getDelayTime() {
        return delayTime;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("DelayRequest{");
        sb.append("topic='").append(topic).append('\'');
        sb.append(", partition=").append(partition);
        sb.append(", msgByteBufferLength=").append(msgByteBuffer.remaining());
        sb.append(", delayTime=").append(delayTime);
        sb.append('}');
        return sb.toString();
    }
}
