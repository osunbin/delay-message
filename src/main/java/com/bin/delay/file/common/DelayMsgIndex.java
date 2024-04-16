package com.bin.delay.file.common;

public class DelayMsgIndex implements  Comparable<DelayMsgIndex>{
    private long offset; //log offset;  索引的偏移量
    private int size; //data size
    private long delayEndTime; //seconds
    /**
     *   那个索引文件
     */
    private long delayLogOffset;



    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public long getDelayEndTime() {
        return delayEndTime;
    }

    public void setDelayEndTime(long delayEndTime) {
        this.delayEndTime = delayEndTime;
    }



    public long getDelayLogOffset() {
        return delayLogOffset;
    }

    public void setDelayLogOffset(long delayLogOffset) {
        this.delayLogOffset = delayLogOffset;
    }


    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("DelayMsgIndex{");
        sb.append("offset=").append(offset);
        sb.append(", size=").append(size);
        sb.append(", delayEndTime=").append(delayEndTime);
        sb.append(", delayLogOffset=").append(delayLogOffset);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public int compareTo(DelayMsgIndex o) {
        if (delayLogOffset == o.getDelayLogOffset()) {
           return (int) (offset - o.getOffset());
        }
        return (int) (delayLogOffset - o.getDelayLogOffset());
    }
}
