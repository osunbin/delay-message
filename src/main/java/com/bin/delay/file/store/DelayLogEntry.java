package com.bin.delay.file.store;

// 文件名字
// 索引
public class DelayLogEntry {

    private Long delayLogName; // 那个log文件
    private Long offset;        //  log文件下  LogSegment
    private Long offsetIndex; //  索引文件物理偏移量 physical


    public Long getDelayLogName() {
        return delayLogName;
    }

    public void setDelayLogName(Long delayLogName) {
        this.delayLogName = delayLogName;
    }

    public Long getOffset() {
        return offset;
    }

    public void setOffset(Long offset) {
        this.offset = offset;
    }

    public Long getOffsetIndex() {
        return offsetIndex;
    }

    public void setOffsetIndex(Long offsetIndex) {
        this.offsetIndex = offsetIndex;
    }



    public  static int storeSize() {
        return 8 + 8 + 8;
    }


    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("DelayLogEntry{");
        sb.append("delayLogName=").append(delayLogName);
        sb.append(", offset=").append(offset);
        sb.append(", offsetIndex=").append(offsetIndex);
        sb.append('}');
        return sb.toString();
    }
}
