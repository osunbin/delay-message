package com.bin.delay.file.common.record;


import com.bin.delay.file.common.utils.ByteUtils;


import java.nio.ByteBuffer;


public class DefaultRecord implements Record {


    private final int sizeInBytes;

    private final long offset;

    private final ByteBuffer value;

    public DefaultRecord(int sizeInBytes,
                          long offset,
                          ByteBuffer value) {
        this.sizeInBytes = sizeInBytes;
        this.offset = offset;
        this.value = value;
    }



    @Override
    public long offset() {
        return offset;
    }


    @Override
    public int sizeInBytes() {
        return sizeInBytes;
    }


    @Override
    public int valueSize() {
        return value == null ? -1 : value.remaining();
    }


    @Override
    public ByteBuffer value() {
        return value == null ? null : value.duplicate();
    }

    @Override
    public String toString() {
        return String.format("DefaultRecord(offset=%d, timestamp=%d, key=%d bytes, value=%d bytes)",
                offset,
                value == null ? 0 : value.limit());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        DefaultRecord that = (DefaultRecord) o;
        return sizeInBytes == that.sizeInBytes &&
                offset == that.offset &&
                (value == null ? that.value == null : value.equals(that.value));
    }

    @Override
    public int hashCode() {
        int result = sizeInBytes;
        result = 31 * result + (int) (offset ^ (offset >>> 32));
        result = 31 * result + (value != null ? value.hashCode() : 0);
        return result;
    }



}
