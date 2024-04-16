package com.bin.delay.file.common.record;

import java.nio.ByteBuffer;

public class MemoryInputStream {


    private int position;
    private ByteBuffer buffer;
    public MemoryInputStream(ByteBuffer buffer) {
        this.buffer = buffer;
        position = 0;
    }

    public DefaultRecord next(int length) {
        ByteBuffer slice = buffer.slice(position, length);
        position += length;
        return new DefaultRecord(length,position,slice);
    }
}
