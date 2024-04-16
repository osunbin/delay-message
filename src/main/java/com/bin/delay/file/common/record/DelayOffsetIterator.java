package com.bin.delay.file.common.record;

import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

import com.bin.delay.file.common.record.MemoryRecord.DelayOffset;

public class DelayOffsetIterator implements Iterator<Record> {

    private MemoryInputStream memoryInputStream;

    private PriorityQueue<DelayOffset> offsets = new PriorityQueue<>();

    public DelayOffsetIterator(MemoryInputStream memoryInputStream,List<DelayOffset> index) {
        this.memoryInputStream = memoryInputStream;
        offsets.addAll(index);
    }


    @Override
    public boolean hasNext() {
        return !offsets.isEmpty();
    }

    @Override
    public Record next() {
        DelayOffset delayOffset = offsets.poll();
        return memoryInputStream.next(delayOffset.getLength());
    }
}
