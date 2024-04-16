package com.bin.delay.file.common.record;


import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

public class MemoryRecord {

    private final ByteBuffer buffer;
    private final int length;

    private Iterable<Record> batches;



    public MemoryRecord(ByteBuffer buffer, List<DelayOffset> index) {
        this.buffer = buffer;
        length = buffer.remaining();
        batches = batchesFrom(index);
    }


    public Iterable<Record> batches() {
        return batches;
    }

    private Iterable<Record> batchesFrom(List<DelayOffset> index) {
        return new Iterable<Record>() {
            @Override
            public Iterator<Record> iterator() {
                return batchIterator(index);
            }
        };
    }


    private Iterator<Record> batchIterator(List<DelayOffset> index) {

        MemoryInputStream memoryInputStream = new MemoryInputStream(buffer);

        return new DelayOffsetIterator(memoryInputStream, index);
    }


    public class DelayOffset implements Comparable<DelayOffset> {
        private int offset;
        private int length;

        public int getOffset() {
            return offset;
        }

        public void setOffset(int offset) {
            this.offset = offset;
        }

        public int getLength() {
            return length;
        }

        public void setLength(int length) {
            this.length = length;
        }


        @Override
        public int compareTo(DelayOffset o) {
            return this.offset - o.offset;
        }
    }
}
