package com.bin.delay.file.log;

import com.bin.delay.file.common.Position;
import com.bin.delay.file.common.record.DefaultRecord;
import com.bin.delay.file.common.record.FileRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;


public class LogSegment {

    private static final Logger logger = LoggerFactory.getLogger(LogSegment.class);

    static int maxIndexSize = 10 * 1024 * 1024;


    private FileRecords log;
    private long baseOffset;


    /**
     * 每个 Segment 最大值 1G 或者 128MB
     */
    private int maxSegmentBytes = 512 * 1024 * 1024;


    private OffsetIndex offsetIndex;




    public LogSegment(FileRecords log, OffsetIndex offsetIndex, long baseOffset, int maxSegmentBytes) {
        this.log = log;
        this.baseOffset = baseOffset;
        this.offsetIndex = offsetIndex;
        this.maxSegmentBytes = maxSegmentBytes;
    }

    /**
     * 时间轮 找到 索引 索引找到数据
     */
    public int append(long firstOffset, ByteBuffer buffer, int sizeInBytes, long delayTime) throws IOException {

        // 文件偏移量
        int physicalPosition = log.sizeInBytes();
        logger.trace("Inserting {} bytes at offset {} at position {} with largest ", sizeInBytes, firstOffset, log.sizeInBytes());
        int appendedBytes = log.append(buffer, sizeInBytes);
        logger.trace("Appended {} to {} at offset {}", appendedBytes, log.file(), firstOffset);
        //  TODO   物理文件的起始偏移量
        int size = offsetIndex.sizeInBytes();
        offsetIndex.append(physicalPosition, delayTime);
        return size;
    }

    /**
     *
     */
    public DefaultRecord read(long offset) throws IOException {


        Position lookup = offsetIndex.lookup(offset);
        if (lookup == null)
            return null;
        int next = offsetIndex.lookupNext(offset);
        int length = next - lookup.getPosition();
        return log.read(lookup.getPosition(), length);

    }


    public LinkedList<Position> readRange(long from) {
       return offsetIndex.load(from);
    }

    public LinkedList<Position> readRange(long from,long to) {
        return offsetIndex.load(from,to);
    }

    public LinkedList<Position> readAll() {
        // TODO 拉取全部数据
        return offsetIndex.lookAll();
    }

    public boolean shouldRoll(int messagesSize, long maxOffsetInMessages) {
        // 初始化文件大小  - 本次分配的文件大小 - 当前偏移量
        return (0 > maxSegmentBytes - messagesSize - size() || size() > 0
                || offsetIndex.isFull()
                || !canConvertToRelativeOffset(maxOffsetInMessages));
    }

    private void resizeIndexes(int size) throws IOException {
        offsetIndex.resize(size);
    }

    private void sanityCheck() {
        if (offsetIndex.getFile().exists()) {
            offsetIndex.sanityCheck();
        }
    }




    public void onBecomeInactiveSegment() throws IOException {
        offsetIndex.trimToValidSize();
    }

    private boolean canConvertToRelativeOffset(long offset) {
        return (offset - baseOffset) <= Integer.MAX_VALUE;
    }


    public int size() {
        return log.sizeInBytes();
    }

    public int indexSize() {
        return offsetIndex.sizeInBytes();
    }

    public void flush() {
        try {
            log.flush();
            offsetIndex.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void trim() {
        try {
            log.trim();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public FileRecords getLog() {
        return log;
    }

    public long getBaseOffset() {
        return baseOffset;
    }


    public int getMaxSegmentBytes() {
        return maxSegmentBytes;
    }

    public static LogSegment open(File dir, long baseOffset, boolean fileAlreadyExists,
                                  int maxSegmentSize) throws IOException {
        return new LogSegment(
                FileRecords.open(Log.logFile(dir, baseOffset), fileAlreadyExists, maxSegmentSize, false),
                new OffsetIndex(Log.offsetIndexFile(dir, baseOffset), baseOffset, maxIndexSize),
                baseOffset,
                maxSegmentSize);
    }

    public void close() throws IOException {
        offsetIndex.close();
        log.close();
    }

    public void closeHandlers() throws IOException {
        offsetIndex.closeHandler();
        log.closeHandlers();
    }

    public void deleteIfExists() throws IOException {
        offsetIndex.deleteIfExists();
        log.deleteIfExists();
    }
}
