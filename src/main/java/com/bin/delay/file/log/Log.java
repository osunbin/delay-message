package com.bin.delay.file.log;

import com.bin.delay.file.common.DelayLogInfo;
import com.bin.delay.file.common.DelayMsgIndex;
import com.bin.delay.file.common.Position;
import com.bin.delay.file.common.record.DefaultRecord;
import com.bin.delay.file.common.utils.DelayUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class Log {
    private static final Logger logger = LoggerFactory.getLogger(Log.class);


    /**
     * a log file
     */
    private static final String LogFileSuffix = ".log";

    /**
     * an index file
     */
    private static final String IndexFileSuffix = ".index";

    /**
     *  202405151600
     */
    private File dir;

    private final long delayMinuteTime;
    /**
     * 开始恢复的偏移量，即尚未刷新到磁盘的第一个偏移量
     */
    private long recoveryPoint;



    private int initFileSize;



    // 返回给定范围的map
    private ConcurrentNavigableMap<Long, LogSegment> segments
            = new ConcurrentSkipListMap<>();

    private final AtomicLong lastOffset = new AtomicLong(0);

    public Log(String baseDir,final long delayMinuteTime) {
        this.delayMinuteTime = delayMinuteTime;
        dir = new File(baseDir,String.valueOf(delayMinuteTime));
    }

    public Log(String baseDir,final long delayMinuteTime,int initFileSize) {
        this.delayMinuteTime = delayMinuteTime;
        dir = new File(baseDir,String.valueOf(delayMinuteTime));
        this.initFileSize = initFileSize;
        roll(0L);
    }



    /**
     *
     */
    public DelayLogInfo append(ByteBuffer buffer,int sizeInBytes,long delayTime) throws IOException {

        long write = lastOffset.get();
        LogSegment logSegment = maybeRoll(sizeInBytes, write);

        int indexSize = logSegment.append(write, buffer, sizeInBytes, delayTime);

        lastOffset.addAndGet(sizeInBytes);

        long baseOffset = logSegment.getBaseOffset();

        try {
            if (this.lastOffset.get() - this.recoveryPoint >= 1024)
                flush(lastOffset.get());
        }catch (Exception e) {
            logger.error("logSegment flush error");
        }
        DelayLogInfo delayLogInfo = new DelayLogInfo(baseOffset,indexSize);

        return delayLogInfo;
    }


    public Map<LogSegment,LinkedList<Position>> readRange(long fromBaseOffset,long fromOffset,
                                                          long toBaseOffset,long toOffset) {
        ConcurrentNavigableMap<Long, LogSegment> logSegments =
                segments.subMap(fromBaseOffset, toBaseOffset);
        Map<LogSegment,LinkedList<Position>> logSegmentPositions = new HashMap<>();
        Set<Long> baseOffsets = logSegments.keySet();
        for (long base : baseOffsets) {
            LinkedList<Position> positions = null;
            LogSegment logSegment = logSegments.get(base);
            if (fromBaseOffset == base) {
                positions = logSegment.readRange(fromOffset);
            } else {
                positions = logSegment.readAll();
            }
            logSegmentPositions.put(logSegment,positions);
        }
        LogSegment logSegment = segments.get(toBaseOffset);
        logSegmentPositions.put(logSegment,logSegment.readRange(0, toOffset));
        return logSegmentPositions;
    }

    public Map<LogSegment,LinkedList<Position>> readAll() {
        Collection<LogSegment> logSegments = segments.values();
        Map<LogSegment,LinkedList<Position>> logSegmentPositions = new HashMap<>();
        for (LogSegment logSegment : logSegments) {
            LinkedList<Position> positions = logSegment.readAll();
            logSegmentPositions.put(logSegment,positions);
        }
        return logSegmentPositions;
    }

    public DefaultRecord read(long baseOffset, long offset) throws IOException {
        LogSegment logSegment = segments.get(baseOffset);

       return logSegment.read(offset);
    }


    public LogSegment maybeRoll(int messagesSize,  long maxOffsetInMessages) {

        LogSegment segment = segments.lastEntry().getValue();

        if (segment.shouldRoll(messagesSize,  maxOffsetInMessages)) {
            long offset = this.lastOffset.get();
            return roll(offset);
        }
        return segment;
    }


    private LogSegment roll(long newOffset) {

        File logFile = Log.logFile(dir, newOffset);
        if (logFile.exists()) {
            logger.warn("Newly rolled segment file {} already exists; deleting it first", logFile.getAbsolutePath());
            try {
                Files.delete(logFile.toPath());
            } catch (IOException e) {
            }
        }
        if (newOffset != 0)
                segments.lastEntry().getValue().trim();
        LogSegment segment = null;
        try {
            segment = LogSegment.open(dir, newOffset, false,initFileSize);
            segments.put(segment.getBaseOffset(), segment);
            flush(newOffset);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return segment;
    }

    public void deleteLog() {

        Iterator<Map.Entry<Long, LogSegment>> iterator =
                segments.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, LogSegment> entry = iterator.next();
            Long key = entry.getKey();
            LogSegment logSegment = entry.getValue();
            try {
                logSegment.deleteIfExists();
            } catch (IOException e) {
                e.printStackTrace();
            }

            logger.info("destory delay log key {} file {} ", key, logSegment.getLog().file().getName());
        }
        try {
            Files.deleteIfExists(dir.toPath());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void loadSegmentFiles() {
        File[] listFile = dir.listFiles();
        if (listFile == null || listFile.length == 0)
            return;
        List<File> files = Arrays.stream(listFile).sorted(Comparator.comparing(File::getName)).collect(Collectors.toList());
        Set<Long> bads  = new HashSet<>();
        for (File file : files) {
            if (file.isFile()) {
                if (isIndexFile(file)) {
                    long offset = offsetFromFile(file);
                    File logFile = Log.logFile(dir, offset);
                    if (!logFile.exists()) {
                        logger.warn("Found an orphaned index file {}, with no corresponding log file.",file.getAbsolutePath());
                        try {
                            Files.deleteIfExists(file.toPath());
                        } catch (IOException e) {
                            e.printStackTrace();
                            bads.add(offset);
                        }
                    }
                } else if (isLogFile(file)) {
                    long baseOffset = offsetFromFile(file);

                    LogSegment segment = null;
                    try {
                        segment = LogSegment.open(dir, baseOffset, true,initFileSize);
                        segments.put(segment.getBaseOffset(), segment);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        for (long offset : bads) {
            logger.warn("Found an orphaned index file  offset {}, " +
                    "with no corresponding log file, remove segments.",offset);
            segments.remove(offset);
        }
    }

    private void flush(long offset) {
        if (offset <= this.recoveryPoint)
            return;
        for (LogSegment segment : logSegments(this.recoveryPoint, offset))
            segment.flush();

        if (offset > this.recoveryPoint)
            this.recoveryPoint = offset;
    }


    public boolean isExists() {
        Map.Entry<Long, LogSegment> longLogSegmentEntry = segments.firstEntry();
        return longLogSegmentEntry.getValue().size() > 0;
    }
    public LogSegment getLastLogSegment() {
        return segments.lastEntry().getValue();
    }

    public LogSegment logSegment(long baseOffset) {
       return segments.get(baseOffset);
    }

    private Collection<LogSegment> logSegments(long from, long to) {
        synchronized (this) {
            ConcurrentNavigableMap<Long, LogSegment> view = null;
            // 返回与小于或等于给定键的最大键关联的键值映射
            Map.Entry<Long, LogSegment> floor = segments.floorEntry(from);
            if (floor != null) {
                view = segments.subMap(floor.getKey(), to);
            } else {
                view = segments.headMap(to);
            }
            return view.values();
        }
    }


    public static File logFile(File dir, long offset) {
        return new File(dir, filenamePrefixFromOffset(offset) + LogFileSuffix);
    }

    private static String filenamePrefixFromOffset(long offset) {
        NumberFormat nf = NumberFormat.getInstance();
        nf.setMinimumIntegerDigits(20);
        nf.setMaximumFractionDigits(0);
        nf.setGroupingUsed(false);
        return nf.format(offset);
    }

    public static File offsetIndexFile(File dir, long offset) {
       return new File(dir, filenamePrefixFromOffset(offset) + IndexFileSuffix);
    }


    private static long offsetFromFile(File file) {
        String filename = file.getName();
        return Long.parseLong(filename.substring(0, filename.indexOf('.')));
    }

    public static boolean isIndexFile(File file) {
        String filename = file.getName();
        return filename.endsWith(IndexFileSuffix);
    }

    private static boolean isLogFile(File file) {
        return file.getPath().endsWith(LogFileSuffix);
    }


}
