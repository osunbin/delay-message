package com.bin.delay.file;

import com.bin.delay.file.common.DelayLogInfo;
import com.bin.delay.file.common.DelayMsgIndex;
import com.bin.delay.file.common.DelayRequest;
import com.bin.delay.file.common.Position;
import com.bin.delay.file.common.TimeWheel;
import com.bin.delay.file.common.record.DefaultRecord;
import com.bin.delay.file.common.utils.DelayUtil;
import com.bin.delay.file.common.utils.TimeWindow;
import com.bin.delay.file.log.LoadJob;
import com.bin.delay.file.log.Log;
import com.bin.delay.file.log.LogSegment;
import com.bin.delay.file.store.DelayLogEntry;
import com.bin.delay.file.store.DelayStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.concurrent.Executors.defaultThreadFactory;

public class DelayLogManager {

    private static final Logger logger = LoggerFactory.getLogger(DelayLogManager.class);


    // 128M delay log default size
    private static final int delayFileSize = 1024 * 1024 * 512;
    // default delayLog TimeSpan 30 minute
    private static final int delayLogTimeSpan = 30;


    // Resource reclaim interval
    private static final int cleanResourceInterval = 60 * 60;

    private static final long aDayTime = 24 * 60 * 60 * 1000;

    private static final String delayLogPath = "delaylog";


    private static final long startSecond = DelayUtil.getCruSecond();
    private final Object lockObj = new Object();
    private final int initFileSize;
    private volatile LoadJob loadingOffset;
    private final int timeWindow;

    private int loadTimeSpan = 6 * 60 * 1000;



    private final String storePath;
    private final DelayHandler delayHandler;
    private final ConcurrentSkipListMap<Long, Log> delayLogMap =
            new ConcurrentSkipListMap<>(); // forExample 201901081500
    private ExpiredOperation expiredOperation;

    private ScheduledExecutorService sheduleExe = Executors.newScheduledThreadPool(1, new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            Thread thread = defaultThreadFactory().newThread(r);
            thread.setName("DelayLogSchedule_" + r.getClass().getSimpleName());

            return thread;
        }
    });

    private final TimeWheel timeWheel;

    private final String rootDir;
    private final AtomicLong lastPulledSecond;

    private volatile TimeWindow cruPullTimeWindow;

    private long lastDelayStore = startSecond;
    private DelayStore delayStore;

    public DelayLogManager(String rootDir, DelayHandler delayHandler) {
        this(rootDir, delayLogTimeSpan, delayHandler);
    }


    public DelayLogManager(String rootDir, int timeWindow, DelayHandler delayHandler) {
        this.rootDir = rootDir;
        this.storePath = rootDir + File.separator + delayLogPath;
        this.timeWindow = timeWindow;
        this.lastPulledSecond = new AtomicLong(startSecond - 3);
        initFileSize = delayFileSize;
        this.timeWheel = new TimeWheel(lastPulledSecond, timeWindow);
        delayStore = new DelayStore(rootDir);
        this.delayHandler = delayHandler;


    }


    public void start() {

        LoadLogSegmentJob loadLogJob = new LoadLogSegmentJob();

        sheduleExe.scheduleAtFixedRate(loadLogJob, 60 * 2, 60 * 2, TimeUnit.SECONDS);

        sheduleExe.scheduleAtFixedRate(new DeleteDelaylogJob(),
                1000 * 60, cleanResourceInterval, TimeUnit.SECONDS);
        expiredOperation = new ExpiredOperation();
        expiredOperation.start();


    }

    public void shutdown() {
        expiredOperation.shutdown();
        sheduleExe.shutdown();
        try {
            delayStore.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public void load() {
        cruPullTimeWindow = DelayUtil.getCruDelaySpan(lastPulledSecond.get(), timeWindow);
        boolean load = true;
        try {
            delayStore.open();
            DelayLogEntry lastEntry = delayStore.getLastEntry();
            if (lastEntry != null) {
                long minuteTime = cruPullTimeWindow.getMinuteTime();
                Long delayLogName = lastEntry.getDelayLogName();
                Long offset = lastEntry.getOffset();
                Long offsetIndex = lastEntry.getOffsetIndex();
                delayStore.switchSnapshot();

                if (delayLogName < minuteTime) {
                    // 历史数据  名称是索引结尾    log 文件   logSegment  偏移量
                    TimeWindow prevLoadTime = DelayUtil.getPrevLoadTime(lastPulledSecond.get(), timeWindow);
                    long endMinuteTime = prevLoadTime.getMinuteTime();
                    startLoad(delayLogName, null,
                            offset, offsetIndex);
                    if (delayLogName != minuteTime) {
                        for (long i = delayLogName + timeWindow;i <= endMinuteTime;i+=timeWindow) {
                            startLoad(i, null,
                                    0, 0);
                        }

                    }

                } else if (delayLogName == minuteTime) {
                    // 当前数据
                    startLoad(delayLogName, cruPullTimeWindow,
                            offset, offsetIndex);
                    load = false;
                }
            }
            // 处理历史数据

        } catch (IOException e) {
            e.printStackTrace();
        }

        logger.info("load lastPulled second {} ", lastPulledSecond.get());
        if (load)
            startLoad(cruPullTimeWindow.getMinuteTime(), cruPullTimeWindow,
                    0, 0);



    }

    public void append(DelayRequest delayRequest) throws IOException {
        logger.debug("append to delay log {}", delayRequest.getTopic());
        long delayTime = delayRequest.getDelayTime();

        long delayTimeSecond = DelayUtil.getSecond(delayTime);

        long delayTimeMinute = DelayUtil.getDelayMinuteTime(delayTime, timeWindow);
        Log log = delayLogMap.get(delayTimeMinute);
        if (log == null) {
            log = createNewLog(delayTimeMinute);
        }
        ByteBuffer msgByteBuffer = delayRequest.getMsgByteBuffer();

        DelayLogInfo offsetInfo = log.append(msgByteBuffer, msgByteBuffer.remaining(), delayTimeSecond);

        if (needAddTimeWheel(delayTime, offsetInfo.getLogOffset(), offsetInfo.getLogOffset())) {
            DelayMsgIndex delayMsgIndex = new DelayMsgIndex();
            delayMsgIndex.setDelayEndTime(delayTimeSecond);
            delayMsgIndex.setOffset(offsetInfo.getOffset());  // 文件内那个位置
            delayMsgIndex.setDelayLogOffset(offsetInfo.getLogOffset());// 那个文件

            // delayMsgIndex.setSize();
            logger.debug("add to timeWheel DelayMsgIndex {}  topic {}", delayMsgIndex, delayRequest.getTopic());
            timeWheel.addToWheel(delayMsgIndex);
        }
    }


    private boolean needAddTimeWheel(long delayEndTimeMs, long baseOffset, long offset) {
        if (delayEndTimeMs >= cruPullTimeWindow.getStartTimeMs() &&
                delayEndTimeMs < cruPullTimeWindow.getEndTimeMs())
            return true;
        synchronized (lockObj) {
            if (loadingOffset != null) {
                TimeWindow timeWindow = loadingOffset.getTimeSpan();
                if (loadingOffset != null && delayEndTimeMs >= timeWindow.getStartTimeMs() &&
                        delayEndTimeMs < timeWindow.getEndTimeMs()
                        && (baseOffset >= loadingOffset.getBaseOffset() &&
                        offset > loadingOffset.getOffset()))
                    return true;
            }
        }
        return false;
    }


    private synchronized Log createNewLog(long delayTimeMinute) {
        Log log = delayLogMap.get(delayTimeMinute);
        if (log == null) {
            log = new Log(storePath, delayTimeMinute, initFileSize);
            delayLogMap.put(delayTimeMinute, log);
            logger.debug("add new logSegment {}", delayTimeMinute);
        }
        return log;
    }


    class ExpiredOperation extends Thread {

        volatile boolean stopped = false;
        volatile int count = 0;

        public ExpiredOperation() {
            setName("expired op timeWheel");
        }

        @Override
        public void run() {
            logger.info("{} start run ", this.getName());
            while (!stopped) {
                try {
                    Long second = null;
                    if (lastPulledSecond != null && lastPulledSecond.get() != 0)
                        second = lastPulledSecond.get() + 1;
                    else
                        second = DelayUtil.getCruSecond();

                    long timeDiff = System.currentTimeMillis() - second * 1000;
                    if (timeDiff >= 0)
                        doRun(second);

                    if (second - lastDelayStore == 60 * 10)  // 10分钟
                        delayStore.switchSnapshot();

                    long sleepTimeMs = (lastPulledSecond.get() + 1) * 1000 -
                            System.currentTimeMillis();
                    if (sleepTimeMs > 0 && !stopped)
                        Thread.sleep(sleepTimeMs);

                } catch (Throwable ex) {
                    logger.warn(this.getName() + " service has exception. ", ex);
                }
            }
        }


        private void doRun(Long second) {
            try {
                Queue<DelayMsgIndex> queue = timeWheel.getQueue(second);
                cruPullTimeWindow = DelayUtil.getCruDelaySpan(second, timeWindow);
                logger.debug("start deal second {} ", second);
                if (queue == null) {
                    lastPulledSecond.compareAndSet(second - 1, second);
                    return;
                }
                Log log = delayLogMap.get(cruPullTimeWindow.getMinuteTime());


                Iterator<DelayMsgIndex> iterator = queue.iterator();

                while (iterator.hasNext()) {
                    DelayMsgIndex delayMsgIndex = iterator.next();


                    try {
                        DefaultRecord record =
                                log.read(delayMsgIndex.getDelayLogOffset(),
                                        delayMsgIndex.getOffset());
                        if (record != null) {
                            delayHandler.delayMessage(record);
                            logger.debug("delayMessage delayMsgIndex {} record {} ", delayMsgIndex, record);


                            DelayLogEntry entry = new DelayLogEntry();
                            entry.setDelayLogName(cruPullTimeWindow.getMinuteTime());
                            entry.setOffsetIndex(delayMsgIndex.getDelayLogOffset());
                            entry.setOffset(delayMsgIndex.getOffset());
                            try {
                                delayStore.writeEntry(entry);
                                if (++count % 20 == 0) {
                                    delayStore.flushLogs();
                                }
                            } catch (Exception e) {
                                logger.warn("delayStore error", e);
                            }
                        }
                        iterator.remove();
                    } catch (Exception ex) {
                        DelayLogEntry entry = new DelayLogEntry();
                        entry.setDelayLogName(cruPullTimeWindow.getMinuteTime());
                        entry.setOffsetIndex(delayMsgIndex.getDelayLogOffset());
                        entry.setOffset(delayMsgIndex.getOffset());
                        delayStore.writeFailEntry(entry);

                        logger.error(" delayMessage error index {} ", delayMsgIndex, ex);
                    }


                }

                logger.debug("pull And send  success second {} msg size {}", second, queue.size());
                timeWheel.remove(second); // 从时间轮里面移除，注意
                lastPulledSecond.compareAndSet(second - 1, second);

            } catch (Exception ex) {
                logger.error("poll msg second {} error ", second, ex);
            }
        }

        public void shutdown() {
            this.stopped = true;
            logger.info("shutdown thread " + this.getName());

        }
    }


    private void startLoad(long delayMinitueTime, TimeWindow timeWindow,
                           long baseOffset, long offset) {
        logger.debug("start to load delayMinitueTime {} ", delayMinitueTime);
        Log log = delayLogMap.get(delayMinitueTime);
        if (log == null) {

            log = new Log(storePath, delayMinitueTime, initFileSize);
            log.loadSegmentFiles();
            delayLogMap.put(delayMinitueTime, log);
        }
        if (!log.isExists()) {
            log.deleteLog();
            logger.debug("start load , but next {} log is null", delayMinitueTime);
            return;
        }
        if (loadingOffset != null && !loadingOffset.isError() && loadingOffset.getMinuteTime() == delayMinitueTime) {
            logger.info("start load return ,have another loadjob delayMinitueTime {} loadingOffset {} ", delayMinitueTime, loadingOffset);
            return;
        }


        LogSegment lastLogSegment = log.getLastLogSegment();
        long lastBaseOffset = lastLogSegment.getBaseOffset();

        long lastOffset = lastLogSegment.indexSize();

        synchronized (lockObj) {
            loadingOffset = new LoadJob(delayMinitueTime, lastBaseOffset, lastOffset, timeWindow);
        }
        logger.info("start load delayMinitueTime {} LoadOffset {}", delayMinitueTime, loadingOffset);
        int count = 0;
        Map<LogSegment, LinkedList<Position>> logSegmentPositions =
                log.readRange(baseOffset, offset, lastBaseOffset, lastOffset);

        Set<LogSegment> logSegments = logSegmentPositions.keySet();
        for (LogSegment logSegment : logSegments) {
            LinkedList<Position> positions = logSegmentPositions.get(logSegment);

            for (Position position : positions) {
                DelayMsgIndex index = new DelayMsgIndex();
                index.setDelayLogOffset(logSegment.getBaseOffset());
                index.setDelayEndTime(position.getDelayTime());
                index.setOffset(position.getPosition());
                timeWheel.addToWheel(index);
                logger.debug("startLoad add to timeWheel {} ", index);
                count++;
            }
        }

        loadingOffset.setHaveDoneSuccess();
        logger.info("load end delayMinitueTime {} LoadOffset {} success num {} ", delayMinitueTime, loadingOffset, count);

    }


    class LoadLogSegmentJob implements Runnable {

        @Override
        public void run() {
            logger.info("LoadLogSegmentJob run");

            TimeWindow cruMinuteTimeWindow = cruPullTimeWindow;

            // 上一次的load 任务的结束点
            if (loadingOffset != null) {
                return;

            }
            long diff = cruMinuteTimeWindow.getEndTimeMs() - lastPulledSecond.get() * 1000;
            // 离下一个节点还剩5分钟的时候，才load
            if (diff > loadTimeSpan)
                return;
            TimeWindow nextDelaySpan = DelayUtil.getNextLoadTime(lastPulledSecond.get(), timeWindow);
            long nextDelayMinitueTime = nextDelaySpan.getMinuteTime();
            for (int i = 0; i < 3; i++) {
                try {
                    startLoad(nextDelayMinitueTime, nextDelaySpan,
                            0, 0);
                    break;
                } catch (Throwable ex) {
                    loadingOffset.setErrorStatus();
                    ; //异常结束
                    logger.error("start load {} fail ********** may cause repeat ********** ", nextDelayMinitueTime, ex);
                }
            }

        }
    }

    class DeleteDelaylogJob implements Runnable {
        @Override
        public void run() {
            try {
                boolean isTimeDelete = DelayUtil.isTimetoDelete();
                if (isTimeDelete) {
                    long polledMs = lastPulledSecond.get() * 1000;
                    long delete =  polledMs - aDayTime;
                    // 这之前的文件都删除
                    long timeToMinuteTime = DelayUtil.getTimeToMinuteTime(delete);

                    File dir = new File(storePath);
                    File[] files = dir.listFiles();
                    if (files == null)
                        return;
                    Arrays.sort(files);

                    for (File file : files) {
                        String fileName = file.getName();
                        long minute = DelayUtil.parseFileName(fileName).getMinute();
                        if (minute > timeToMinuteTime)
                            break;

                        if (delayLogMap.containsKey(minute))
                            continue;
                        Log expiredLog = new Log(storePath, minute);
                        expiredLog.loadSegmentFiles();
                        expiredLog.deleteLog();

                    }

                }

            } catch (Throwable ex) {
                logger.error("delete delaylog fail ", ex);
            }
        }
    }


}
