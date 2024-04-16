package com.bin.delay.file.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public class TimeWheel {
    private static final Logger log = LoggerFactory.getLogger(TimeWheel.class);

    private final AtomicLong pulledTime; // pulled时间和Manager里面是一个时间

    ConcurrentNavigableMap<Long, Queue<DelayMsgIndex>> data =
            new ConcurrentSkipListMap<>();

    private int tick;

    private long second;

    private long lastSecond;

    private int timeWindow;

    public TimeWheel(AtomicLong pulledTime,int timeWindow) {
        this.pulledTime = pulledTime;
        this.timeWindow = timeWindow;
        lastSecond = pulledTime.get();
        this.second = 0;
    }


    public void addToWheel(DelayMsgIndex delayMsgIndex) {
        long delayEndTime = delayMsgIndex.getDelayEndTime();

        if (delayEndTime <= pulledTime.get()) {
            // 已经过期了,要加入时间论需要调整以下时间,
            // 但是不能修改元数据,因为需要根据时间找到对应数据所在的文件
            log.warn("add to polledTime msgIndex {} polledTime {} ",
                    delayMsgIndex, pulledTime);
            delayEndTime = pulledTime.get() + 1;
        }

        Queue<DelayMsgIndex> queue = data.get(delayEndTime);
        if (queue == null) {
            synchronized (this) {
                queue = data.get(delayEndTime);
                if (queue == null) {

                    queue = new ConcurrentLinkedQueue<DelayMsgIndex>();
                    data.put(delayEndTime, queue);
                }
            }
        }
        queue.add(delayMsgIndex);
    }


    public Queue<DelayMsgIndex> remove(long delayEndSecond) {
        return data.remove(delayEndSecond);
    }

    public Queue<DelayMsgIndex> getQueue(long delayEndSecond) {
        if (lastSecond != delayEndSecond) {
            lastSecond = delayEndSecond;
            if (++second % timeWindow == 0) {
                ++tick;
            }
        }
        Queue<DelayMsgIndex> queue = data.get(delayEndSecond);
        if (queue == null) {
            Map.Entry<Long, Queue<DelayMsgIndex>> firstEntry = data.firstEntry();
            if (firstEntry != null) {
                Long key = firstEntry.getKey();
                if (key < delayEndSecond)
                    queue = data.remove(key);
            }
        }
        return queue;
    }

    public Long getPulledTime() {
        return pulledTime.get();
    }


}
