package com.bin.delay.file;

import com.bin.delay.file.common.record.Record;


public interface DelayHandler {


    boolean delayMessage(Record data);
}
