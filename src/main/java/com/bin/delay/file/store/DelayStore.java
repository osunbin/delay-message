package com.bin.delay.file.store;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;


public class DelayStore {

    private static final String delay_store = "delay_store.txt";

    private static final String delay_store_bak = "delay_store_bak.txt";

    private static final String fail_delay_store = "fail_delay_store.txt";



    private BufferedRaf logRaf;
    private BufferedRaf faiLogRaf;
    private File currentFile;

    private String storePath;


    private String storeBakPath;

    private String failStorePath;



    private boolean bakflag = false;

    public DelayStore(String baseDir) {
        storePath = baseDir + File.separator + delay_store;
        storeBakPath = baseDir + File.separator + delay_store_bak;
        failStorePath = baseDir + File.separator + fail_delay_store;
        this.currentFile = new File(storePath);
    }

    public void open() throws IOException {
        if (!this.currentFile.exists() && !this.currentFile.createNewFile() && !this.currentFile.exists()) {
            throw new IOException("Cannot create directory " + this.currentFile.getAbsolutePath());
        } else {
            BufferedRaf oldRaf = openForAppend(new File(storeBakPath));
            long length = oldRaf.length();
            if (length > 0) {
                this.logRaf = oldRaf;
                bakflag = true;
            } else {
                this.logRaf = openForAppend(this.currentFile);
            }

        }
    }



    public void switchSnapshot() throws IOException {
        File newFile = null;
        if (!bakflag) {
            newFile = new File(storeBakPath);
            bakflag = true;
        } else {
            newFile = new File(storePath);
            bakflag = false;
        }
        BufferedRaf newRaf = openForAppend(newFile);

        this.logRaf.close();
        this.logRaf = newRaf;
        File old = currentFile;
        this.currentFile = newFile;
        Files.delete(old.toPath());
    }

    public void writeFailEntry(DelayLogEntry delayLogEntry) {
        if (faiLogRaf == null) {
            try {
                faiLogRaf = openForAppend(new File(failStorePath));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        if (faiLogRaf != null) {
            try {
                faiLogRaf.writeLong(delayLogEntry.getDelayLogName());
                faiLogRaf.writeLong(delayLogEntry.getOffset());
                faiLogRaf.writeLong(delayLogEntry.getOffsetIndex());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    public void writeEntry(DelayLogEntry delayLogEntry) throws IOException {
        logRaf.writeLong(delayLogEntry.getDelayLogName());
        logRaf.writeLong(delayLogEntry.getOffset());
        logRaf.writeLong(delayLogEntry.getOffsetIndex());
    }

    public static void main(String[] args) throws IOException {
        DelayStore delayStore = new DelayStore("D:\\code\\self");

        delayStore.open();

        /*DelayLogEntry delayLogEntry = new DelayLogEntry();
        delayLogEntry.setDelayLogName(201901081500L);
        delayLogEntry.setOffset(345435L);
        delayLogEntry.setOffsetIndex(453453454L);
        delayStore.writeEntry(delayLogEntry);
        delayStore.flushLogs();
        System.out.println(delayStore.logRaf.length());*/

        DelayLogEntry lastEntry = delayStore.getLastEntry();
        if (lastEntry != null)
            System.out.println(lastEntry);

        System.out.println("测试结束");
        delayStore.close();
    }

    public DelayLogEntry getLastEntry() {
        long length = logRaf.length();
        int storeSize = DelayLogEntry.storeSize();
        if (length >= storeSize) {
            logRaf.seek(length - storeSize);
            Long delayLogName = null;
            Long offset = null;
            Long offsetIndex = null;
            try {
                delayLogName = logRaf.readLong();
                offset = logRaf.readLong();
                offsetIndex = logRaf.readLong();
            } catch (IOException e) {
                // 异常
                return null;
            }
            DelayLogEntry delayLogEntry = new DelayLogEntry();
            delayLogEntry.setDelayLogName(delayLogName);
            delayLogEntry.setOffset(offset);
            delayLogEntry.setOffsetIndex(offsetIndex);
            return delayLogEntry;
        }
        return null;
    }

    public void flushLogs() throws IOException {
        this.logRaf.force();

    }

    public void close() throws IOException {
        this.flushLogs();
        this.logRaf.close();
    }


    private static BufferedRaf openForAppend(File file) throws IOException {
        BufferedRaf raf = new BufferedRaf(new RandomAccessFile(file, "rw"));
        raf.seek(raf.length());
        return raf;
    }


}
