package com.bin.delay.file.common.record;

import com.bin.delay.file.common.errors.DelayException;
import com.bin.delay.file.common.utils.Utils;


import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 写入是一个buffer
 */
public class FileRecords implements Closeable {



    private final int end;


    // mutable state
    private final AtomicInteger size;
    private final FileChannel channel;
    private volatile File file;


    public FileRecords(File file,
                       FileChannel channel,

                       int end) throws IOException {
        this.file = file;
        this.channel = channel;

        this.end = end;
        this.size = new AtomicInteger();

        int limit = Math.min((int) channel.size(), end);
        size.set(limit);

        // if this is not a slice, update the file pointer to the end of the file
        // set the file position to the last byte in the file
        channel.position(limit);

    }


    public int sizeInBytes() {
        return size.get();
    }

    /**
     * Get the underlying file.
     *
     * @return The file
     */
    public File file() {
        return file;
    }

    /**
     * Get the underlying file channel.
     *
     * @return The file channel
     */
    public FileChannel channel() {
        return channel;
    }


    public ByteBuffer readInto(ByteBuffer buffer, int position) throws IOException {
        Utils.readFully(channel, buffer, position);
        buffer.flip();
        return buffer;
    }


    public DefaultRecord read(int position, int length) throws IOException {
        if (position < 0)
            throw new IllegalArgumentException("Invalid position: " + position);
        if (length < 0)
            throw new IllegalArgumentException("Invalid length: " + length);

        if (length > sizeInBytes() -  position) {
            length = sizeInBytes() -  position;
        }
        ByteBuffer allocate = ByteBuffer.allocate(length);
        readInto(allocate,  position);
        return new DefaultRecord(length,position,allocate);
    }

    public MemoryRecord read(int position, int length, List<MemoryRecord.DelayOffset> offsets) throws IOException {
        if (position < 0)
            throw new IllegalArgumentException("Invalid position: " + position);
        if (length < 0)
            throw new IllegalArgumentException("Invalid length: " + length);

        if (length > sizeInBytes() -  position) {
            length = sizeInBytes() -  position;
        }
        ByteBuffer allocate = ByteBuffer.allocate(length);
        readInto(allocate,  position);
        return new MemoryRecord(allocate, offsets);
    }



    public long writeTo(GatheringByteChannel destChannel, long offset, int length) throws IOException {
        long newSize = Math.min(channel.size(), end);
        int oldSize = sizeInBytes();
        if (newSize < oldSize)
            throw new DelayException(String.format(
                    "Size of FileRecords %s has been truncated during write: old size %d, new size %d",
                    file.getAbsolutePath(), oldSize, newSize));


        int count = Math.min(length, oldSize);
        return channel.transferTo(offset, count, destChannel);
    }


    public int append(ByteBuffer buffer, int sizeInBytes) throws IOException {
        int written = writeFullyTo(buffer, sizeInBytes);
        size.getAndAdd(written);
        return written;
    }

    private int writeFullyTo(ByteBuffer buffer, int sizeInBytes) throws IOException {
        buffer.mark();
        int written = 0;
        while (written < sizeInBytes)
            written += channel.write(buffer);
        buffer.reset();
        return written;
    }

    public void flush() throws IOException {
        channel.force(true);
    }


    public void close() throws IOException {
        flush();
        trim();
        channel.close();
    }


    public void closeHandlers() throws IOException {
        channel.close();
    }


    public boolean deleteIfExists() throws IOException {
        Utils.closeQuietly(channel, "FileChannel");
        return Files.deleteIfExists(file.toPath());
    }


    public void trim() throws IOException {
        truncateTo(sizeInBytes());
    }

    public void setFile(File file) {
        this.file = file;
    }


    public void renameTo(File f) throws IOException {
        try {
            Utils.atomicMoveWithFallback(file.toPath(), f.toPath());
        } finally {
            this.file = f;
        }
    }

    public int truncateTo(int targetSize) throws IOException {
        int originalSize = sizeInBytes();
        if (targetSize > originalSize || targetSize < 0)
            throw new DelayException("Attempt to truncate log segment to " + targetSize + " bytes failed, " +
                    " size of this log segment is " + originalSize + " bytes.");
        if (targetSize < (int) channel.size()) {
            channel.truncate(targetSize);
            size.set(targetSize);
        }
        return originalSize - targetSize;
    }


    public static FileRecords open(File file,
                                   boolean mutable,
                                   boolean fileAlreadyExists,
                                   int initFileSize,
                                   boolean preallocate) throws IOException {
        FileChannel channel = openChannel(file, mutable, fileAlreadyExists, initFileSize, preallocate);

        return new FileRecords(file, channel,  initFileSize);
    }

    public static FileRecords open(File file,
                                   boolean fileAlreadyExists,
                                   int initFileSize,
                                   boolean preallocate) throws IOException {
        return open(file, true, fileAlreadyExists, initFileSize, preallocate);
    }

    public static FileRecords open(File file, boolean mutable) throws IOException {
        return open(file, mutable, false, 0, false);
    }

    public static FileRecords open(File file) throws IOException {
        return open(file, true);
    }


    private static FileChannel openChannel(File file,
                                           boolean mutable,
                                           boolean fileAlreadyExists,
                                           int initFileSize,
                                           boolean preallocate) throws IOException {
        if (mutable) {
            if (fileAlreadyExists) {
                return new RandomAccessFile(file, "rw").getChannel();
            } else {
                if (preallocate) {
                    // 预分配  写日志
                    RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
                    randomAccessFile.setLength(initFileSize);
                    return randomAccessFile.getChannel();
                } else {
                    return new RandomAccessFile(file, "rw").getChannel();
                }
            }
        } else {
            return new FileInputStream(file).getChannel();
        }
    }


}
