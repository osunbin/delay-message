package com.bin.delay.file.log;

import com.bin.delay.file.common.Position;
import com.bin.delay.file.common.utils.MappedByteBuffers;
import com.bin.delay.file.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import java.util.concurrent.locks.ReentrantLock;

import static java.lang.Math.ceil;

public class OffsetIndex {

    private static final Logger logger = LoggerFactory.getLogger(OffsetIndex.class);


    private File file;
    /**
     * 文件名
     */
    private long baseOffset;
    private int maxIndexSize = -1;

    private long _length;


    private MappedByteBuffer mmap;


    private volatile int _maxEntries;


    private volatile int _entries;



    ReentrantLock lock = new ReentrantLock();

    /**
     * delayTime      long 8
     * physicalOffset int  4
     */
    public int entrySize() {
        return 12;
    }

    public OffsetIndex(File file, long baseOffset, int maxIndexSize) {
        this.file = file;
        this.baseOffset = baseOffset;
        this.maxIndexSize = maxIndexSize;
        mmap = openMmap();
        _maxEntries = mmap.limit() / entrySize();
        _entries = mmap.position() / entrySize();

    }


    private MappedByteBuffer openMmap() {

        MappedByteBuffer idx = null;
        RandomAccessFile raf = null;
        try {
            boolean newlyCreated = file.createNewFile();
            raf = new RandomAccessFile(file, "rw");
            if (newlyCreated) {
                if (maxIndexSize < entrySize())
                    throw new IllegalArgumentException("Invalid max index size: " + maxIndexSize);
                raf.setLength(roundDownToExactMultiple(maxIndexSize, entrySize()));
            }

            this._length = raf.length();
            idx = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, _length);
            if (newlyCreated)
                idx.position(0);
            else
                // if this is a pre-existing index, assume it is valid and set position to last entry
                idx.position(roundDownToExactMultiple(idx.limit(), entrySize()));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (raf != null) {
                try {
                    raf.close();
                } catch (IOException e) {
                }
            }
        }
        return idx;
    }


    public synchronized boolean resize(int newSize) throws IOException {
        int roundedNewSize = roundDownToExactMultiple(newSize, entrySize());
        if (_length == roundedNewSize) {
            return false;
        } else {
            RandomAccessFile raf = null;
            try {
                raf = new RandomAccessFile(file, "rw");
                int position = mmap.position();

                if (IS_WINDOWS)
                    safeForceUnmap();

                raf.setLength(roundedNewSize);
                _length = roundedNewSize;
                mmap = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, roundedNewSize);
                _maxEntries = mmap.limit() / entrySize();
                mmap.position(position);
                return true;
            } finally {
                if (raf != null) {
                    try {
                        raf.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }


    public void renameTo(File f) throws IOException {
        try {
            Utils.atomicMoveWithFallback(file.toPath(), f.toPath());
        } finally {
            file = f;
        }
    }


    public synchronized void flush() {
        mmap.force();
    }

    public boolean deleteIfExists() throws IOException {
        synchronized (this) {
            safeForceUnmap();
        }
        Files.deleteIfExists(file.toPath());
        return true;
    }

    // 修剪此段以刚好适合有效的条目，并从文件中删除所有尾随的未写入字节
    public synchronized void trimToValidSize() throws IOException {
        resize(entrySize() * _entries);
    }


    public int sizeInBytes() {
        return entrySize() * _entries;
    }

    public void close() throws IOException {
        trimToValidSize();
    }


    public synchronized void closeHandler() {
        safeForceUnmap();
    }

    public void sanityCheck() {
       if (length() % entrySize() != 0)
            throw new RuntimeException("Index file " + file.getAbsolutePath() + " is corrupt, found " + length() + " bytes which is " +
                    "neither positive nor a multiple of " + entrySize());
    }

    public void truncate() {
        truncateToEntries(0);
    }




    private synchronized void truncateToEntries(int entries) {
        _entries = entries;
        mmap.position(_entries * entrySize());
    }




    public Position parseEntry(ByteBuffer buffer, int n) {
        return new Position(physical(buffer, n), relativeDelayTime(buffer, n));
    }

    private long relativeDelayTime(ByteBuffer buffer, int n) {
        return buffer.getLong(n * entrySize() + 4);
    }


    private int physical(ByteBuffer buffer, int n) {
        return buffer.getInt(n * entrySize());
    }


    private synchronized Position lastEntry() {
        if (_entries == 0) {
            return null;
        }
        return parseEntry(mmap, _entries - 1);
    }




    public LinkedList<Position> load(long from){
        return load(from,(long)(_entries - 1) * entrySize());
    }

    // TODO 0  120
    //  0  -> 10
    public LinkedList<Position> load(long from, long to) {
        if (IS_WINDOWS)
            lock.lock();

        try {
            MappedByteBuffer idx = mmap.duplicate();
            int length = (int) (to - from);
            int size = length / entrySize();
            int start = (int) (from / entrySize());
            LinkedList<Position> positions = new LinkedList<>();
            for (int i = start; i <= start + size; i++) {
                positions.addLast(parseEntry(idx, i));
            }

            return positions;
        } finally {
            if (IS_WINDOWS)
                lock.unlock();
        }
    }

    public LinkedList<Position> lookAll() {
        if (IS_WINDOWS)
            lock.lock();

        try {

            MappedByteBuffer idx = mmap.duplicate();
            LinkedList<Position> positions = new LinkedList<>();
            for (int i = 0; i < _entries; i++) {
                positions.addLast(parseEntry(idx, i));
            }
            return positions;
        } finally {
            if (IS_WINDOWS)
                lock.unlock();
        }
    }


    public Position lookup(long targetOffset) {
        if (IS_WINDOWS)
            lock.lock();
        try {
            if(_entries == 0)
                return null;
            MappedByteBuffer idx = mmap.duplicate();
            int slot = (int) (targetOffset / entrySize());
            if (slot >= _entries) {
                return null;
            }
            return parseEntry(idx, slot);
        } finally {
            if (IS_WINDOWS)
                lock.unlock();
        }
    }

    public int lookupNext(long targetOffset) {
        if (IS_WINDOWS)
            lock.lock();
        try {
            MappedByteBuffer idx = mmap.duplicate();
            int slot = (int) (targetOffset / entrySize());
            if (slot == _entries - 1) {
                return lastEntry().getPosition();
            }
            Position position = parseEntry(idx, slot + 1);
            return position.getPosition();
        } finally {
            if (IS_WINDOWS)
                lock.unlock();
        }
    }




    public Position entry(int n) {
        if (IS_WINDOWS)
            lock.lock();
        try {
            if (n >= _entries)
                throw new IllegalArgumentException("Attempt to fetch the " + n + "th entry from an index of size " + _entries);
            MappedByteBuffer idx = mmap.duplicate();

            return parseEntry(idx,n);
        } finally {
            if (IS_WINDOWS)
                lock.unlock();
        }
    }

    // TODO
    public synchronized void append( int position, long delayTime) {
        Objects.requireNonNull(!isFull(), "Attempt to append to a full index (size = " + _entries + ").");

        logger.debug("Adding index entry {}to {}.",  position, file.getName());

        mmap.putInt(position);
        mmap.putLong(delayTime);
        _entries += 1;

        Objects.requireNonNull(_entries * entrySize() == mmap.position(), entries() + " entries but file position in index is " + mmap.position() + ".");

    }


    public void reset() throws IOException {
        truncate();
        resize(maxIndexSize);
    }


    public boolean isFull() {
        return _entries >= _maxEntries - entrySize();
    }


    public int maxEntries() {
        return _maxEntries;
    }

    public int entries() {
        return _entries;
    }


    private long length() {
        return _length;
    }







    private int roundDownToExactMultiple(int number, int factor) {
        return factor * (number / factor);
    }

    private void safeForceUnmap() {

        try {
            MappedByteBuffers.unmap(file.getAbsolutePath(), mmap);
        } catch (IOException e) {
            logger.error("Error unmapping index {}", file, e);
        } finally {
            mmap = null;
        }
    }


    public File getFile() {
        return file;
    }

    public static final String NAME;

    public static final boolean IS_WINDOWS;

    static {
        NAME = System.getProperty("os.name").toLowerCase(Locale.ROOT);
        IS_WINDOWS = NAME.startsWith("windows");
    }

}
