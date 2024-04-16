package com.bin.delay.file.store;

import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

public class BufferedRaf implements Closeable {

    static final int BUFFER_SIZE = 16384;
    private final RandomAccessFile raf;
    private final byte[] buf;
    private final ByteBuffer auxBuf;
    private BufferedRaf.Mode mode;
    private long fileLength;
    private long bufBaseFileOffset;
    private int bufLimit;
    private long filePointer;

    BufferedRaf(RandomAccessFile raf) throws IOException {
        this.buf = new byte[16384];
        this.auxBuf = ByteBuffer.wrap(new byte[8]);
        this.raf = raf;
        this.fileLength = raf.length();
    }


    public long length() {
        return fileLength;
    }

    public void setLength(long newLength) throws IOException {
        this.flush();
        this.raf.setLength(newLength);
        this.fileLength = newLength;
    }

    public long filePointer() {
        return this.filePointer;
    }

    public void flush() throws IOException {
        if (this.mode == BufferedRaf.Mode.WRITING) {
            this.flushBuffer();
        }

    }

    public void force() throws IOException {
        this.flush();
        this.raf.getFD().sync();
    }

    public void close() throws IOException {
        this.flush();
        this.raf.close();
    }

    public void seek(long offset) {
        this.filePointer = offset;
    }

    public void skipBytes(long length) {
        this.seek(this.filePointer + length);
    }


    public void read(byte[] buffer, int start, int count) throws IOException {
        if (start + count > buffer.length) {
            throw new IllegalArgumentException("Requested to read more than fits into the buffer");
        } else {
            this.ensure(BufferedRaf.Mode.READING);

            if (this.filePointer + (long) count > length()) {
                throw new IllegalArgumentException("Requested to read beyond the end of file");
            } else {
                this.fillBufferIfNeeded();
                int bytesRead = 0;

                while (true) {
                    int srcOffset = (int) (this.filePointer - this.bufBaseFileOffset);
                    int bytesToCopy = Math.min(count - bytesRead, this.bufLimit - srcOffset);
                    System.arraycopy(this.buf, srcOffset, buffer, start + bytesRead, bytesToCopy);
                    bytesRead += bytesToCopy;
                    this.filePointer += (long) bytesToCopy;
                    if (bytesRead == count) {
                        return;
                    }

                    this.fillBuffer();
                }
            }
        }
    }

    public void copyTo(BufferedRaf dest) throws IOException {
        this.ensure(BufferedRaf.Mode.READING);
        this.fillBufferIfNeeded();

        while (this.filePointer != this.fileLength) {
            int srcOffset = (int) (this.filePointer - this.bufBaseFileOffset);
            int bytesToCopy = Math.min((int) (this.fileLength - this.filePointer), this.bufLimit - srcOffset);
            dest.write(this.buf, srcOffset, bytesToCopy);
            this.filePointer += (long) bytesToCopy;
            this.fillBuffer();
        }

    }

    private void fillBufferIfNeeded() throws IOException {
        long bufOffset = this.filePointer - this.bufBaseFileOffset;
        if (bufOffset < 0L || bufOffset > (long) this.bufLimit) {
            this.fillBuffer();
        }

    }

    public void write(byte[] bytes, int start, int count) throws IOException {
        this.ensure(BufferedRaf.Mode.WRITING);
        if (this.filePointer != this.bufBaseFileOffset + (long) this.bufLimit) {
            this.flushBuffer();
            this.bufBaseFileOffset = this.filePointer;
        }

        if (this.filePointer + (long) count > this.fileLength) {
            this.fileLength = this.filePointer + (long) count;
        }

        int doneCount = 0;

        do {
            int copySize = Math.min(count - doneCount, this.buf.length - this.bufLimit);
            System.arraycopy(bytes, start + doneCount, this.buf, this.bufLimit, copySize);
            this.bufLimit += copySize;
            if (this.bufLimit == this.buf.length) {
                this.flushBuffer();
            }

            doneCount += copySize;
        } while (doneCount != count);

        this.filePointer += (long) count;
    }

    public int readByte() throws IOException {
        this.read(this.auxBuf.array(), 0, 1);
        return this.auxBuf.get(0) & 255;
    }

    public int readInt() throws IOException {
        this.read(this.auxBuf.array(), 0, 4);
        return this.auxBuf.getInt(0);
    }

    public long readLong() throws IOException {
        this.read(this.auxBuf.array(), 0, 8);
        return this.auxBuf.getLong(0);
    }

    public void writeByte(int b) throws IOException {
        this.auxBuf.put(0, (byte) b);
        this.write(this.auxBuf.array(), 0, 1);
    }

    public void writeInt(int value) throws IOException {
        this.auxBuf.putInt(0, value);
        this.write(this.auxBuf.array(), 0, 4);
    }

    public void writeLong(long value) throws IOException {
        this.auxBuf.putLong(0, value);
        this.write(this.auxBuf.array(), 0, 8);
    }



    public long available() {
        return this.fileLength - this.filePointer;
    }

    private void ensure(BufferedRaf.Mode requestedMode) throws IOException {
        if (this.mode != requestedMode) {
            if (this.mode == BufferedRaf.Mode.WRITING) {
                this.flushBuffer();
            } else if (this.mode == BufferedRaf.Mode.READING) {
                this.bufLimit = 0;
            }

            this.mode = requestedMode;
        }
    }

    private void flushBuffer() throws IOException {
        this.raf.seek(this.bufBaseFileOffset);
        this.raf.write(this.buf, 0, this.bufLimit);
        this.bufBaseFileOffset += (long) this.bufLimit;
        this.bufLimit = 0;
    }

    private void fillBuffer() throws IOException {
        this.raf.seek(this.filePointer);
        this.bufLimit = Math.min(this.buf.length, (int) (this.fileLength - this.filePointer));
        int offset = 0;

        do {
            int readCount = this.raf.read(this.buf, offset, this.bufLimit - offset);
            if (readCount == -1) {
                throw new IOException("Unexpected EOF reached at " + this.raf.getFilePointer());
            }

            offset += readCount;
        } while (offset != this.bufLimit);

        this.bufBaseFileOffset = this.filePointer;
    }


    private static enum Mode {
        READING,
        WRITING;

        private Mode() {
        }
    }

}
