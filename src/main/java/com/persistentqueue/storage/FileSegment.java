package com.persistentqueue.storage;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;


/**
 * Storage segment implementation backed by file
 */
public class FileSegment extends AbstractStorageSegment {

    /**
     * File backed by this segment
     */
    private RandomAccessFile raf;

    public FileSegment() {
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public boolean isClosed() {
        return this.closed;
    }

    @Override
    public void init(String path, String name, String ext, int segmentId, int initialLength) {
        FileSegment fileSegment = null;
        try {
            raf = initializeFile(path, name, ext, segmentId, initialLength);
            //In case segment cached and close and create again on same instance
            closed = false;
            open = true;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Reading bytes from file
     *
     * @param position
     */
    @Override
    public byte[] read(long position, int offset, int count) {
        byte[] buff = null;
        if (closed) {
            throw new RuntimeException("segment closed before reading");
        }
        try {
            raf.seek(position);
            buff = new byte[count];
            raf.readFully(buff, offset, count);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return buff;
    }

    /**
     * @param position
     * @param buff
     */
    public void write(long position, byte[] buff) {
        try {
            if (raf != null && position + buff.length <= initialLength) {
                raf.seek(position);
                raf.write(buff, 0, buff.length);
            } else {
                throw new RuntimeException("Exceeded initial file length to write");
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ByteBuffer getByteBuffer(long position) {
        ByteBuffer byteBuffer = null;
        return byteBuffer;
    }

    @Override
    public boolean isSpaceAvailable(int dataLength) {
        boolean status = false;
        try {
            if (closed) {
                throw new RuntimeException("segment closed");
            }
            if (raf.getFilePointer() + dataLength <= initialLength) {
                status = true;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return status;
    }

    @Override
    public void seekToPosition(int position) {
        try {
            raf.seek(position);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int getCurrentPosition() {
        int pointer = 0;
        try {
            pointer = (int) raf.getFilePointer();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return pointer;
    }

    @Override
    public int remaining(int position) {
        int remaining = 0;
        try {
            remaining = initialLength - position;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return remaining;
    }

    @Override
    public boolean isDirty() {
        return false;
    }

    @Override
    public void setDirty(boolean dirty) {

    }

    @Override
    public boolean isDelete() {
        return this.delete;
    }

    @Override
    public void setDelete(boolean delete) {
        this.delete = delete;
    }

    /**
     * Closes open file
     */
    @Override
    public void close() throws IOException {
        try {
            if (isOpen()) {
                if (raf != null) {
                    raf.close();
                    closed = true;
                }
                open = false;
            }
            if (isDelete()) {
                deleteFile();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
