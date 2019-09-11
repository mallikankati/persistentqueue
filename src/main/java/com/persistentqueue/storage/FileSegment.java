package com.persistentqueue.storage;

import java.io.RandomAccessFile;


/**
 * Storage segment implementation backed by file
 */
public class FileSegment extends AbstractStorageSegment {

    /**
     * File backed by this segment
     */
    private RandomAccessFile raf;

    private boolean closed = false;

    private boolean open = false;

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
     * @param buff
     * @param offset
     * @param count
     */
    public void read(long position, byte[] buff, int offset, int count) {
        if (closed) {
            throw new RuntimeException("segment closed before reading");
        }
        try {
            if (position + count <= initialLength) {
                raf.seek(position);
                raf.readFully(buff, offset, count);
            } else {
                throw new RuntimeException("Exceeded initial file length to read");
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param position
     * @param buff
     * @param offset
     * @param count
     */
    public void write(long position, byte[] buff, int offset, int count) {
        try {
            if (raf != null && position + count <= initialLength) {
                raf.seek(position);
                raf.write(buff, offset, count);
            } else {
                throw new RuntimeException("Exceeded initial file length to write");
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
        }catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    @Override
    public int getCurrentPosition() {
        int pointer = 0;
        try {
            pointer = (int)raf.getFilePointer();
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

    /**
     * Closes open file
     *
     * @param delete If this flag is true, underneath file get deleted from the disk
     */
    @Override
    public void close(boolean delete) {
        try {
            if (isOpen()) {
                if (raf != null) {
                    raf.close();
                    closed = true;
                }
                open = false;
            }
            if (delete) {
                deleteFile();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
