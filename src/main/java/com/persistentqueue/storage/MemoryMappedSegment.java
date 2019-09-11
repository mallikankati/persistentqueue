package com.persistentqueue.storage;

import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Segment backed by {@link java.nio.MappedByteBuffer} which should be faster
 * compare to regular files which internally copy buffers twice from kernel to user space
 */
public class MemoryMappedSegment extends AbstractStorageSegment {

    /**
     * MappedByteBuffer backed by this segment
     */
    private MappedByteBuffer mappedByteBuffer;

    private boolean closed =false;

    private boolean open = false;
    @Override
    public boolean isOpen() {
        return this.open;
    }

    @Override
    public boolean isClosed() {
        return this.closed;
    }

    @Override
    public void init(String path, String name, String ext, int segmentId, int initialLength) {
        RandomAccessFile raf = null;
        try {
            raf = initializeFile(path, name, ext, segmentId, initialLength);
            this.mappedByteBuffer = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, initialLength);
            //In case segment cached and reused same object
            this.closed = false;
            this.open = true;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (raf != null) {
                try {
                    raf.close();
                } catch (Exception e) {
                }
            }
        }
    }

    @Override
    public void read(long position, byte[] buff, int offset, int count) {
        this.mappedByteBuffer.position((int) position);
        this.mappedByteBuffer.get(buff, offset, count);
    }

    @Override
    public void write(long position, byte[] buff, int offset, int count) {
        this.mappedByteBuffer.position((int) position);
        this.mappedByteBuffer.put(buff, offset, count);
    }

    @Override
    public boolean isSpaceAvailable(int dataLength) {
        boolean status = false;
        if (this.closed){
            throw new RuntimeException("Segment closed");
        }
        if (this.mappedByteBuffer.remaining() > dataLength){
            status = true;
        }
        return status;
    }

    @Override
    public void seekToPosition(int position) {
        this.mappedByteBuffer.position(position);
    }

    @Override
    public int getCurrentPosition() {
        return this.mappedByteBuffer.position();
    }

    @Override
    public int remaining(int position) {
        this.mappedByteBuffer.position(position);
        return this.mappedByteBuffer.remaining();
    }

    @Override
    public void close(boolean delete) {
        if (isOpen()) {
            closeDirectBuffer(this.mappedByteBuffer);
            this.open = false;
        }
        if (delete){
            deleteFile();
        }
        this.closed = true;
    }

    private static void closeDirectBuffer(ByteBuffer cb) {
        if (cb == null || !cb.isDirect()) return;
        // we could use this type cast and call functions without reflection code,
        // but static import from sun.* package is risky for non-SUN virtual machine.
        //try { ((sun.nio.ch.DirectBuffer)cb).cleaner().clean(); } catch (Exception ex) { }

        // JavaSpecVer: 1.6, 1.7, 1.8, 9, 10
        boolean isOldJDK = System.getProperty("java.specification.version", "99").startsWith("1.");
        try {
            if (isOldJDK) {
                Method cleaner = cb.getClass().getMethod("cleaner");
                cleaner.setAccessible(true);
                Method clean = Class.forName("sun.misc.Cleaner").getMethod("clean");
                clean.setAccessible(true);
                clean.invoke(cleaner.invoke(cb));
            } else {
                Class unsafeClass;
                try {
                    unsafeClass = Class.forName("sun.misc.Unsafe");
                } catch (Exception ex) {
                    // jdk.internal.misc.Unsafe doesn't yet have an invokeCleaner() method,
                    // but that method should be added if sun.misc.Unsafe is removed.
                    unsafeClass = Class.forName("jdk.internal.misc.Unsafe");
                }
                Method clean = unsafeClass.getMethod("invokeCleaner", ByteBuffer.class);
                clean.setAccessible(true);
                Field theUnsafeField = unsafeClass.getDeclaredField("theUnsafe");
                theUnsafeField.setAccessible(true);
                Object theUnsafe = theUnsafeField.get(null);
                clean.invoke(theUnsafe, cb);
            }
        } catch (Exception ex) {
        }
        cb = null;
    }
}
