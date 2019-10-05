package com.persistentqueue.storage;

import java.io.IOException;
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
     * ThreadLocal MappedByteBuffer backed by this segment
     */
    private ThreadLocalBuffer threadLocalBuffer;

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
             MappedByteBuffer mappedByteBuffer = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, initialLength);
             this.threadLocalBuffer = new ThreadLocalBuffer(mappedByteBuffer);
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
    public byte[] read(long position, int offset, int count) {
        ByteBuffer byteBuffer = this.threadLocalBuffer.get();
        byte[] buff = null;
        byteBuffer.position((int) position);
        buff = new byte[count];
        byteBuffer.get(buff, 0, count);
        return buff;
    }

    @Override
    public void write(long position, byte[] buff) {
        ByteBuffer byteBuffer = this.threadLocalBuffer.get();
        byteBuffer.position((int) position);
        byteBuffer.put(buff);
    }

    @Override
    public ByteBuffer getByteBuffer(long position)
    {
        ByteBuffer byteBuffer = this.threadLocalBuffer.get();
        byteBuffer.position((int) position);
        return byteBuffer;
    }

    @Override
    public boolean isSpaceAvailable(int dataLength) {
        boolean status = false;
        if (this.closed) {
            throw new RuntimeException("Segment closed");
        }
        ByteBuffer byteBuffer = this.threadLocalBuffer.get();
        if (byteBuffer.remaining() > dataLength) {
            status = true;
        }
        return status;
    }

    @Override
    public void seekToPosition(int position) {
        ByteBuffer byteBuffer = this.threadLocalBuffer.get();
        byteBuffer.position(position);
    }

    @Override
    public int getCurrentPosition() {
        ByteBuffer byteBuffer = this.threadLocalBuffer.get();
        return byteBuffer.position();
    }

    @Override
    public int remaining(int position) {
        int remaining = 0;
        ByteBuffer byteBuffer = this.threadLocalBuffer.get();
        byteBuffer.position(position);
        remaining = byteBuffer.remaining();

        return remaining;
    }

    @Override
    public boolean isDelete() {
        return this.delete;
    }

    @Override
    public void setDelete(boolean delete) {
        this.delete = delete;
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
        } catch (Exception ignore) {
        }
        cb = null;
    }

    @Override
    public void close() throws IOException {
        synchronized (this) {
            if (isOpen()) {
                this.threadLocalBuffer.close();
                this.open = false;
                this.threadLocalBuffer = null;
            }
            if (isDelete()) {
                deleteFile();
            }
            this.closed = true;
        }
    }

    private class ThreadLocalBuffer extends ThreadLocal<ByteBuffer>{
        private ByteBuffer byteBuffer;

        public ByteBuffer getBuffer(){
            return this.byteBuffer;
        }

        ThreadLocalBuffer(ByteBuffer byteBuffer){
            this.byteBuffer = byteBuffer;
        }

        @Override
        protected ByteBuffer initialValue() {
            return this.byteBuffer.duplicate();
        }

        private void close(){
            MappedByteBuffer buffer = (MappedByteBuffer)byteBuffer ;
            buffer.force();
            closeDirectBuffer(byteBuffer);
            byteBuffer = null;
        }
    }
}
