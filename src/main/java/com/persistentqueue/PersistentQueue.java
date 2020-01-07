package com.persistentqueue;

import com.persistentqueue.storage.SegmentIndexer;
import com.persistentqueue.storage.StorageSegment;

import java.io.Closeable;
import java.util.AbstractQueue;
import java.util.Iterator;

/**
 * A reliable, efficient, file-based, FIFO queue. Add and remove operations take O(1) but underneath it can have multiple memory read/writes.
 * <p>
 * It combines the functionality from {@link https://github.com/square/tape/blob/master/tape/src/main/java/com/squareup/tape2/QueueFile.java}
 * and {@link https://github.com/VoltDB/voltdb/blob/master/src/frontend/org/voltdb/utils/PersistentBinaryDeque.java}
 * <p>
 * Off heap implementation of queue to survive sudden burst load to run with low memory profile. Data is backed by {@link java.nio.MappedByteBuffer}
 * for efficient writing. There is another implementation with {@link java.io.RandomAccessFile}
 * <p>
 * It can change between these two implementaion using {@link StorageSegment.SegmentType}
 * <p>
 * When Queue initialized it creates two files which contains metadata and data.
 * Add element to the queue, will create a data file and which grows max capacity and creates new files to store data.
 * Data removed from queue, will delete the files once every element deleted from the file.
 * <p>
 * Metadata file contains two headers
 * MetadataHeader (total 28 bytes)
 * <p>
 * 4 bytes      Version indicator
 * 8 bytes      Element count
 * 4 bytes      Head pointer segment
 * 4 bytes      Head pointer position
 * 4 bytes      Tail pointer segment
 * 4 bytes      Tail pointer position
 * <p>
 * SegmentHeader
 * 4 bytes      Total segments
 * ....         Array of segments each 4 bytes in length
 * <p>
 * Data file contains multiple data headers
 * DataHeader
 * 4 bytes    Data length
 * ....       data content
 * <p>
 * It uses {@link PersistentQueueSerializer} to serializer/deserialize the objects
 */
public class PersistentQueue<E> extends AbstractQueue<E> implements Closeable, Iterable<E> {

    /**
     * This is to track generic type
     */
    //protected Class<E> genericType;

    /**
     * Directory to store files
     */
    protected String path;

    /**
     * Name of the queue
     */
    protected String name;

    /**
     * initial dataSegmentSize;
     */
    protected int dataSegmentSize;

    protected SegmentIndexer segmentIndexer;

    protected boolean cleanStorageOnRestart = true;

    protected PersistentQueueSerializer<E> serializer = new PersistentQueueSerializer<E>() {
    };

    /**
     * Bytes needs to be compressed at serialize/deserialize
     */
    protected boolean compress = true;

    public PersistentQueue(String path, String name, int dataSegmentSize, boolean cleanStorageOnRestart) {
        this.path = path;
        this.name = name;
        this.dataSegmentSize = dataSegmentSize;
        this.cleanStorageOnRestart = cleanStorageOnRestart;
    }

    public void init(StorageSegment.SegmentType segmentType,
                     PersistentQueueSerializer<E> serializer) {
        //this.genericType = typeClass;
        segmentIndexer = new SegmentIndexer();
        if (segmentType != null) {
            segmentIndexer.setSegmentType(segmentType);
        }
        if (serializer != null) {
            this.serializer = serializer;
        }
        segmentIndexer.initialize(this.path, this.name, this.dataSegmentSize);
    }

    @Override
    public void close() {
        try {
            segmentIndexer.close(cleanStorageOnRestart);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public Iterator<E> iterator() {
        return new PersistentQueueIterator();
    }

    @Override
    public int size() {
        return segmentIndexer.getTotalElements();
    }

    @Override
    public boolean offer(E e) {
        byte[] buff = serialize(e);
        segmentIndexer.writeToSegment(buff);
        return true;
    }

    @Override
    public E poll() {
        byte[] buff = segmentIndexer.readFromSegment();
        E e = deserialize(buff);
        return e;
    }

    @Override
    public E peek() {
        long currentStartIndex = segmentIndexer.getStartIndex();
        byte[] buff = segmentIndexer.readElementFromSegment(currentStartIndex, false);
        E e = deserialize(buff);
        return e;
    }

    public void clear() {
        segmentIndexer.clear();
    }

    protected byte[] serialize(E e) {
       /*Kryo kryo = serializePool.obtain();
       byte[] buff = null;
       try {
           ByteArrayOutputStream baos  = new ByteArrayOutputStream();
           Output output = new Output(baos);
           kryo.writeObject(output, e);
           output.flush();
           output.close();
           buff = baos.toByteArray();
       }finally {
           serializePool.free(kryo);
       }*/
        byte[] buff = serializer.serialize(e);
        return buff;
    }

    protected E deserialize(byte[] buff) {
        E e = null;
       /*if (buff != null && buff.length > 0) {
           Kryo kryo = deserializePool.obtain();
           try {
               Input input = new Input(buff);
               e = (E) kryo.readObject(input, genericType);
           } finally {
               deserializePool.free(kryo);
           }
       }*/
        e = serializer.deserialize(buff);
        return e;
    }

    protected final class PersistentQueueIterator implements Iterator<E> {

        Iterator<byte[]> it = segmentIndexer.iterator();

        @Override
        public boolean hasNext() {
            return it.hasNext();
        }

        @Override
        public E next() {
            byte[] buff = it.next();
            return deserialize(buff);
        }

        @Override
        public void remove() {
            throw new RuntimeException("Not implemented");
        }
    }
}
