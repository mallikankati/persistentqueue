package com.persistentqueue;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.util.Pool;
import com.persistentqueue.storage.SegmentIndexer;
import com.persistentqueue.storage.StorageSegment;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.util.AbstractQueue;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;

/**
 * A reliable, efficient, file-based, FIFO queue. Add and remove operations take O(1) but underneath it can have multiple memory read/writes.
 *
 * It combines the functionality from {@link https://github.com/square/tape/blob/master/tape/src/main/java/com/squareup/tape2/QueueFile.java}
 * and {@link https://github.com/VoltDB/voltdb/blob/master/src/frontend/org/voltdb/utils/PersistentBinaryDeque.java}
 *
 * Off heap implementation of queue to survive sudden burst load to run with low memory profile. Data is backed by {@link java.nio.MappedByteBuffer}
 * for efficient writing. There is another implementation with {@link java.io.RandomAccessFile}
 *
 * It can change between these two implementaion using {@link StorageSegment.SegmentType}
 *
 * When Queue initialized it creates two files which contains metadata and data.
 * Add element to the queue, will create a data file and which grows max capacity and creates new files to store data.
 * Data removed from queue, will delete the files once every element deleted from the file.
 *
 * Metadata file contains two headers
 *    MetadataHeader (total 28 bytes)
 *
 *        4 bytes      Version indicator
 *        8 bytes      Element count
 *        4 bytes      Head pointer segment
 *        4 bytes      Head pointer position
 *        4 bytes      Tail pointer segment
 *        4 bytes      Tail pointer position
 *
 *    SegmentHeader
 *        4 bytes      Total segments
 *        ....         Array of segments each 4 bytes in length
 *
 * Data file contains multiple data headers
 *    DataHeader
 *       4 bytes    Data length
 *       ....       data content
 *
 * It uses {@link Kryo} serializer/deserialize the objects
 *
 */
public class PersistentQueue<E> extends AbstractQueue<E> implements Closeable, Iterable<E> {

    /**
     * This is to track generic type
     */
    protected Class<E> genericType;

    protected Class<?>[] extraGenericType;
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

    /**
     * Bytes needs to be compressed at serialize/deserialize
     */
    protected boolean compress = true;

    public PersistentQueue(String path, String name, int dataSegmentSize){
        this.path = path;
        this.name = name;
        this.dataSegmentSize = dataSegmentSize;
    }

    public void init(Class<E> typeClass, StorageSegment.SegmentType segmentType, Class<?> ... extraGenericType){
        this.genericType = typeClass;
        segmentIndexer = new SegmentIndexer();
        if (segmentType != null){
            segmentIndexer.setSegmentType(segmentType);
        }
        this.extraGenericType = extraGenericType;
        segmentIndexer.initialize(this.path, this.name, this.dataSegmentSize);
    }

    /*
      // Pool constructor arguments: thread safe, soft references, maximum capacity
     */
    protected Pool<Kryo> serializePool = new Pool<Kryo>(true, false,
            Runtime.getRuntime().availableProcessors()) {
        @Override
        protected Kryo create() {
            return createKryo();
        }
    };

    protected Pool<Kryo> deserializePool = new Pool<Kryo>(true, false,
            Runtime.getRuntime().availableProcessors()) {
        @Override
        protected Kryo create() {
            return createKryo();
        }
    };

    private Kryo createKryo(){
        Kryo kryo = new Kryo();
        kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
        kryo.register(ArrayList.class);
        kryo.register(HashMap.class);
        kryo.register(LinkedHashMap.class);
        kryo.register(genericType);
        if (extraGenericType != null && extraGenericType.length > 0){
            for (Class<?> clzz : extraGenericType){
                kryo.register(clzz);
            }
        }
        return kryo;
    }

   @Override
    public void close() {
       try {
           segmentIndexer.close(cleanStorageOnRestart);
       }catch (Exception e){
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

    protected byte[] serialize(E e){
       Kryo kryo = serializePool.obtain();
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
       }
       return buff;
    }

    protected E deserialize(byte[] buff){
       E e = null;
       if (buff != null && buff.length > 0) {
           Kryo kryo = deserializePool.obtain();
           try {
               Input input = new Input(buff);
               e = (E) kryo.readObject(input, genericType);
           } finally {
               deserializePool.free(kryo);
           }
       }
        return e;
    }

    protected final class PersistentQueueIterator implements Iterator<E>{

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
