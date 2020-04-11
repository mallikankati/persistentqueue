package com.persistentqueue.storage;

import com.persistentqueue.PersistentQueue;
import com.persistentqueue.storage.utils.PersistentUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Segment indexer manages metadata and read/write elements in the queue.
 * <p>
 * Default metadata segment size is fixed length 4KB but it can be customized to different page sizes.
 * <p>
 * Metadata segment consists of start and tail pointers of data index. These pointers are used to traverse
 * the queue. New element to the queue add at the end and read happens from the front of the queue.
 * start and tail pointers are monotonically increasing numbers
 * <p>
 * Index header consists of
 * <ul>
 * <li>data segment id </li>
 * <li>data offset in the segment</li>
 * <li>length of the data</li>
 * <li>insertion timestamp in millis</li>
 * </ul>
 * Which occupies 2^5 = 32 bytes.
 * </p>
 * <p>
 * Insertion algorithm as follows
 * <ul>
 * <li>
 * read tail pointer from metadata and derive data index using
 * data index = (tail pointer)/(total no of elements in a index)
 * total no of elements are fixed 2^18 = 256*1024
 * </li>
 * <li>
 * Derive the offset using
 * ((tail index)%total no of elements)*32 (each index header length)
 * </li>
 * </ul>
 * </p>
 *
 * @author Mallik Ankati
 */
public class SegmentIndexer implements Iterable<byte[]> {

    private static final Logger logger = LoggerFactory.getLogger(SegmentIndexer.class);

    private static int VERSION = 1;

    /**
     * Stores metadata in the segment
     */
    private StorageSegmentManager metadataStorageManager;

    /**
     * stores data index information
     */
    private StorageSegmentManager dataIndexStorageManager;

    /**
     * stores real data in the segment
     */
    private StorageSegmentManager dataStorageManager;

    /**
     * Extension of data segment file
     */
    private static final String DATA_SEGEMENT_EXT = ".dat";

    /**
     * Extension of metadata segment file
     */
    private static final String METADATA_SEGEMENT_EXT = ".meta";

    private static final String DATA_INDEX_SEGEMENT_EXT = ".index";

    /**
     * Metadata header 36 bytes in length which described in {@link PersistentQueue}
     */
    private static final int DEFAULT_METADATA_HEADER_LENGTH = 20;

    private MainHeader mainHeader;

    /**
     * Default metadata segment size
     */
    private static int DEFAULT_METADATA_SEG_SIZE = 1024;

    /**
     * Total no of data index elements can be stored in a segment
     */
    private static int TOTAL_NO_OF_DATA_INDEX_ELEMENTS = 256 * 1024;

    /**
     * Each index element is 32 bytes in length(datasegmentid + dataoffset + datalength +timestamp)
     */
    private static int DATA_INDEX_ELEMENT_HEADER = 32;

    /**
     * Each  data index segment size in bytes
     */
    private static int DATA_INDEX_SEG_SIZE = TOTAL_NO_OF_DATA_INDEX_ELEMENTS * DATA_INDEX_ELEMENT_HEADER;

    /**
     * Default data segment size.
     */
    private static int DEFAULT_DATA_SEG_SIZE = 32 * 1024 * 1024; //32MB

    /**
     * Storage path
     */
    private String path = null;

    private String name = null;

    /**
     * Data segment size provided by the user
     */
    private int initialSize;

    private static int DEFAULT_DATA_INDEX_CACHE_TTL = 2000;

    private static int DEFAULT_DATA_CACHE_TTL = 2000;

    /**
     * Monotonically increasing number for each segment
     */
    private AtomicInteger dataSegmentCounter = null;

    private ReadWriteLock metadataLock = new ReentrantReadWriteLock();

    private Lock metadataWriteLock = metadataLock.writeLock();

    private Lock metadataReadLock = metadataLock.readLock();

    private ReadWriteLock dataSegmentLock = new ReentrantReadWriteLock();

    private Lock dataReadLock = dataSegmentLock.readLock();

    private Lock dataWriteLock = dataSegmentLock.writeLock();

    private AtomicLong startPositionCounter;

    private int currentDataSegmentId;

    private int currentDataSegmentOffset;

    private AtomicLong tailPositionCounter;

    private int previousReadDataIndexSegmentId;

    private int previousReadDataSegmentId;

    private boolean printReadSegInfo = true;

    private AtomicInteger writeCounter = new AtomicInteger(0);

    private AtomicInteger readCounter = new AtomicInteger(0);

    /**
     * Segment types are FILE or MEMORYMAPPED
     */
    private StorageSegment.SegmentType segmentType = StorageSegment.SegmentType.MEMORYMAPPED;

    /**
     * @param segmentType
     */
    public void setSegmentType(StorageSegment.SegmentType segmentType) {
        this.segmentType = segmentType;
    }

    public SegmentIndexer() {
        this.mainHeader = new MainHeader();
        this.mainHeader.version = VERSION;
    }

    public void initialize(String path, String name, int initialSize) {
        if (path == null || path.trim().length() <= 0) {
            throw new RuntimeException("Invalid path");
        }
        if (name == null || name.trim().length() <= 0) {
            throw new RuntimeException("Invalid queue name");
        }
        if (initialSize <= 0) {
            initialSize = DEFAULT_DATA_SEG_SIZE;
        }
        this.path = path;
        this.name = name;
        this.initialSize = initialSize;
        //this.dataSegmentCounter = new AtomicInteger(100);
        this.startPositionCounter = new AtomicLong(0);
        this.tailPositionCounter = new AtomicLong(0);
        metadataStorageManager = new StorageSegmentManager(this.path, this.name, METADATA_SEGEMENT_EXT,
                0, DEFAULT_METADATA_SEG_SIZE);
        metadataStorageManager.init(this.segmentType, 20 * 1000);
        //read existing metadata
        readMetadataHeader();
        this.startPositionCounter.set(mainHeader.startPosition);
        this.tailPositionCounter.set(mainHeader.tailPosition);

        //calculate index and it's offset from .index file. Data insertion starts from index and offset.
        long index = this.tailPositionCounter.get();
        if (index != 0) {
            index--;
        }
        int currentIndexSegmentId = getDataIndexSegmentId(index);
        int currentIndexOffset = getDataIndexOffset(index);

        dataIndexStorageManager = new StorageSegmentManager(this.path, this.name, DATA_INDEX_SEGEMENT_EXT,
                0, DATA_INDEX_SEG_SIZE);
        dataIndexStorageManager.init(this.segmentType, DEFAULT_DATA_INDEX_CACHE_TTL);
        try {
            //From index read data segment and it's offset.
            StorageSegment dataIndexSegment = dataIndexStorageManager.acquireSegment(currentIndexSegmentId);
            ByteBuffer buffer = dataIndexSegment.getByteBuffer(currentIndexOffset);
            this.currentDataSegmentId = buffer.getInt();
            int dataItemOffset = buffer.getInt();
            int dataLength = buffer.getInt();
            this.currentDataSegmentOffset = dataItemOffset + dataLength;

            dataStorageManager = new StorageSegmentManager(this.path, this.name, DATA_SEGEMENT_EXT,
                    this.currentDataSegmentId, initialSize);
            dataStorageManager.init(this.segmentType, DEFAULT_DATA_CACHE_TTL);

            //this.currentDataSegmentId = dataSegmentCounter.get();
            //this.currentDataSegmentOffset = 0;
            //mainHeader.startPosition = startPositionCounter.get();
            //mainHeader.tailPosition = tailPositionCounter.get();
            writeMetadataHeader(mainHeader);
        } finally {
            dataIndexStorageManager.releaseSegment(currentIndexSegmentId);
        }
    }

    public boolean isEmpty() {
        return getTotalElements() == 0;
    }

    public int getTotalElements() {
        return (int) (this.tailPositionCounter.get() - this.startPositionCounter.get());
    }

    public long getStartIndex() {
        return this.startPositionCounter.get();
    }

    private int getDataIndexSegmentId(long position) {
        return (int) (position / TOTAL_NO_OF_DATA_INDEX_ELEMENTS);
    }

    private int getDataIndexOffset(long position) {
        return (int) ((position % TOTAL_NO_OF_DATA_INDEX_ELEMENTS) * DATA_INDEX_ELEMENT_HEADER);
    }

    /**
     * @param buff
     */
    public void writeToSegment(byte[] buff) {
        logger.debug("before write in writeSegment() " + mainHeader);
        if (buff == null || buff.length == 0) {
            throw new RuntimeException("Can not write empty buffer");
        }
       /* if (buff.length >= this.initialSize) {
            throw new RuntimeException("buffer size is greater than default file size. " +
                    "Either increase initial size or reduce buffer size");
        }*/
        int dataIndexSegmentId = 0;
        dataWriteLock.lock();
        try {
            //update data segment
            boolean isBigSizeBuffer = false;
            if (this.currentDataSegmentOffset + buff.length > this.initialSize) {
                if (buff.length > this.initialSize) {
                    isBigSizeBuffer = true;
                }
                this.currentDataSegmentId++;
                this.currentDataSegmentOffset = 0;
            }
            StorageSegment segment = null;
            if (!isBigSizeBuffer) {
                segment = dataStorageManager.acquireSegment(this.currentDataSegmentId);
            } else {
                segment = dataStorageManager.acquireSegment(this.currentDataSegmentId, buff.length);
            }
            segment.write(this.currentDataSegmentOffset, buff);

            //update data index segment
            dataIndexSegmentId = getDataIndexSegmentId(this.tailPositionCounter.get());
            int dataIndexItemOffset = getDataIndexOffset(this.tailPositionCounter.get());
            StorageSegment dataIndexSegment = dataIndexStorageManager.acquireSegment(dataIndexSegmentId);
            ByteBuffer buffer = dataIndexSegment.getByteBuffer(dataIndexItemOffset);
            buffer.putInt(this.currentDataSegmentId);
            buffer.putInt(this.currentDataSegmentOffset);
            buffer.putInt(buff.length);
            buffer.putLong(System.currentTimeMillis());

            this.currentDataSegmentOffset += buff.length;
            this.tailPositionCounter.incrementAndGet();

            //update metadata segment
            MainHeader mHeader = new MainHeader();
            mHeader.tailPosition = this.tailPositionCounter.get();
            mHeader.startPosition = mainHeader.startPosition;
            writeMetadataHeader(mHeader);
            writeCounter.incrementAndGet();
            if (writeCounter.get() % 1000000 == 0) {
                logger.debug("write segment:" + mainHeader);
                StringBuffer sb = new StringBuffer();
                sb.append("index :").append(dataIndexStorageManager);
                sb.append(", data :").append(dataStorageManager);
                logger.debug("Cache details [" + sb.toString() + "]");
                writeCounter = new AtomicInteger(0);
            }
        } finally {
            dataStorageManager.releaseSegment(this.currentDataSegmentId);
            dataIndexStorageManager.releaseSegment(dataIndexSegmentId);
            dataWriteLock.unlock();
        }

        logger.debug("after write in writeSegment() " + mainHeader);
    }

    public byte[] readFromSegment() {
        byte[] buff = null;
        dataReadLock.lock();
        try {
            buff = readElementFromSegment(mainHeader.startPosition, true);
            readCounter.incrementAndGet();
            if (readCounter.get() % 1000000 == 0) {
                logger.debug("read segment:" + mainHeader);
                StringBuffer sb = new StringBuffer();
                sb.append("index :").append(dataIndexStorageManager);
                sb.append(", data :").append(dataStorageManager);
                logger.debug("Cache details [" + sb.toString() + "]");
                readCounter = new AtomicInteger(0);
            }
        } finally {
            dataReadLock.unlock();
        }
        return buff;
    }

    public byte[] readElementFromSegment(long index, boolean markForDelete) {
        logger.debug("begin readSegment " + mainHeader + ", delete:" + markForDelete);
        /*if (mainHeader.startPosition == mainHeader.tailPosition) {
            return null;
        }*/
        int dataIndexSegmentId = 0;
        int dataSegmentId = 0;
        byte[] buff = null;
        try {
            dataIndexSegmentId = getDataIndexSegmentId(index);
            int dataIndexItemOffset = getDataIndexOffset(index);
            StorageSegment dataIndexSegment = dataIndexStorageManager.acquireSegment(dataIndexSegmentId);
            ByteBuffer buffer = dataIndexSegment.getByteBuffer(dataIndexItemOffset);
            dataSegmentId = buffer.getInt();
            int dataItemOffset = buffer.getInt();
            int dataLength = buffer.getInt();
            StorageSegment dataSegment = null;
            if (dataLength < this.initialSize) {
                dataSegment = dataStorageManager.acquireSegment(dataSegmentId);
            } else {
                dataSegment = dataStorageManager.acquireSegment(dataSegmentId, dataLength);
            }
            if (printReadSegInfo) {
                logger.debug("read is at [index:" + dataIndexSegmentId + ", ioffset:" + dataIndexItemOffset +
                        ", data:" + dataSegmentId + ", doffset:" + dataItemOffset + "]");
            }
            buff = dataSegment.read(dataItemOffset, 0, dataLength);
            if (markForDelete) {
                this.startPositionCounter.incrementAndGet();
                MainHeader mHeader = new MainHeader();
                mHeader.startPosition = this.startPositionCounter.get();
                mHeader.tailPosition = mainHeader.tailPosition;
                writeMetadataHeader(mHeader);
            }
        } finally {
            dataIndexStorageManager.releaseSegment(dataIndexSegmentId);
            dataStorageManager.releaseSegment(dataSegmentId);
            printReadSegInfo = false;
            if (markForDelete) {
                if (previousReadDataIndexSegmentId != dataIndexSegmentId) {
                    dataIndexStorageManager.markForDelete(previousReadDataIndexSegmentId);
                    logger.debug("data index seg to close:" + previousReadDataIndexSegmentId);
                    printReadSegInfo = true;
                }
                if (previousReadDataSegmentId != dataSegmentId) {
                    dataStorageManager.markForDelete(previousReadDataSegmentId);
                    logger.debug("data seg to close:" + previousReadDataSegmentId);
                    printReadSegInfo = true;
                }
                previousReadDataIndexSegmentId = dataIndexSegmentId;
                previousReadDataSegmentId = dataSegmentId;
            }
        }
        logger.debug("end readSegment " + mainHeader + ", delete:" + markForDelete);
        return buff;
    }

    public void remove() {
        remove(1);
    }

    public void remove(int elements) {
        if (elements == 0) {
            return;
        }
        //If remove all elements
        if (elements == getTotalElements()) {
            clear();
            return;
        }
        if (isEmpty()) {
            throw new NoSuchElementException();
        }
        for (int i = 0; i < elements; i++) {
            readFromSegment();
        }
    }

    public void clear() {
        close(true);
        initialize(this.path, this.name, this.initialSize);
    }

    public void close(boolean delete) {
        try {
            this.metadataStorageManager.close(delete);
            this.dataIndexStorageManager.close(delete);
            this.dataStorageManager.close(delete);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void writeMetadataHeader(MainHeader mHeader) {
        metadataWriteLock.lock();
        try {
            StorageSegment metadataSegment = metadataStorageManager.acquireSegment(0);
            mainHeader.startPosition = mHeader.startPosition;
            mainHeader.tailPosition = mHeader.tailPosition;
            metadataSegment.write(0, mainHeader.convertToBytes());
        } finally {
            metadataStorageManager.releaseSegment(0);
            metadataWriteLock.unlock();
        }
    }

    public void readMetadataHeader() {
        metadataReadLock.lock();
        try {
            StorageSegment metadataSegment = metadataStorageManager.acquireSegment(0);
            byte[] mainBuff = metadataSegment.read(0, 0, DEFAULT_METADATA_HEADER_LENGTH);
            mainHeader.readBytes(mainBuff);
        } finally {
            metadataStorageManager.releaseSegment(0);
            metadataReadLock.unlock();
        }
    }

    @Override
    public Iterator<byte[]> iterator() {
        return new ElementIterator();
    }

    /**
     * Iterator which traverse the elements
     */
    private final class ElementIterator implements Iterator<byte[]> {

        int currentIndex = 0;
        long currentPosition = mainHeader.startPosition;
        int currentTotalElements = getTotalElements();

        ElementIterator() {
        }

        private void checkForModification() {
            if (currentTotalElements != getTotalElements()) {
                throw new ConcurrentModificationException();
            }
        }

        @Override
        public boolean hasNext() {
            checkForModification();
            return currentIndex != currentTotalElements;
        }

        @Override
        public byte[] next() {
            checkForModification();
            if (isEmpty()) {
                throw new NoSuchElementException();
            }
            byte[] buff = null;
            dataReadLock.lock();
            try {
                buff = readElementFromSegment(currentPosition, false);
                this.currentIndex++;
                this.currentPosition++;
            } finally {
                dataReadLock.unlock();
            }
            return buff;
        }

        @Override
        public void remove() {
            SegmentIndexer.this.remove();
            currentIndex--;
            currentTotalElements = getTotalElements();
        }
    }

    public void printStats() {
    }

    private static class MainHeader {
        private int version = 1;
        private long startPosition;
        private long tailPosition;

        private byte[] convertToBytes() {
            byte[] buff = new byte[DEFAULT_METADATA_HEADER_LENGTH];
            PersistentUtil.writeInt(buff, 0, version);
            PersistentUtil.writeLong(buff, 4, startPosition);
            PersistentUtil.writeLong(buff, 12, tailPosition);

            return buff;
        }

        private void readBytes(byte[] buff) {
            version = PersistentUtil.readInt(buff, 0);
            startPosition = PersistentUtil.readLong(buff, 4);
            tailPosition = PersistentUtil.readLong(buff, 12);
        }

        public String toString() {
            StringBuffer sb = new StringBuffer();
            sb.append("mainheader[ start pos:").append(startPosition)
                    .append(", tail pos:").append(tailPosition)
                    .append("] ");
            return sb.toString();
        }
    }
}
