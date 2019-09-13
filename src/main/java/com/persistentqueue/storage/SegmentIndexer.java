package com.persistentqueue.storage;

import com.persistentqueue.PersistentQueue;
import com.persistentqueue.storage.utils.PersistentUtil;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * Segment indexer manages metadata and read/write elements in the queue.
 * <p>
 * Default metadata segment size is fixed length 4096 bytes but it can be increased by multiple page sizes.
 * <p>
 * Metadata segment consists of main header + segment header
 * <p>
 * Main header occupies 28 bytes, segment header occupies remaining 4068 bytes
 * Segment header contains
 * 4 bytes  total segments count
 * 4064 bytes array of segment id's
 * <p>
 * Each segment is 4 bytes, that means default max segment's can be limited to 1016 files.
 * <p>
 * Note: If No of elements inserted crossing this limit
 * 1)Increase initial data file size or
 * 2)Increase default metadata segment size
 */
public class SegmentIndexer implements Iterable<byte[]> {

    private static final Logger logger = Logger.getLogger(SegmentIndexer.class.getName());

    private static int VERSION = 1;

    /**
     * Stores metadata in the segment
     */
    private StorageSegment metadataSegment;

    /**
     * start segment
     */
    private StorageSegment startSegment;

    /**
     * If write heavy applications, tail pointer can be ahead of start pointer.
     * In that scenario tail segment could be different from start segment.
     */
    private StorageSegment tailSegment;

    /**
     * Metadata header 28 bytes in length which described in {@link PersistentQueue}
     */
    private static final int DEFAULT_METADATA_HEADER_LENGTH = 28;

    /**
     * metadata buffer 28 bytes in length
     */
    //private byte[] metadaBuff = new byte[DEFAULT_METADATA_HEADER_LENGTH];

    private MainHeader mainHeader;

    private SegmentHeader segmentHeader;

    /**
     * Default metadata segment size
     */
    private static int DEFAULT_METADATA_SEG_SIZE = 1024*1024;

    /**
     * Default data segment size.
     */
    private static int DEFAULT_DATA_SEG_SIZE = 4096;

    /**
     * Segment types are FILE or MEMORY, default it to FILE
     */
    private StorageSegment.SegmentType segmentType = StorageSegment.SegmentType.MEMORYMAPPED;

    /**
     * Extension of data segment file
     */
    private static final String DATA_SEGEMENT_EXT = ".dat";

    /**
     * Extension of metadata segment file
     */
    private static final String METADATA_SEGEMENT_EXT = ".meta";

    /**
     * Storage path
     */
    private String path = null;

    private String name = null;

    private int initialSize;

    /**
     * Monotonically increasing number for each segment
     */
    private AtomicInteger segmentCounter = new AtomicInteger(100);

    /**
     * To protect from multiple threads changing
     */
    private AtomicInteger count;

    public void setSegmentType(StorageSegment.SegmentType segmentType) {
        this.segmentType = segmentType;
    }

    public SegmentIndexer() {
        this.mainHeader = new MainHeader();
        this.mainHeader.version = VERSION;
        this.segmentHeader = new SegmentHeader();
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
        this.count = new AtomicInteger();
        metadataSegment = createAndOpenSegment(path, name, METADATA_SEGEMENT_EXT, -1,
                DEFAULT_METADATA_SEG_SIZE);
        startSegment = tailSegment = createAndOpenSegment(path, name, DATA_SEGEMENT_EXT,
                segmentCounter.getAndIncrement(), initialSize);
        mainHeader.totalElements = count.get();
        mainHeader.startSegmentId = startSegment.getSegmentId();
        mainHeader.startPosition = 0;
        mainHeader.tailSegmentId = 0;
        mainHeader.tailPosition = 0;
        segmentHeader.segments.clear();
        segmentHeader.length = 0;
        segmentHeader.addSegment(startSegment.getSegmentId());
        writeMetadataHeader();
    }

    /**
     * If the segment is already open it will return it otherwise it will open a storage segment for read/write
     *
     * @param segmentId
     * @return
     */
    public StorageSegment getSegment(int segmentId) {
        if (mainHeader.startSegmentId == segmentId) {
            return startSegment;
        }
        if (mainHeader.tailSegmentId == segmentId) {
            return tailSegment;
        }
        StorageSegment segment = createAndOpenSegment(this.path, this.name, DATA_SEGEMENT_EXT,
                segmentId, initialSize);
        return segment;
    }

    public int getNextSegmentId(int segmentId) {
        int tempSegmentId = -1;
        for (int i = 0; i < segmentHeader.segments.size(); i++) {
            if (segmentHeader.segments.get(i) == segmentId) {
                tempSegmentId = segmentHeader.segments.get(i + 1);
                break;
            }
        }
        return tempSegmentId;
    }

    public StorageSegment getNextSegment(int segmentId) {
        StorageSegment tempSegment = null;
        int tempSegmentId = getNextSegmentId(segmentId);
        if (tempSegmentId != -1) {
            if (tempSegmentId == this.tailSegment.getSegmentId()) {
                tempSegment = this.tailSegment;
            } else if (tempSegmentId == this.startSegment.getSegmentId()) {
                tempSegment = this.startSegment;
            } else {
                tempSegment = createAndOpenSegment(this.path, this.name, DATA_SEGEMENT_EXT,
                        tempSegmentId, this.initialSize);
            }
        }
        return tempSegment;
    }

    public boolean isEmpty() {
        return getTotalElements() == 0;
    }

    public int getTotalElements() {
        return this.count.get();
    }

    public int getTotalSegments() {
        return segmentHeader.segments.size();
    }

    /**
     * @param buff
     * @param offset
     * @param count
     */
    public void writeToSegment(byte[] buff, int offset, int count) {
        if (buff == null || buff.length == 0) {
            throw new RuntimeException("Can not write empty buffer");
        }
        int position = mainHeader.tailPosition;
        StorageSegment segment = tailSegment;
        int remain = segment.remaining(position);
        Element element = null;
        List<StorageSegment> closeSegments = new ArrayList<>();
        if (remain >= Element.ELEMENT_HEADER_LENGTH) {
            writeDataLength(segment, position, count);
            position += Element.ELEMENT_HEADER_LENGTH;
            element = writeElementToSegment(segment, position, buff, offset, count, closeSegments);
        } else {
            closeSegments.add(segment);
            segment = createAndOpenSegment(this.path, this.name, DATA_SEGEMENT_EXT, segmentCounter.getAndIncrement(),
                    this.initialSize);
            position = 0;
            writeDataLength(segment, position, count);
            position += Element.ELEMENT_HEADER_LENGTH;
            element = writeElementToSegment(segment, position, buff, offset, count, closeSegments);
        }
        mainHeader.tailSegmentId = element.segment.getSegmentId();
        if (tailSegment.getSegmentId() != mainHeader.tailSegmentId) {
            //tailSegment.close(false);
            tailSegment = element.segment;
        }
        mainHeader.tailPosition = element.position;
        mainHeader.totalElements = this.count.incrementAndGet();
        writeMetadataHeader();
        for (StorageSegment tempSegment : closeSegments) {
            int tempSegmentId = tempSegment.getSegmentId();
            if (tempSegmentId != mainHeader.startSegmentId
                    && tempSegmentId != mainHeader.tailSegmentId) {
                tempSegment.close(false);
                logger.finest("start segid:" + mainHeader.startSegmentId + ", tail segid:" + mainHeader.tailSegmentId
                        + ", close segid:" + tempSegmentId+", delete:" + false);
            }
        }
    }

    private void writeDataLength(StorageSegment segment, int position, int dataLength) {
        byte[] buff = new byte[Element.ELEMENT_HEADER_LENGTH];
        //write Element length as 4 bytes to data segment
        PersistentUtil.writeInt(buff, 0, dataLength);
        segment.write(position, buff, 0, Element.ELEMENT_HEADER_LENGTH);
    }

    private Element writeElementToSegment(StorageSegment segment, int position, byte[] buff, int offset, int count,
                                          List<StorageSegment> closeSegments) {
        int remain = segment.remaining(position);
        int segmentId = segment.getSegmentId();
        if (remain >= count) {
            segment.write(position, buff, offset, count);
            position += count;
        } else {
            segment.write(position, buff, offset, remain);
            closeSegments.add(segment);
            int currentOffset = offset + remain;
            int currentRemainBytes = count - remain;
            while (currentRemainBytes > 0) {
                segment = createAndOpenSegment(this.path, this.name, DATA_SEGEMENT_EXT,
                        segmentCounter.getAndIncrement(), this.initialSize);
                position = 0;
                remain = segment.remaining(position);
                if (remain >= currentRemainBytes) {
                    segment.write(0, buff, currentOffset, currentRemainBytes);
                    position = currentRemainBytes;
                    currentRemainBytes = 0;
                } else {
                    segment.write(0, buff, currentOffset, remain);
                    currentOffset += remain;
                    currentRemainBytes -= remain;
                    segmentHeader.addSegment(segment.getSegmentId());
                    closeSegments.add(segment);
                }
            }
        }
        segmentHeader.addSegment(segment.getSegmentId());
        Element element = new Element(position, count);
        element.segment = segment;
        return element;
    }

    public byte[] readFromSegment(boolean movePositionAfterRead) {
        if (mainHeader.startSegmentId == mainHeader.tailSegmentId &&
                mainHeader.startPosition == mainHeader.tailPosition) {
            return null;
        }
        int position = mainHeader.startPosition;
        StorageSegment segment = startSegment;
        List<StorageSegment> closeSegmentList = new ArrayList<>();
        Element element = readElementFromSegment(segment, position, closeSegmentList, movePositionAfterRead);
        if (movePositionAfterRead) {
            mainHeader.startSegmentId = element.segment.getSegmentId();
            if (startSegment.getSegmentId() != mainHeader.startSegmentId) {
                //startSegment.close(false);
                startSegment = element.segment;
            }
            mainHeader.startPosition = element.position;
            mainHeader.totalElements = this.count.decrementAndGet();
            for (StorageSegment tempSegment : closeSegmentList) {
                int tempSegmentId = tempSegment.getSegmentId();
                if (tempSegmentId != mainHeader.startSegmentId) {
                    segmentHeader.removeSegment(tempSegmentId);
                    tempSegment.close(true);
                    logger.finest("start segid:" + mainHeader.startSegmentId + ", tail segid:" + mainHeader.tailSegmentId
                            + ", close segid:" + tempSegmentId+", delete:" + true);
                }
            }
            writeMetadataHeader();
        } else {
            for (StorageSegment tempSegment : closeSegmentList) {
                int tempSegmentId = tempSegment.getSegmentId();
                if (tempSegmentId != mainHeader.startSegmentId && tempSegmentId != mainHeader.tailSegmentId) {
                    //segmentHeader.removeSegment(tempSegmentId);
                    tempSegment.close(false);
                    logger.finest("start segid:" + mainHeader.startSegmentId + ", tail segid:" + mainHeader.tailSegmentId
                            + ", close segid:" + tempSegmentId+", delete:" + false);
                }
            }
        }
        return element.buff;
    }

    private Element readElementFromSegment(StorageSegment segment, int position,
                                           List<StorageSegment> closeSegmentList,
                                           boolean movePositionAfterRead) {
        int dataLength = 0;
        Element element = null;
        int segmentId = segment.getSegmentId();
        int remain = segment.remaining(position);
        //Check element.ELEMENT_HEADER_LENGTH should fit into remaining bytes
        if (remain >= Element.ELEMENT_HEADER_LENGTH) {
            dataLength = readDataLengthFromSegment(segment, position);
            position += Element.ELEMENT_HEADER_LENGTH;
            element = readElementFromSegment(segment, position,
                    dataLength, closeSegmentList, movePositionAfterRead);
        } else {
            closeSegmentList.add(segment);
            segment = getNextSegment(segmentId);
            position = 0;
            dataLength = readDataLengthFromSegment(segment, position);
            position += Element.ELEMENT_HEADER_LENGTH;
            element = readElementFromSegment(segment, position, dataLength,
                    closeSegmentList, movePositionAfterRead);
        }
        return element;
    }

    private int readDataLengthFromSegment(StorageSegment segment, int position) {
        byte[] buff = new byte[Element.ELEMENT_HEADER_LENGTH];
        segment.read(position, buff, 0, buff.length);
        int dataLength = PersistentUtil.readInt(buff, 0);
        return dataLength;
    }

    private Element readElementFromSegment(StorageSegment segment, int position, int dataLength,
                                           List<StorageSegment> closeSegmentList,
                                           boolean movePositionAfterRead) {
        byte[] buff = new byte[dataLength];
        int offset = 0;
        int segmentId = segment.getSegmentId();
        int remain = segment.remaining(position);
        if (dataLength > 0 && remain >= dataLength) {
            segment.read(position, buff, offset, dataLength);
            position += dataLength;
        } else {
            segment.read(position, buff, offset, remain);
            closeSegmentList.add(segment);
            int currentOffset = offset + remain;
            int currentRemainBytes = dataLength - remain;
            while (currentRemainBytes > 0) {
                segment = getNextSegment(segmentId);
                segmentId = segment.getSegmentId();
                position = 0;
                remain = segment.remaining(position);
                if (remain >= currentRemainBytes) {
                    segment.read(position, buff, currentOffset, currentRemainBytes);
                    position = currentRemainBytes;
                    currentRemainBytes = 0;
                    if (!movePositionAfterRead) {
                        closeSegmentList.add(segment);
                    }
                } else {
                    segment.read(position, buff, currentOffset, remain);
                    currentOffset += remain;
                    currentRemainBytes -= remain;
                    closeSegmentList.add(segment);
                }
            }
        }
        Element header = new Element(position, dataLength);
        header.buff = buff;
        header.segment = segment;
        return header;
    }

    private StorageSegment createAndOpenSegment(String path, String name, String ext,
                                                int segmentId, int initialSize) {
        StorageSegment storageSegment = null;
        //TODO: Need to move to factory class
        if (StorageSegment.SegmentType.MEMORYMAPPED.toString().equalsIgnoreCase(segmentType.toString())) {
            storageSegment = new MemoryMappedSegment();
            storageSegment.init(path, name, ext, segmentId, initialSize);
        } else {
            storageSegment = new FileSegment();
            storageSegment.init(path, name, ext, segmentId, initialSize);
        }
        return storageSegment;
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
            readFromSegment(true);
        }
    }

    public void clear() {
        close(true);
        initialize(this.path, this.name, this.initialSize);
    }

    public void close(boolean delete) {
        metadataSegment.close(delete);
        startSegment.close(delete);
        tailSegment.close(delete);
        for (int tempSegmentId : segmentHeader.segments) {
            StorageSegment tempSegment = getSegment(tempSegmentId);
            if (tempSegmentId != startSegment.getSegmentId() &&
                    tempSegmentId != tailSegment.getSegmentId()) {
                tempSegment.close(true);
            }
        }
    }

    public void writeMetadataHeader() {
        metadataSegment.write(0, mainHeader.convertToBytes(), 0,
                DEFAULT_METADATA_HEADER_LENGTH);
        metadataSegment.write(DEFAULT_METADATA_HEADER_LENGTH, segmentHeader.convertToBytes(),
                0, segmentHeader.length);
    }

    public void readMetadataHeader() {
        byte[] mainBuff = new byte[DEFAULT_METADATA_HEADER_LENGTH];
        metadataSegment.read(0, mainBuff, 0, DEFAULT_METADATA_HEADER_LENGTH);
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
        int currentPosition = mainHeader.startPosition;
        StorageSegment currentSegment = startSegment;
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
            List<StorageSegment> closeSegments = new ArrayList<>();
            Element element = readElementFromSegment(currentSegment, currentPosition,
                    closeSegments, false);
            for (StorageSegment tempSegment : closeSegments){
                int tempSegmentId = tempSegment.getSegmentId();
                if (startSegment.getSegmentId() != tempSegmentId &&
                        tailSegment.getSegmentId() != tempSegmentId &&
                        element.segment.getSegmentId() != tempSegmentId){
                    tempSegment.close(false);
                }
            }
            this.currentIndex++;
            this.currentPosition = element.position;
            this.currentSegment = element.segment;
            return element.buff;
        }

        @Override
        public void remove() {
            SegmentIndexer.this.remove();
            currentIndex--;
            currentTotalElements = getTotalElements();
        }
    }

    public void printStats(){

    }
    private static class MainHeader {
        private int version = 1;
        private long totalElements;
        private int startSegmentId;
        private int startPosition;
        private int tailSegmentId;
        private int tailPosition;

        private byte[] convertToBytes() {
            byte[] buff = new byte[DEFAULT_METADATA_HEADER_LENGTH];
            PersistentUtil.writeInt(buff, 0, version);
            PersistentUtil.writeLong(buff, 4, totalElements);
            PersistentUtil.writeInt(buff, 12, startSegmentId);
            PersistentUtil.writeInt(buff, 16, startPosition);
            PersistentUtil.writeInt(buff, 20, tailSegmentId);
            PersistentUtil.writeInt(buff, 24, tailPosition);
            return buff;
        }

        private void readBytes(byte[] buff) {
            version = PersistentUtil.readInt(buff, 0);
            totalElements = PersistentUtil.readLong(buff, 4);
            startSegmentId = PersistentUtil.readInt(buff, 12);
            startPosition = PersistentUtil.readInt(buff, 16);
            tailSegmentId = PersistentUtil.readInt(buff, 20);
            tailPosition = PersistentUtil.readInt(buff, 24);
        }
    }

    private static class SegmentHeader {
        //To write total segment count, it's 4 bytes
        private int SEGMENT_HEADER_LENGTH = 4;
        //Total segments to read
        private int length;
        //each segment 4 bytes * total segments
        private List<Integer> segments = new ArrayList<>();

        private byte[] convertToBytes() {
            ByteBuffer byteBuff = ByteBuffer.allocate(length);
            IntBuffer intBuffer = byteBuff.asIntBuffer();
            intBuffer.put(length);
            int[] segmentsArray = new int[segments.size()];
            int i = 0;
            for (int seg : segments) {
                segmentsArray[i] = seg;
                i++;
            }
            intBuffer.put(segmentsArray);
            return byteBuff.array();
        }

        private void addSegment(int segmentId) {
            if (!this.segments.contains(segmentId)) {
                this.segments.add(segmentId);
                length = SEGMENT_HEADER_LENGTH + segments.size() * 4;
            }
        }

        private void removeSegment(int segmentId) {
            if (this.segments.contains(segmentId)) {
                this.segments.remove(this.segments.indexOf(segmentId));
                length -= 4;
            }
        }
    }

    private static class Element {
        private static int ELEMENT_HEADER_LENGTH = 4;
        private int position;
        private int length;
        private byte[] buff;
        private StorageSegment segment;

        Element(int position, int length) {
            this.position = position;
            this.length = length;
        }
    }
}
