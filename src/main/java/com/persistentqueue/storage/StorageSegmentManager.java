package com.persistentqueue.storage;

import com.persistentqueue.storage.utils.PersistentUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Cache the {@link StorageSegment} and provides memory usage under control.
 */
public class StorageSegmentManager {

    private static final Logger logger = LoggerFactory.getLogger(StorageSegmentManager.class);

    private String path;

    /**
     * Initial length of the file
     */
    private int initialLength;

    /*
     * Name of the file
     */
    private String name;

    /**
     * file extension
     */
    private String ext;

    private int segmentId;

    private Map<Integer, CacheValue> cache = new ConcurrentHashMap<>();

    private Map<Integer, Object> segmentLocks = new HashMap<>();

    private ReentrantLock totalLock = new ReentrantLock();

    private ExecutorService executorService = Executors.newCachedThreadPool();

    private long timeToLiveInMillis = 5000;

    private long cleanupFrequencyInMillis = 5000;

    private StorageSegment.SegmentType segmentType = StorageSegment.SegmentType.MEMORYMAPPED;

    private boolean startCleanupTask = false;

    private boolean closeInProgress = false;

    private boolean cleanInProgress = false;

    public StorageSegmentManager(String path, String name, String ext, int segmentId, int initialLength) {
        this.path = path;
        this.name = name;
        this.ext = ext;
        this.segmentId = segmentId;
        this.initialLength = initialLength;
    }

    public void init(StorageSegment.SegmentType segmentType, long timeToLiveInMillis,
                     long cleanupFrequencyInMillis,
                     boolean startCleanupTask) {
        if (this.initialLength <= 0){
            throw new RuntimeException("initial segment size should be more than 1KB");
        }
        if (this.path == null || this.path.trim().length() <=0){
            throw new RuntimeException("Need a valid path");
        }
        if (this.name == null || this.name.trim().length() <=0){
            throw new RuntimeException("Need a valid name");
        }
        if (this.ext == null || this.ext.trim().length() <=0){
            throw new RuntimeException("Need a valid file extension");
        }
        if (this.segmentId < 0){
            throw new RuntimeException("Segment id should be greater than zero");
        }
        if (segmentType != null) {
            this.segmentType = segmentType;
        }
        if (timeToLiveInMillis > 0) {
            this.timeToLiveInMillis = timeToLiveInMillis;
        }
        if (cleanupFrequencyInMillis > 0){
            this.cleanupFrequencyInMillis = cleanupFrequencyInMillis;
        }
        if (startCleanupTask) {
            executorService.submit(new CleanupTask(this.cleanupFrequencyInMillis));
        }
    }

    public void close(boolean delete) {
       /* while(cleanInProgress){
            logger.info("cleanup task in progress, waiting :" + 1000 +"millis");
            this.closeInProgress = true;
            PersistentUtil.sleep(1000);
        }*/
        totalLock.lock();
        try {
            this.closeInProgress = true;
            for (CacheValue cacheValue : cache.values()) {
                try {
                    cacheValue.storageSegment.setDelete(delete);
                    cacheValue.storageSegment.close();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }finally {
            this.closeInProgress = false;
            totalLock.unlock();
        }
        if (startCleanupTask) {
            executorService.shutdown();
        }
    }

    public StorageSegment acquireSegment(int segmentId) {
        CacheValue cacheValue = cache.get(segmentId);
        if (cacheValue == null) {
            Object segmentLock = null;
            try {
                totalLock.lock();
                try {
                    if (!segmentLocks.containsKey(segmentId)) {
                        segmentLocks.put(segmentId, new Object());
                    }
                    segmentLock = segmentLocks.get(segmentId);
                } finally {
                    totalLock.unlock();
                }
                synchronized (segmentLock) {
                    cacheValue = cache.get(segmentId);
                    if (cacheValue == null) {
                        cacheValue = putAndGet(segmentId);
                    }
                }
            } finally {
                totalLock.lock();
                try {
                    //if you don't remove, who will cleanup from this map
                    segmentLocks.remove(segmentId);
                } finally {
                    totalLock.unlock();
                }
            }
        } else {
            cacheValue.lastAccessTime = System.currentTimeMillis();
            cacheValue.refCount.incrementAndGet();
        }

        return cacheValue.storageSegment;
    }

    public int getCachedSegments(){
        return cache.size();
    }

    private CacheValue putAndGet(int segmentId) {
        CacheValue cacheValue = new CacheValue();
        cacheValue.lastAccessTime = System.currentTimeMillis();
        cacheValue.refCount.incrementAndGet();
        cacheValue.ttl = this.timeToLiveInMillis;
        StorageSegment segment = createSegment(this.path, this.name, this.ext, segmentId, this.initialLength);
        cacheValue.storageSegment = segment;
        cache.put(segmentId, cacheValue);
        return cacheValue;
    }

    private StorageSegment createSegment(String path, String name, String ext,
                                         int segmentId, int initialSize) {
        StorageSegment storageSegment = null;
        if (StorageSegment.SegmentType.MEMORYMAPPED.toString().equalsIgnoreCase(segmentType.toString())) {
            storageSegment = new MemoryMappedSegment();
            storageSegment.init(path, name, ext, segmentId, initialSize);
        } else {
            storageSegment = new FileSegment();
            storageSegment.init(path, name, ext, segmentId, initialSize);
        }
        return storageSegment;
    }

    public void releaseSegment(int segmentId) {
        CacheValue cacheValue = cache.get(segmentId);
        if (cacheValue != null) {
            cacheValue.refCount.decrementAndGet();
        }
    }

    private class CacheValue {
        private long lastAccessTime;
        private AtomicInteger refCount = new AtomicInteger(0);
        private long ttl;
        private StorageSegment storageSegment;

        CacheValue() {
        }

        private boolean isExpired(long currentTime){
            return refCount.get() <= 0 && (currentTime > (lastAccessTime + ttl));
        }
    }

    //This suppose to be in cache.
    private class CleanupTask implements Runnable {

        private long timeInMillis;

        CleanupTask(long timeInMillis) {
            this.timeInMillis = timeInMillis;
        }

        @Override
        public void run() {
            while (!closeInProgress) {
                try {
                    List<Integer> dirtySegments = new ArrayList<>();
                    long currentTime = System.currentTimeMillis();
                    for (CacheValue cacheValue : cache.values()) {
                        if (cacheValue.isExpired(currentTime)){
                            if (cacheValue.storageSegment.isDirty()) {
                                cacheValue.storageSegment.close();
                                dirtySegments.add(cacheValue.storageSegment.getSegmentId());
                            }
                        }
                    }
                    cleanInProgress = true;
                    if (!dirtySegments.isEmpty()) {
                        for (Integer tempSegmentId : dirtySegments) {
                            cache.remove(tempSegmentId);
                        }
                    }
                    Thread.sleep(this.timeInMillis);
                } catch (Exception e) {
                    logger.info(e.getMessage(), e);
                } finally {
                    cleanInProgress = false;
                }
            }
        }
    }
}
