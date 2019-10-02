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

    private StorageSegment.SegmentType segmentType = StorageSegment.SegmentType.MEMORYMAPPED;

    public StorageSegmentManager(String path, String name, String ext, int segmentId, int initialLength) {
        this.path = path;
        this.name = name;
        this.ext = ext;
        this.segmentId = segmentId;
        this.initialLength = initialLength;
    }

    public void init(StorageSegment.SegmentType segmentType, long timeToLiveInMillis) {
        if (this.initialLength <= 0) {
            throw new RuntimeException("initial segment size should be more than 1KB");
        }
        if (this.path == null || this.path.trim().length() <= 0) {
            throw new RuntimeException("Need a valid path");
        }
        if (this.name == null || this.name.trim().length() <= 0) {
            throw new RuntimeException("Need a valid name");
        }
        if (this.ext == null || this.ext.trim().length() <= 0) {
            throw new RuntimeException("Need a valid file extension");
        }
        if (this.segmentId < 0) {
            throw new RuntimeException("Segment id should be greater than zero");
        }
        if (segmentType != null) {
            this.segmentType = segmentType;
        }
        if (timeToLiveInMillis > 0) {
            this.timeToLiveInMillis = timeToLiveInMillis;
        }
    }

    public void close(boolean delete) {
        totalLock.lock();
        try {
            for (CacheValue cacheValue : cache.values()) {
                try {
                    cacheValue.storageSegment.setDelete(delete);
                    if (!cacheValue.storageSegment.isClosed()) {
                        cacheValue.storageSegment.close();
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            if (delete) {
                cache.clear();
            }
        } finally {
            totalLock.unlock();
        }
        executorService.shutdown();
    }

    public StorageSegment acquireSegment(int segmentId) {
        return acquireSegment(segmentId, this.initialLength);
    }

    public StorageSegment acquireSegment(int segmentId, int customSize) {
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
                        cacheValue = putAndGet(segmentId, customSize);
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


    public int getCachedSegments() {
        return cache.size();
    }

    private CacheValue putAndGet(int segmentId, int customSize) {
        List<StorageSegment> closeSegments = identifyCloseSegments(segmentId);
        CacheValue cacheValue = new CacheValue();
        cacheValue.lastAccessTime = System.currentTimeMillis();
        cacheValue.refCount.incrementAndGet();
        cacheValue.ttl = this.timeToLiveInMillis;
        StorageSegment segment = createSegment(this.path, this.name, this.ext, segmentId, customSize);
        cacheValue.storageSegment = segment;
        cache.put(segmentId, cacheValue);
        executorService.submit(new CleanupTask(closeSegments));
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

    public void markForDelete(int segmentId) {
        CacheValue cacheValue = cache.get(segmentId);
        if (cacheValue != null) {
            cacheValue.storageSegment.setDelete(true);
        }
    }

    private class CacheValue {
        private long lastAccessTime;
        private AtomicInteger refCount = new AtomicInteger(0);
        private long ttl;
        private StorageSegment storageSegment;

        CacheValue() {
        }

        private boolean isExpired(long currentTime) {
            return refCount.get() <= 0 && (currentTime > (lastAccessTime + ttl));
        }
    }

    private List<StorageSegment> identifyCloseSegments(int segmentId) {
        List<StorageSegment> dirtySegments = new ArrayList<>();
        long currentTime = System.currentTimeMillis();
        for (CacheValue cacheValue : cache.values()) {
            if (cacheValue.storageSegment.getSegmentId() != segmentId && cacheValue.isExpired(currentTime)) {
                dirtySegments.add(cacheValue.storageSegment);
            }
        }
        return dirtySegments;
    }

    //This suppose to be in cache.
    private class CleanupTask implements Runnable {

        List<StorageSegment> closeSegments;

        CleanupTask(List<StorageSegment> closeSegments) {
            this.closeSegments = closeSegments;
        }

        @Override
        public void run() {
            try {
                if (!closeSegments.isEmpty()) {
                    for (StorageSegment tempSegment : closeSegments) {
                        logger.debug("Cleanup in progress for segment [ ext:" + tempSegment.getExtension() +
                                ", id:" + tempSegment.getSegmentId() + "]");
                        if (!tempSegment.isClosed()) {
                            tempSegment.close();
                        }
                        cache.remove(tempSegment.getSegmentId());
                    }
                }
            } catch (Exception e) {
                logger.info(e.getMessage(), e);
            } finally {
            }
        }
    }

    public String toString(){
        StringBuffer sb = new StringBuffer();
        sb.append("storageManager [ ext:").append(this.ext);
        sb.append(", size:" + this.getCachedSegments());
        sb.append(", ").append(cache.keySet()).append("]");
        return sb.toString();
    }
}
