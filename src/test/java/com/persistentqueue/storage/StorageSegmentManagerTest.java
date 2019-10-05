package com.persistentqueue.storage;

import com.persistentqueue.storage.utils.PersistentUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;

public class StorageSegmentManagerTest extends AbstractBaseStorageTest{

    private StorageSegmentManager getStorageSegmentManager(){
        StorageSegmentManager storageSegmentManager  = new StorageSegmentManager(this.path, this.name, this.dataFileExt,
                0, this.initialSize);
        storageSegmentManager.init(null, 2000);
        return storageSegmentManager;
    }

    @Test
    public void testIndexPageIdentifier(){
        int indexBits = 18;
        int indexItemBytes = 5;
        System.out.println("Items per page: " + (1<<indexBits));
        System.out.println("Index item length in bytes :" + (1<<indexItemBytes));
        System.out.println("Index page size in bytes: " + ((1<<indexItemBytes)*(1<<indexBits)));
        AtomicLong pageIndex = new AtomicLong(0);
        //for (int i = 0; i < ((1<<indexBits) +2); i++) {
             for (int i = 0; i < 20; i++) {
            long tempIndex = pageIndex.get();
            System.out.println("i:" + i + ", index  :" + (tempIndex >> indexBits));
            System.out.println("i:" + i + ", offset :" + ((tempIndex - ((tempIndex>>indexBits) << indexBits)) << indexItemBytes));
            pageIndex.incrementAndGet();
        }
    }

    @Test
    public void testInit(){
        StorageSegmentManager segmentManager = getStorageSegmentManager();
        segmentManager.close(true);
    }

    @Test
    public void testAcquireSegment(){
        StorageSegmentManager segmentManager = getStorageSegmentManager();
        try {
            StorageSegment segment = segmentManager.acquireSegment(0);
            Assert.assertEquals("Expected memory mapped file open as true", true, segment.isOpen());
            Assert.assertEquals("Expected memory mapped file closed is false", false, segment.isClosed());
            Assert.assertEquals("Expected cache size should be 1", 1, segmentManager.getCachedSegments());
            segmentManager.releaseSegment(0);
        } finally {
            segmentManager.close(true);
        }
    }

    @Test
    public void testCleanupTask(){
        StorageSegmentManager segmentManager = getStorageSegmentManager();
        try {
            StorageSegment segment = segmentManager.acquireSegment(0);
            Assert.assertEquals("Expected cache size before sleep should be 1", 1, segmentManager.getCachedSegments());
            PersistentUtil.sleep(2500);
            Assert.assertEquals("Expected cache size after sleep should be 1", 1, segmentManager.getCachedSegments());
            //segment.setDirty(true);
            segment.setDelete(true);
            segmentManager.releaseSegment(0);
            segment = segmentManager.acquireSegment(1);
            PersistentUtil.sleep(2500);
            segmentManager.releaseSegment(1);
            Assert.assertEquals("Expected cache size after releasesegment should be 1", 1, segmentManager.getCachedSegments());
        } finally {
            segmentManager.close(true);
        }
    }
}
