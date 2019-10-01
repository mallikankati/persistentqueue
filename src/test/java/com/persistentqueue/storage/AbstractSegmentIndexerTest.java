package com.persistentqueue.storage;

import org.junit.Assert;
import org.junit.Test;

import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.NoSuchElementException;

public abstract class AbstractSegmentIndexerTest extends AbstractBaseStorageTest {

    protected abstract SegmentIndexer getIndexer();

    @Test
    public void testInitialize() {
        SegmentIndexer indexer = getIndexer();
        indexer.close(true);
    }

    @Test
    public void testSimpleReadAndWrite() {
        SegmentIndexer indexer = getIndexer();
        try {
            byte[] buff = oneKBText.getBytes();
            indexer.writeToSegment(buff);
            Assert.assertEquals("Total elements mismatch after write", 1, indexer.getTotalElements());
            byte[] tempBuff = indexer.readFromSegment();
            String tempStr = new String(tempBuff);
            Assert.assertEquals("Expected string not matched", oneKBText, tempStr);
            Assert.assertEquals("Total elements mismatch after read", 0, indexer.getTotalElements());
            Assert.assertEquals("Total elements mismatch after read", true, indexer.isEmpty());
        } finally {
            indexer.close(true);
        }
    }

    @Test
    public void testMultipleReadAndWrite() {
        SegmentIndexer indexer = getIndexer();
        try {
            byte[] buff = oneKBText.getBytes();
            indexer.writeToSegment(buff);
            Assert.assertEquals("Total elements mismatch after write", 1, indexer.getTotalElements());
            buff = twoKBText.getBytes();
            indexer.writeToSegment(buff);
            Assert.assertEquals("Total elements mismatch after second write", 2, indexer.getTotalElements());
            byte[] tempBuff = indexer.readFromSegment();
            String tempStr = new String(tempBuff);
            Assert.assertEquals("Expected 1KB string not matched", oneKBText, tempStr);
            Assert.assertEquals("Total elements mismatch after first read", 1, indexer.getTotalElements());
            tempBuff = indexer.readFromSegment();
            tempStr = new String(tempBuff);
            Assert.assertEquals("Expected 2KB string not matched", twoKBText, tempStr);
            Assert.assertEquals("Total elements mismatch after second read", 0, indexer.getTotalElements());
            Assert.assertEquals("isEmpty() should return true", true, indexer.isEmpty());
        } finally {
            indexer.close(true);
        }
    }

    @Test
    public void testMultipleSmallReadAndWrite() {
        SegmentIndexer indexer = getIndexer();
        String prefix = "This is a test message to verify multiple reads ";
        int totalElements = 10;
        try {
            byte[] buff = null;
            for (int i = 0; i < totalElements; i++) {
                String temp = prefix + i;
                buff = temp.getBytes();
                indexer.writeToSegment(buff);
            }
            Assert.assertEquals("Total elements mismatch after " + totalElements + " write", totalElements,
                    indexer.getTotalElements());
            for (int i = 0; i < totalElements; i++) {
                String temp = prefix + i;
                buff = indexer.readFromSegment();
                String tempReadStr = new String(buff);
                Assert.assertEquals("Expected string not matched", temp, tempReadStr);
                int totalElementsAfterRead = totalElements - i - 1;
                Assert.assertEquals("Total elements mismatch after " + i + " read", totalElementsAfterRead,
                        indexer.getTotalElements());
            }
        } finally {
            indexer.close(true);
        }
    }

    @Test
    public void testTwoSegmentSmallReadAndWrite() {
        SegmentIndexer indexer = getIndexer();
        String prefix = "This is a test message to verify multiple reads ";
        int totalElements = 100;
        try {
            byte[] buff = null;
            for (int i = 0; i < totalElements; i++) {
                String temp = prefix + i;
                buff = temp.getBytes();
                indexer.writeToSegment(buff);
            }
            Assert.assertEquals("Total elements mismatch after " + totalElements + " write", totalElements,
                    indexer.getTotalElements());
            for (int i = 0; i < totalElements; i++) {
                String temp = prefix + i;
                buff = indexer.readFromSegment();
                String tempReadStr = new String(buff);
                Assert.assertEquals("Expected string not matched", temp, tempReadStr);
                int totalElementsAfterRead = totalElements - i - 1;
                Assert.assertEquals("Total elements mismatch after " + i + " read", totalElementsAfterRead,
                        indexer.getTotalElements());
            }
        } finally {
            indexer.close(true);
        }
    }

    @Test
    public void testMultipleMessagesMultipleSegmentReadAndWrite() {
        SegmentIndexer indexer = getIndexer();
        try {
            byte[] buff = threeKBText.getBytes();
            indexer.writeToSegment(buff);
            Assert.assertEquals("Total elements mismatch after write", 1, indexer.getTotalElements());
            buff = twoKBText.getBytes();
            indexer.writeToSegment(buff);
            Assert.assertEquals("Total elements mismatch after second write", 2,
                    indexer.getTotalElements());
            buff = fiveKBText.getBytes();
            indexer.writeToSegment(buff);
            Assert.assertEquals("Total elements mismatch after third write", 3,
                    indexer.getTotalElements());
            byte[] tempBuff = indexer.readFromSegment();
            String tempStr = new String(tempBuff);
            Assert.assertEquals("Expected 3KB string not matched", threeKBText, tempStr);
            Assert.assertEquals("Total elements mismatch after first read", 2, indexer.getTotalElements());
            tempBuff = indexer.readFromSegment();
            tempStr = new String(tempBuff);
            Assert.assertEquals("Expected 2KB string not matched", twoKBText, tempStr);
            Assert.assertEquals("Total elements mismatch after second read", 1, indexer.getTotalElements());
            tempBuff = indexer.readFromSegment();
            tempStr = new String(tempBuff);
            Assert.assertEquals("Expected 5KB string not matched", fiveKBText, tempStr);
            Assert.assertEquals("Total elements mismatch after third read", 0, indexer.getTotalElements());
        } finally {
            indexer.close(true);
        }
    }

    @Test
    public void testSingleMessageMultipleSegmentsReadAndWrite() {
        SegmentIndexer indexer = getIndexer();
        try {
            String tenKBText = fiveKBText + fiveKBText;
            byte[] buff = tenKBText.getBytes();
            indexer.writeToSegment(buff);
            Assert.assertEquals("Total elements mismatch after write", 1, indexer.getTotalElements());
            byte[] tempBuff = indexer.readFromSegment();
            String tempStr = new String(tempBuff);
            Assert.assertEquals("Expected 10KB string not matched", tenKBText, tempStr);
            Assert.assertEquals("Total elements mismatch after read", 0, indexer.getTotalElements());
        } finally {
            indexer.close(true);
        }
    }

    @Test
    public void testIterator() {
        SegmentIndexer indexer = getIndexer();
        String prefix = "This is a test message to verify multiple reads ";
        int totalElements = 100;
        try {
            byte[] buff = null;
            for (int i = 0; i < totalElements; i++) {
                String temp = prefix + i;
                buff = temp.getBytes();
                indexer.writeToSegment(buff);
            }
            Assert.assertEquals("Total elements mismatch after " + totalElements + " write", totalElements,
                    indexer.getTotalElements());
            Iterator<byte[]> it = indexer.iterator();
            int i = 0;
            while (it.hasNext()) {
                String temp = prefix + i;
                buff = it.next();
                String tempReadStr = new String(buff);
                Assert.assertEquals("Expected string not matched", temp, tempReadStr);
                i++;
            }
            for (i = 0; i < totalElements; i++) {
                String temp = prefix + i;
                buff = indexer.readFromSegment();
                String tempReadStr = new String(buff);
                Assert.assertEquals("Expected string not matched", temp, tempReadStr);
                int totalElementsAfterRead = totalElements - i - 1;
                Assert.assertEquals("Total elements mismatch after " + i + " read", totalElementsAfterRead,
                        indexer.getTotalElements());
            }
        } finally {
            indexer.close(true);
        }
    }

    @Test
    public void testConcurrentModification() {
        SegmentIndexer indexer = getIndexer();
        try {
            byte[] buff = oneKBText.getBytes();
            indexer.writeToSegment(buff);
            indexer.writeToSegment(buff);
            indexer.writeToSegment(buff);
            indexer.writeToSegment(buff);
            Assert.assertEquals("Total elements mismatch after write", 4, indexer.getTotalElements());
            Iterator<byte[]> it = indexer.iterator();
            if (it.hasNext()) {
                it.next();
                indexer.writeToSegment(buff);
                boolean thrown = false;
                try {
                    it.next();
                } catch (ConcurrentModificationException cme) {
                    thrown = true;
                }
                Assert.assertEquals("Expected ConcurrentModificationException but didn't throw", true, thrown);
            }
            byte[] tempBuff = indexer.readFromSegment();
            String tempStr = new String(tempBuff);
            Assert.assertEquals("Expected 1KB string not matched", oneKBText, tempStr);
            Assert.assertEquals("Total elements mismatch after read", 4, indexer.getTotalElements());
        } finally {
            indexer.close(true);
        }
    }

    @Test
    public void testNosuchElementException() {
        SegmentIndexer indexer = getIndexer();
        try {
            byte[] buff = oneKBText.getBytes();
            Iterator<byte[]> it = indexer.iterator();
            boolean thrown = false;
            try {
                it.next();
            } catch (NoSuchElementException e) {
                thrown = true;
            }
            Assert.assertEquals("Expected NoSuchElementException but didn't throw", true, thrown);
        } finally {
            indexer.close(true);
        }
    }

    @Test
    public void testRemove() {
        SegmentIndexer indexer = getIndexer();
        String prefix = "This is a test message to verify multiple reads ";
        try {
            byte[] buff = prefix.getBytes();
            indexer.writeToSegment(buff);
            indexer.writeToSegment(buff);
            indexer.writeToSegment(buff);
            indexer.writeToSegment(buff);
            Assert.assertEquals("Total elements mismatch after write", 4,
                    indexer.getTotalElements());
            indexer.remove();
            Assert.assertEquals("Total elements mismatch after remove", 3,
                    indexer.getTotalElements());
            indexer.remove(2);
            Assert.assertEquals("Total elements mismatch after two remove", 1,
                    indexer.getTotalElements());
        } finally {
            indexer.close(true);
        }
    }

    @Test
    public void testNRemove() {
        SegmentIndexer indexer = getIndexer();
        String prefix = "This is a test message to verify multiple reads ";
        try {
            byte[] buff = prefix.getBytes();
            indexer.writeToSegment(buff);
            indexer.writeToSegment(buff);
            indexer.writeToSegment(buff);
            indexer.writeToSegment(buff);
            Assert.assertEquals("Total elements mismatch after write", 4,
                    indexer.getTotalElements());
            indexer.remove(4);
            Assert.assertEquals("Total elements mismatch after remove", 0,
                    indexer.getTotalElements());
            indexer.writeToSegment(buff);
            Assert.assertEquals("Total elements mismatch after write", 1,
                    indexer.getTotalElements());
            byte[] tempBuff = indexer.readFromSegment();
            String tempStr = new String(tempBuff);
            Assert.assertEquals("String mismatch after read", tempStr,
                    prefix);
            Assert.assertEquals("Total elements mismatch after read", 0,
                    indexer.getTotalElements());
        } finally {
            indexer.close(true);
        }
    }
}
