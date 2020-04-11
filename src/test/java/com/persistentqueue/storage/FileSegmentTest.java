package com.persistentqueue.storage;

import org.junit.Assert;
import org.junit.Test;

public class FileSegmentTest extends AbstractBaseStorageTest {

    @Test
    public void testInit() throws Exception {
        StorageSegment segment = getFileSegment(0);
        try {
            Assert.assertEquals("Expected file open as true", true, segment.isOpen());
            Assert.assertEquals("Path mismatch", this.path, segment.getPath());
            Assert.assertEquals("Name mismatch", this.name, segment.getName());
            Assert.assertEquals("File extension mismatch", this.dataFileExt, segment.getExtension());
            Assert.assertEquals("File initial size mismatch", this.initialSize, segment.getLength());
            Assert.assertEquals("Segment id mismatch", 0, segment.getSegmentId());
            Assert.assertEquals("File current position mismatch", 0, segment.getCurrentPosition());
            Assert.assertEquals("Remaining size mismatch", this.initialSize, segment.remaining(0));
            segment.close();
            Assert.assertEquals("Expected file open as false", false, segment.isOpen());
            Assert.assertEquals("Expected file close as true", true, segment.isClosed());
            segment.setDelete(true);
        } finally {
            try {
                segment.close();
            } catch (Exception ignore) {
            }
        }
    }

    @Test
    public void testReadAndWrite() {
        StorageSegment segment = getFileSegment(0);
        try {
            byte[] buff = oneKBText.getBytes();
            segment.write(0, buff);
            int length = buff.length;
            byte[] tempBuff = new byte[length];
            Assert.assertEquals("Remaining space mismatch after write", (this.initialSize - length), segment.remaining(length));
            Assert.assertEquals("Remaining space should match to total space", (this.initialSize), segment.remaining(0));
            Assert.assertEquals("Current position mismatch", length, segment.getCurrentPosition());
            Assert.assertEquals("Space Available should return true", true, segment.isSpaceAvailable(length));
            Assert.assertEquals("Space Available should return true", true, segment.isSpaceAvailable(3 * length));
            Assert.assertEquals("Space Available should return false", false, segment.isSpaceAvailable(4 * length));
            tempBuff = segment.read(0, 0, buff.length);
            String tempText = new String(tempBuff);
            Assert.assertEquals("Both String should be equal", oneKBText, tempText);
        } finally {
            try {
                segment.setDelete(true);
                segment.close();
            } catch (Exception ignore) {
            }
        }
    }

    @Test
    public void testClose() {
        StorageSegment segment = getFileSegment(0);
        try {
            byte[] buff = oneKBText.getBytes();
            segment.write(0, buff);
            int length = buff.length;
            byte[] tempBuff = new byte[length];
            segment.close();
            segment = getFileSegment(0);
            segment.seekToPosition(length);
            Assert.assertEquals("Remaining space mismatch after write", (this.initialSize - length), segment.remaining(length));
            Assert.assertEquals("Remaining space should match to total space", (this.initialSize), segment.remaining(0));
            Assert.assertEquals("Current position mismatch", length, segment.getCurrentPosition());
            Assert.assertEquals("Space Available should return true", true, segment.isSpaceAvailable(length));
            Assert.assertEquals("Space Available should return true", true, segment.isSpaceAvailable(3 * length));
            Assert.assertEquals("Space Available should return false", false, segment.isSpaceAvailable(4 * length));
            tempBuff = segment.read(0, 0, buff.length);
            String tempText = new String(tempBuff);
            Assert.assertEquals("Both String should be equal", oneKBText, tempText);
        } catch (Exception e) {

        } finally {
            try {
                segment.setDelete(true);
                segment.close();
            } catch (Exception ignore) {
            }
        }
    }
}
