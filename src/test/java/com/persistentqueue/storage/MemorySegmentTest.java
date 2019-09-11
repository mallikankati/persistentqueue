package com.persistentqueue.storage;

import org.junit.Assert;
import org.junit.Test;

public class MemorySegmentTest extends AbstractBaseStorageTest {

    @Test
    public void testInit(){
        StorageSegment segment = getMemorySegment(0);
        try {
            Assert.assertEquals("Expected memory mapped file open as true", true, segment.isOpen());
            Assert.assertEquals("memory mapped file path mismatch", this.path, segment.getPath());
            Assert.assertEquals("memory mapped file name mismatch", this.name, segment.getName());
            Assert.assertEquals("memory mapped file extension mismatch", this.dataFileExt, segment.getExtension());
            Assert.assertEquals("memory mapped file initial size mismatch", this.initialSize, segment.getLength());
            Assert.assertEquals("memory mapped Segment id mismatch", 0, segment.getSegmentId());
            Assert.assertEquals("memory mapped file current position mismatch", 0, segment.getCurrentPosition());
            Assert.assertEquals("memory mapped file Remaining size mismatch", this.initialSize, segment.remaining(0));
        } finally {
            segment.close(true);
        }
        Assert.assertEquals("memory mapped file open as false", false, segment.isOpen());
        Assert.assertEquals("memory mapped file close as true", true, segment.isClosed());
    }

    @Test
    public void testReadAndWrite(){
        StorageSegment segment = getMemorySegment(0);
        try {
            byte[] buff = oneKBText.getBytes();
            segment.write(0, buff, 0, buff.length);
            int length = buff.length;
            byte[] tempBuff = new byte[length];
            Assert.assertEquals("memory mapped file remaining space mismatch after write", (this.initialSize - length), segment.remaining(length));
            Assert.assertEquals("memory mapped file remaining space should match to total space", (this.initialSize), segment.remaining(0));
            //TODO need to fix this check
           // Assert.assertEquals("memory mapped file current position mismatch", length, segment.getCurrentPosition());
            Assert.assertEquals("memory mapped file Space Available should return true", true, segment.isSpaceAvailable(length));
            Assert.assertEquals("memory mapped file Space Available should return true", true, segment.isSpaceAvailable(3 * length));
            Assert.assertEquals("memory mapped file Space Available should return false", false, segment.isSpaceAvailable(4 * length));
            segment.read(0, tempBuff, 0, buff.length);
            String tempText = new String(tempBuff);
            Assert.assertEquals("Both String should be equal", oneKBText, tempText);
        } finally {
            segment.close(true);
        }
    }

    @Test
    public void testClose(){
        StorageSegment segment = getMemorySegment(0);
        try {
            byte[] buff = oneKBText.getBytes();
            segment.write(0, buff, 0, buff.length);
            int length = buff.length;
            byte[] tempBuff = new byte[length];
            segment.close(false);
            segment = getMemorySegment(0);
            segment.seekToPosition(length);
            Assert.assertEquals("memory mapped file remaining space mismatch after write", (this.initialSize - length), segment.remaining(length));
            Assert.assertEquals("memory mapped file remaining space should match to total space", (this.initialSize), segment.remaining(0));
            //TODO need to fix this
            //Assert.assertEquals("memory mapped file current position mismatch", length, segment.getCurrentPosition());
            Assert.assertEquals("memory mapped file space Available should return true", true, segment.isSpaceAvailable(length));
            Assert.assertEquals("memory mapped file space Available should return true", true, segment.isSpaceAvailable(3 * length));
            Assert.assertEquals("memory mapped file space Available should return false", false, segment.isSpaceAvailable(4 * length));
            segment.read(0, tempBuff, 0, buff.length);
            String tempText = new String(tempBuff);
            Assert.assertEquals("memory mapped file both String should be equal", oneKBText, tempText);
        } finally {
            segment.close(true);
        }
    }
}
