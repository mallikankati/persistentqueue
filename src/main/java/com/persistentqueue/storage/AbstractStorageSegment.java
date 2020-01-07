package com.persistentqueue.storage;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Abstract implementation of StorageSegment
 */
public abstract class AbstractStorageSegment implements StorageSegment {

    protected String path;
    /**
     * Initial length of the file
     */
    protected int initialLength;

    /**
     * Name of the file
     */
    protected String name;

    /**
     * file extension
     */
    protected String ext;

    protected int segmentId;

    protected String fullPath;

    protected boolean closed = false;

    protected boolean open = false;

    protected boolean delete = false;

    protected AtomicBoolean readWriteStatus = new AtomicBoolean(false);

    protected RandomAccessFile initializeFile(String path, String name, String ext,
                                              int segmentId, int initialLength) throws IOException {
        File file = null;
        File dirPath = new File(path);
        if (!dirPath.exists()){
            dirPath.mkdirs();
        }
        if (segmentId > 0) {
            this.fullPath = path + "/" + name + "_" + segmentId + ext;
        } else {
            this.fullPath = path + "/" + name + ext;
        }
        file = new File(fullPath);
        this.path = path;
        this.segmentId = segmentId;
        this.name = name;
        this.ext = ext;
        this.initialLength = initialLength;
        if (!file.exists()) {
            File tempFile = new File(file.getPath() + ".tmp");
            RandomAccessFile raf = new RandomAccessFile(tempFile, "rwd");
            try {
                raf.setLength(initialLength);
                raf.seek(0);
            } finally {
                raf.close();
            }
            if (!tempFile.renameTo(file)) {
                throw new IOException("Rename failed from:" + tempFile.getName() + ", to:" + file.getName());
            }
        }
        return new RandomAccessFile(file, "rwd");
    }

    protected void deleteFile() {
        File file = new File(this.fullPath);
        if (file.exists()) {
            Path tempPath = Paths.get(this.fullPath);
            try {
                Files.deleteIfExists(tempPath);
            } catch (Exception e){
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public String getPath() {
        return this.path;
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public String getExtension() {
        return this.ext;
    }

    @Override
    public int getLength() {
        return this.initialLength;
    }

    @Override
    public int getSegmentId() {
        return this.segmentId;
    }

    @Override
    public boolean isReadOrWriteInProgress(){
        return readWriteStatus.get();
    }

    public void setReadWriteStatus(boolean status){
        readWriteStatus.set(status);
    }
}
