package com.persistentqueue;

import com.persistentqueue.storage.StorageSegment;

public class PersistentQueueBuilder<T> {
    private String path;
    private String name;
    private int fileSize = 5*1024*1024; //default set to 5MB file
    private boolean blocking = false;
    private boolean cleanStorageOnRestart = true;

    //private Class<T> typeClass;
    private StorageSegment.SegmentType segmentType;
    private PersistentQueueSerializer<T> serializer;

    public PersistentQueueBuilder<T> path(String path) {
        this.path = path;
        return this;
    }

    public PersistentQueueBuilder<T> name(String name) {
        this.name = name;
        return this;
    }

    public PersistentQueueBuilder<T> fileSize(int fileSize) {
        this.fileSize = fileSize;
        return this;
    }

    public  PersistentQueueBuilder<T> cleanStorageOnRestart(boolean cleanStorageOnRestart){
        this.cleanStorageOnRestart = cleanStorageOnRestart;
        return this;
    }

    public PersistentQueueBuilder<T> segmentType(StorageSegment.SegmentType segmentType) {
        this.segmentType = segmentType;
        return this;
    }

    public PersistentQueueBuilder<T> serializer(PersistentQueueSerializer<T> serializer) {
        this.serializer = serializer;
        return this;
    }

    public PersistentBlockingQueueBuilder<T> blocking(boolean blocking) {
        return new PersistentBlockingQueueBuilder<>(this);
    }

    /*public PersistentQueueBuilder<T> with(Consumer<PersistentQueueBuilder<T>> builderFunc) {
        builderFunc.accept(this);
        return this;
    }*/


    public PersistentQueue<T> build() {
        PersistentQueue<T> pq = null;
        pq = new PersistentQueue<>(this.path, this.name, this.fileSize, this.cleanStorageOnRestart);
        pq.init(this.segmentType, this.serializer);
        return pq;
    }

    public class PersistentBlockingQueueBuilder<T> {
        private int capacity = Integer.MAX_VALUE;

        private PersistentQueueBuilder<T> pqb;

        public PersistentBlockingQueueBuilder(PersistentQueueBuilder<T> pqb) {
            this.pqb = pqb;
        }

        public PersistentBlockingQueueBuilder<T> capacity(int capacity) {
            this.capacity = capacity;
            return this;
        }

        public PersistentBlockingQueue<T> build() {
            PersistentBlockingQueue<T> pbq = new PersistentBlockingQueue<>(this.pqb.path,
                    this.pqb.name, this.pqb.fileSize, this.capacity, this.pqb.cleanStorageOnRestart);
            pbq.init(this.pqb.segmentType, this.pqb.serializer);
            return pbq;
        }
    }
}
