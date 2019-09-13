package com.persistentqueue;

import com.persistentqueue.storage.StorageSegment;

import java.util.function.Consumer;

public class PersistentQueueBuilder<T> {
    public String path;
    public String name;
    public int fileSize;
    public boolean blocking = false;
    public Class<T> typeClass;
    public StorageSegment.SegmentType segmentType;

    public PersistentQueueBuilder<T> with(Consumer<PersistentQueueBuilder<T>> builderFunc){
        builderFunc.accept(this);
        return this;
    }

    public PersistentQueue<T> build(){
        PersistentQueue<T> pq = null;
        if (!blocking) {
            pq = new PersistentQueue<>(this.path, this.name, this.fileSize);
        } else {
            pq = new PersistentBlockingQueue<>(this.path, this.name, this.fileSize);
        }
        pq.init(this.typeClass, this.segmentType);
        return pq;
    }

}
