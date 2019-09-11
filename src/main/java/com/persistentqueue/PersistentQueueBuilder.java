package com.persistentqueue;

import java.util.function.Consumer;

public class PersistentQueueBuilder<T> {
    public String path;
    public String name;
    public int fileSize;
    public boolean blocking = false;
    public Class<T> typeClass;

    public PersistentQueueBuilder<T> with(Consumer<PersistentQueueBuilder<T>> builderFunc){
        builderFunc.accept(this);
        return this;
    }

    public PersistentQueue<T> build(){
        PersistentQueue<T> pq = new PersistentQueue<>(this.path, this.name, this.fileSize);
        pq.init(this.typeClass);
        return pq;
    }
}
