package com.persistentqueue;


import com.persistentqueue.PersistentQueue;

import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class PersistentBlockingQueue<E> extends PersistentQueue<E> implements BlockingQueue<E> {


    public PersistentBlockingQueue(String path, String name, int dataSegmentSize){
        super(path, name, dataSegmentSize);
    }

    @Override
    public void put(E e) throws InterruptedException {
    }

    @Override
    public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
        return false;
    }

    @Override
    public E take() throws InterruptedException {
        return null;
    }

    @Override
    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        return null;
    }

    @Override
    public int remainingCapacity() {
        return 0;
    }

    @Override
    public int drainTo(Collection<? super E> c) {
        return 0;
    }

    @Override
    public int drainTo(Collection<? super E> c, int maxElements) {
        return 0;
    }
}
