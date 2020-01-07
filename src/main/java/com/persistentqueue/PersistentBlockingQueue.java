package com.persistentqueue;


import com.persistentqueue.storage.StorageSegment;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * PersistentBlockingQueue implements {@link BlockingQueue} interface which optionally capacity bounded.
 * {@link Integer}#MAX_VALUE is the max capacity.
 * <p>
 * Note: This implementation not support {@link java.util.Iterator}#remove
 *
 * @param <E>
 */
public class PersistentBlockingQueue<E> extends PersistentQueue<E> implements BlockingQueue<E> {

    /**
     * Managing the size of the queue external to the
     */
    private final AtomicInteger count = new AtomicInteger();

    /**
     * Capacity is not specified, uses this value to guard to get unbounded
     */
    private int capacity = Integer.MAX_VALUE;

    /**
     * Held while take and poll operations
     */
    private final ReentrantLock takeLock = new ReentrantLock();

    /**
     * Waiting for take and poll tasks
     */
    private final Condition notEmpty = takeLock.newCondition();

    /**
     * Held while put and offer operations
     */
    private final ReentrantLock putLock = new ReentrantLock();

    /**
     * Waiting for put and offer tasks
     */
    private final Condition notFull = putLock.newCondition();


    public PersistentBlockingQueue(String path, String name, int dataSegmentSize, boolean cleanStorageOnRestart) {
        super(path, name, dataSegmentSize, cleanStorageOnRestart);
    }

    /**
     * Capacity bounded
     *
     * @param path            directory where to store the elements
     * @param name            name of the queue
     * @param dataSegmentSize each data segment size
     * @param capacity        capacity of this queue
     */
    public PersistentBlockingQueue(String path, String name, int dataSegmentSize, int capacity, boolean cleanStorageOnRestart) {
        super(path, name, dataSegmentSize, cleanStorageOnRestart);
        if (capacity <= 0) {
            throw new IllegalArgumentException();
        }
        this.capacity = capacity;
    }

    /**
     * Initializing queue
     * @param segmentType
     * @param serializer
     */
    public void init(StorageSegment.SegmentType segmentType,
                     PersistentQueueSerializer<E> serializer) {
        super.init(segmentType, serializer);
        this.count.set(super.size());
    }
    /**
     * Signals a waiting take. Called only from put/offer (which do not
     * otherwise ordinarily lock takeLock.)
     */
    private void signalNotEmpty() {
        final ReentrantLock takeLock = this.takeLock;
        takeLock.lock();
        try {
            notEmpty.signal();
        } finally {
            takeLock.unlock();
        }
    }

    /**
     * Signals a waiting put. Called only from take/poll.
     */
    private void signalNotFull() {
        final ReentrantLock putLock = this.putLock;
        putLock.lock();
        try {
            notFull.signal();
        } finally {
            putLock.unlock();
        }
    }

    /**
     * Locks to prevent both puts and takes.
     */
    private void fullyLock() {
        putLock.lock();
        takeLock.lock();
    }

    /**
     * Unlocks to allow both puts and takes.
     */
    private void fullyUnlock() {
        takeLock.unlock();
        putLock.unlock();
    }

    @Override
    public int size() {
        return super.size();
    }

    @Override
    public void put(E e) throws InterruptedException {
        if (e == null) throw new NullPointerException();
        int c = -1;
        final ReentrantLock putLock = this.putLock;
        putLock.lockInterruptibly();
        try {
            while (count.get() == capacity) {
                notFull.await();
            }
            super.offer(e);
            c = count.getAndIncrement();
            if (c + 1 < capacity) {
                notFull.signal();
            }
        } finally {
            putLock.unlock();
        }
        if (c == 0) {
            signalNotEmpty();
        }
    }

    @Override
    public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
        if (e == null) throw new NullPointerException();
        long nanos = unit.toNanos(timeout);
        int c = -1;
        final ReentrantLock putLock = this.putLock;
        putLock.lockInterruptibly();
        try {
            while (count.get() == capacity) {
                if (nanos <= 0) {
                    return false;
                }
                nanos = notFull.awaitNanos(nanos);
            }
            super.offer(e);
            c = count.getAndIncrement();
            if (c + 1 < capacity) {
                notFull.signal();
            }
        } finally {
            putLock.unlock();
        }
        if (c == 0) {
            signalNotEmpty();
        }
        return true;
    }

    @Override
    public E take() throws InterruptedException {
        E e;
        final AtomicInteger count = this.count;
        int c = -1;
        final ReentrantLock takeLock = this.takeLock;
        takeLock.lockInterruptibly();
        try {
            while (count.get() == 0) {
                notEmpty.await();
            }
            e = super.poll();
            c = count.getAndDecrement();
            if (c > 1) {
                notEmpty.signal();
            }
        } finally {
            takeLock.unlock();
        }
        if (c == capacity) {
            signalNotFull();
        }
        return e;
    }

    @Override
    public E poll() {
        final AtomicInteger count = this.count;
        if (count.get() == 0) {
            return null;
        }
        int c = -1;
        E e = null;
        final ReentrantLock takeLock = this.takeLock;
        takeLock.lock();
        try {
            if (count.get() > 0) {
                e = super.poll();
                c = count.getAndDecrement();
                if (c > 0) {
                    notEmpty.signal();
                }
            }
        } finally {
            takeLock.unlock();
        }
        if (c == capacity) {
            signalNotFull();
        }
        return e;
    }

    @Override
    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        E e = null;
        final AtomicInteger count = this.count;
        int c = -1;
        long nanos = unit.toNanos(timeout);
        final ReentrantLock takeLock = this.takeLock;
        takeLock.lockInterruptibly();
        try {
            while (count.get() == 0) {
                if (nanos <= 0) {
                    return null;
                }
                nanos = notEmpty.awaitNanos(nanos);
            }
            e = super.poll();
            c = count.getAndDecrement();
            if (c > 1) {
                notEmpty.signal();
            }
        } finally {
            takeLock.unlock();
        }
        if (c == capacity) {
            signalNotFull();
        }
        return e;
    }

    @Override
    public boolean offer(E e) {
        if (e == null) throw new NullPointerException();
        final AtomicInteger count = this.count;
        if (count.get() == capacity)
            return false;
        int c = -1;
        final ReentrantLock putLock = this.putLock;
        putLock.lock();
        try {
            if (count.get() < capacity) {
                super.offer(e);
                c = count.getAndIncrement();
                if (c + 1 < capacity) {
                    notFull.signal();
                }
            }
        } finally {
            putLock.unlock();
        }
        if (c == 0) {
            signalNotEmpty();
        }
        return c >= 0;
    }

    @Override
    public E peek() {
        if (count.get() == 0) {
            return null;
        }
        E e = null;
        final ReentrantLock takeLock = this.takeLock;
        takeLock.lock();
        try {
            e = super.peek();
        } finally {
            takeLock.unlock();
        }
        return e;
    }

    @Override
    public void clear() {
        fullyLock();
        try {
            super.clear();
            if (count.getAndSet(0) == capacity) {
                notFull.signal();
            }
        } finally {
            fullyUnlock();
        }
    }

    @Override
    public int remainingCapacity() {
        return capacity - size();
    }

    @Override
    public int drainTo(Collection<? super E> c) {
        return drainTo(c, Integer.MAX_VALUE);
    }

    @Override
    public int drainTo(Collection<? super E> c, int maxElements) {
        if (c == null) {
            throw new NullPointerException();
        }
        if (c == this) {
            throw new IllegalArgumentException();
        }
        if (maxElements <= 0) {
            return 0;
        }
        final ReentrantLock takeLock = this.takeLock;
        takeLock.lock();
        boolean signalNotFull = false;
        int i = 0;
        try {
            int n = Math.min(count.get(), maxElements);
            try {
                while (i < n) {
                    E e = super.poll();
                    c.add(e);
                    i++;
                }
            } finally {
                if (i > 0) {
                    signalNotFull = count.getAndAdd(-i) == capacity;
                }
            }
        } finally {
            takeLock.unlock();
            if (signalNotFull) {
                signalNotFull();
            }
        }
        return i;
    }

    @Override
    public Iterator<E> iterator() {
        return new BlockingQueueItr();
    }

    private class BlockingQueueItr implements Iterator<E> {
        Iterator<E> it = new PersistentQueueIterator();

        @Override
        public boolean hasNext() {
            return it.hasNext();
        }

        @Override
        public E next() {
            fullyLock();
            try {
                return it.next();
            } finally {
                fullyUnlock();
            }
        }

        @Override
        public void remove() {
            fullyLock();
            try {
                it.remove();
            } finally {
                fullyUnlock();
            }
        }
    }

    @Override
    public boolean remove(Object o) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public boolean contains(Object o) {
        throw new RuntimeException("Not implemented");
    }
}
