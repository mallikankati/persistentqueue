package com.persistentqueue;

import com.persistentqueue.storage.AbstractBaseStorageTest;
import com.persistentqueue.storage.utils.PersistentUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

public class PersistentBlockingQueueTest extends AbstractBaseStorageTest {

    private static final Logger logger = LoggerFactory.getLogger(PersistentBlockingQueue.class.getName());

    private <T> PersistentBlockingQueue<T> getPersistentQueue(Class<T> typeClass) {
        PersistentBlockingQueue<T> pq = new PersistentQueueBuilder<T>()
                .path(this.path)
                .name(this.name)
                .fileSize(this.initialSize)
                .typeClass(typeClass)
                .blocking(true)
                .build();
        return pq;
    }

    private ExecutorService threadPool;

    @Before
    public void setUp() {
        threadPool = Executors.newCachedThreadPool();
    }

    @Test
    public void testBuilder() {
        PersistentBlockingQueue<String> pq = getPersistentQueue(String.class);
        pq.close();
    }

    @Test
    public void testSimpleAddWithInteger() {
        PersistentBlockingQueue<Integer> pq = getPersistentQueue(Integer.class);
        try {
            int numElements = 10;
            for (int i = 0; i < numElements; i++) {
                pq.put(i);
            }
            Assert.assertEquals("Total size mismatch", numElements, pq.size());
            for (int i = 0; i < numElements; i++) {
                int tempInt = pq.peek();
                Assert.assertEquals("Multiple peek call returning different results", 0, tempInt);
            }
            Assert.assertEquals("Total size mismatch", numElements, pq.size());
            for (int i = 0; i < numElements; i++) {
                int tempInt = pq.poll(1, TimeUnit.SECONDS);
                Assert.assertEquals("Multiple poll call fails results", i, tempInt);
            }
            Assert.assertEquals("Total size mismatch after poll", 0, pq.size());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            pq.close();
        }
    }

    @Test
    public void testPutAndDrainWithOneThread() {
        PersistentBlockingQueue<String> pq = getPersistentQueue(String.class);
        try {
            int numElements = 10;
            String prefix = "This is a test";
            Producer<String> producer = new Producer<>(pq, numElements, prefix);
            threadPool.submit(producer);
            producer.latch.await();
            try {
                List<String> list = new ArrayList<>();
                Assert.assertEquals("Before retrieval queue size zero", numElements, pq.size());
                int tempCount = PersistentUtil.drain(pq, list, numElements, 1, TimeUnit.SECONDS);
                Assert.assertEquals("Retrieved elements count not matching part of drain", numElements, tempCount);
                Assert.assertEquals("Retrieved elements count not matching", numElements, list.size());
                for (int i = 0; i < numElements; i++) {
                    String tempStr = prefix + i;
                    Assert.assertEquals("Elements not matching ", tempStr, list.get(i));
                }
                Assert.assertEquals("After retrieval queue should be empty", true, pq.isEmpty());
                Assert.assertEquals("After retrieval queue size should be zero", 0, pq.size());
            } catch (Exception e) {
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            pq.close();
        }
    }

    @Test
    public void testPutAndDrainWithMultipleThreads() {
        PersistentBlockingQueue<String> pq = getPersistentQueue(String.class);
        try {
            int numElements = 2000;
            threadPool.submit(new Producer<>(pq, 400, "This is first thread"));
            threadPool.submit(new Producer<>(pq, 400, "This is second thread"));
            threadPool.submit(new Producer<>(pq, 400,"This is third thread"));
            threadPool.submit(new Producer<>(pq, 400,"This is fourth thread"));
            threadPool.submit(new Producer<>(pq, 400,"This is fifth thread"));
            try {
                List<String> list = new ArrayList<>();
                int tempCount = PersistentUtil.drain(pq, list, numElements, Long.MAX_VALUE, TimeUnit.NANOSECONDS);
                Assert.assertEquals("Retrieved elements count not matching part of drain", numElements, tempCount);
                Assert.assertEquals("Retrieved elements count not matching", numElements, list.size());
                Assert.assertEquals("After retrieval queue should be empty", true, pq.isEmpty());
                Assert.assertEquals("After retrieval queue size should be zero", 0, pq.size());
            } catch (Exception e) {
                logger.debug(e.getMessage(), e);
            }
        } catch (Exception e) {
            logger.debug(e.getMessage(), e);
        } finally {
            pq.close();
        }
    }

    private class Producer<T> implements Callable<Void> {
        int numElements;
        BlockingQueue<T> q;
        String prefix;
        CountDownLatch latch = new CountDownLatch(1);

        Producer(BlockingQueue<T> q, int numElements, String prefix) {
            this.q = q;
            this.numElements = numElements;
            this.prefix = prefix;
        }

        @Override
        public Void call() throws Exception {
            try {
                for (int i = 0; i < numElements; i++) {
                    if (prefix != null) {
                        String str = prefix + i;
                        q.put((T) str);
                    } else {
                        Integer t = i;
                        q.put((T) t);
                    }
                }
            } finally {
                latch.countDown();
            }
            return null;
        }
    }
}
