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

public class PersistentBlockingQueueTest extends AbstractBaseStorageTest {

    private static final Logger logger = LoggerFactory.getLogger(PersistentBlockingQueue.class.getName());

    private <T> PersistentBlockingQueue<T> getPersistentQueue() {
        PersistentBlockingQueue<T> pq = new PersistentQueueBuilder<T>()
                .path(this.path)
                .name(this.name)
                .fileSize(this.initialSize)
                .blocking(true)
                .build();
        return pq;
    }

    private <T> PersistentBlockingQueue<T> getPersistentQueue(int initialSize) {
        PersistentBlockingQueue<T> pq = new PersistentQueueBuilder<T>()
                .path(this.path)
                .name(this.name)
                .fileSize(initialSize)
                .blocking(true)
                .build();
        return pq;
    }

    private <T> PersistentBlockingQueue<T> getPersistentQueue(boolean cleanStorageOnRestart) {
        PersistentBlockingQueue<T> pq = new PersistentQueueBuilder<T>()
                .path(this.path)
                .name(this.name)
                .fileSize(initialSize)
                .cleanStorageOnRestart(cleanStorageOnRestart)
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
        PersistentBlockingQueue<String> pq = getPersistentQueue();
        pq.close();
    }

    @Test
    public void testSimpleAddWithInteger() {
        PersistentBlockingQueue<Integer> pq = getPersistentQueue();
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
    public void testCloseAndOpenAddWithInteger() {
        PersistentBlockingQueue<Integer> pq = getPersistentQueue(false);
        try {
            int numElements = 100;
            int halfCount = numElements/2;
            for (int i = 0; i < halfCount; i++) {
                pq.put(i);
            }
            Assert.assertEquals("Total size mismatch", halfCount, pq.size());
            pq.close();
            pq = getPersistentQueue();
            for (int i = 0; i < halfCount; i++) {
                int tempInt = pq.peek();
                Assert.assertEquals("Multiple peek call returning different results", 0, tempInt);
            }
            Assert.assertEquals("Total size mismatch", halfCount, pq.size());
            for (int i = 0; i < halfCount; i++) {
                pq.put((halfCount + i));
            }
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
        PersistentBlockingQueue<String> pq = getPersistentQueue();
        try {
            int numElements = 10;
            String prefix = "This is a test";
            Producer<String> producer = new Producer<>(pq, numElements, 1, prefix);
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
        PersistentBlockingQueue<String> pq = getPersistentQueue();
        try {
            int numElements = 2000;
            threadPool.submit(new Producer<>(pq, 400, 0, "This is first thread"));
            threadPool.submit(new Producer<>(pq, 400, 1, "This is second thread"));
            threadPool.submit(new Producer<>(pq, 400, 2, "This is third thread"));
            threadPool.submit(new Producer<>(pq, 400, 3, "This is fourth thread"));
            threadPool.submit(new Producer<>(pq, 400, 4, "This is fifth thread"));
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

    @Test
    public void testPutAndDrainWithMultipleDrainerThreads() {
        int tempInitialSize = 64 * 1024 * 1024;
        PersistentBlockingQueue<String> pq = getPersistentQueue(tempInitialSize);
        boolean loadTest = false;
        try {
            int loopCount = 1;
            int totalElements = 10000;
            if (loadTest) {
                loopCount = 10;
                totalElements = 1000000;
            }
            for (int j = 0; j < loopCount; j++) {
                logger.info("iteration :" + j);

                logger.info("Total elements:" + totalElements);
                long startTime = System.currentTimeMillis();
                for (int i = 0; i < 5; i++) {
                    threadPool.submit(new Producer<>(pq, totalElements / 5, i, oneKBText));
                }
                CountDownLatch latch = new CountDownLatch(2);
                Drainer<String> drainer1 = new Drainer<>(pq, latch, totalElements / 2, 0);
                Drainer<String> drainer2 = new Drainer<>(pq, latch, totalElements / 2, 1);
                threadPool.submit(drainer1);
                threadPool.submit(drainer2);
                latch.await();
                long endTime = System.currentTimeMillis();
                List<String> list = drainer1.list;
                list.addAll(drainer2.list);
                Assert.assertEquals("Total retrieved elements from drainer threads ", totalElements, list.size());
                Assert.assertEquals("Queue size should be zero ", 0, pq.size());
                logger.info("Total time for processing:" + (endTime - startTime) + "millis");
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
        int threadNum;
        String prefix;
        CountDownLatch latch = new CountDownLatch(1);

        Producer(BlockingQueue<T> q, int numElements, int threadNum, String prefix) {
            this.q = q;
            this.numElements = numElements;
            this.threadNum = threadNum;
            this.prefix = prefix;
        }

        Producer(BlockingQueue<T> q, int numElements, int threadNum, String prefix, CountDownLatch latch) {
            this.q = q;
            this.numElements = numElements;
            this.threadNum = threadNum;
            this.prefix = prefix;
            this.latch = latch;
        }

        @Override
        public Void call() throws Exception {
            String currentName = Thread.currentThread().getName();
            Thread.currentThread().setName("producer-" + threadNum);
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
                Thread.currentThread().setName(currentName);
            }
            return null;
        }
    }

    private class Drainer<T> implements Callable<List<T>> {
        int numElements;
        BlockingQueue<T> q;
        List<T> list;
        int threadNum;
        CountDownLatch latch;

        Drainer(BlockingQueue<T> q, CountDownLatch latch, int numElements, int threadNum) {
            this.q = q;
            this.latch = latch;
            this.numElements = numElements;
            this.threadNum = threadNum;
        }

        @Override
        public List<T> call() throws Exception {
            List<T> list = new ArrayList<>();
            String currentName = Thread.currentThread().getName();
            Thread.currentThread().setName("consumer-" + threadNum);
            try {
                int tempCount = PersistentUtil.drain(this.q, list, numElements, Long.MAX_VALUE, TimeUnit.NANOSECONDS);
                Assert.assertEquals("Retrieved elements count not matching part of drain", numElements, tempCount);
                Assert.assertEquals("Retrieved elements count not matching", numElements, list.size());
                Assert.assertEquals("After retrieval queue should be empty", true, this.q.isEmpty());
                Assert.assertEquals("After retrieval queue size should be zero", 0, this.q.size());
            } catch (Exception e) {
                logger.debug(e.getMessage(), e);
            } finally {
                this.list = list;
                latch.countDown();
                Thread.currentThread().setName(currentName);
            }
            return list;
        }
    }
}
