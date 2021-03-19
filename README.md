## Diskbacked Queue

Diskbacked persistent queue which implements BlockingQueue interface in java. It leverage MemoryMapped files to receive the best performance.

These kind of queue used when it require
   - Low heap memory foot prints, when item stored in queue which stores in memorymapped file backed by disk.
   - Don't lose any events which stored in memory. When events stored in java.util.Queue<>, events get lost on reboot. PersistentQueue will preserve the insertion order with no loss of data.
   - It's a library not require any big distributed messaging like kafka infrastructure. This is not a distributed PersistentQueue, which works in only one JVM
   
### Sample code to use BlockingQueue

```java
public class PersistentBlockingQueueTest {

     public static void main(String[] args){
        PersistentBlockingQueue<Integer> pq = new PersistentQueueBuilder<Integer>()
                .path("valid directory")
                .name("name without space")
                .fileSize(134217728) //128MB 
                .blocking(true)
                .build();
        try {
            int loopCount = 1;
            int totalElements = 100000;
            //In order to load test increase loopCount & totalElements
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
``` 

### Sample code to use without Blocking
```
 PersistentQueue<T> pq = new PersistentQueueBuilder<T>()
                .path(this.path)
                .name(this.name)
                .fileSize(this.initialSize)
                .build();
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
            
```
NOTE: 
  1. Part of this implementation I googled many places to get the desired need but I didn't find single implementation. Bits and pieces available in many places. If I copy any one of your code, please feel free to take credit. I didn't remember all the sites which I browsed while implementing. I really apologize if I don't mention your name.
  2. Also when I implemented there was a urgent need in current company which created some shortcuts in clean interfaces and readability of the code.
