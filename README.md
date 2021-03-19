## Diskbacked Queue

Diskbacked persistent queue which implements BlockingQueue interface in java. It leverage MemoryMapped files to receive the best performance.

These kind of queue used when it require
   - Low heap memory foot prints, when item stored in queue which stores in memorymapped file backed by disk.
   - Don't lose any events which stored in memory. When events stored in java.util.Queue<>, events get lost on reboot. PersistentQueue will preserve the insertion order with no loss of data.
   - It's a library not require any big distributed messaging like kafka infrastructure. This is not a distributed PersistentQueue, which works in only one JVM
   
### Sample code to use BlockingQueue

```java
PersistentBlockingQueue<Integer> pq = new PersistentQueueBuilder<Integer>()
                .path("valid directory")
                .name("name without space")
                .fileSize(134217728) //128MB 
                .blocking(true)
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
