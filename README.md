## Diskbacked BlockingQueue

Diskbacked persistent queue which implements BlockingQueue interface in java. It takes leverage of java MemoryMapped files.

Sample example to use

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
