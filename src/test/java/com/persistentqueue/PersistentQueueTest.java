package com.persistentqueue;

import com.persistentqueue.storage.AbstractBaseStorageTest;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;

public class PersistentQueueTest extends AbstractBaseStorageTest {

    private static final Logger logger = LoggerFactory.getLogger(PersistentQueueTest.class);

    private <T> PersistentQueue<T> getPersistentQueue() {
        /*PersistentQueue<T> pq = new PersistentQueueBuilder<T>()
                .with($ -> {
                    $.path = this.path;
                    $.name = this.name;
                    $.fileSize = this.initialSize;
                    $.typeClass = typeClass;
                   // $.segmentType = StorageSegment.SegmentType.FILE;
                }).build();
        */
        PersistentQueue<T> pq = new PersistentQueueBuilder<T>()
                .path(this.path)
                .name(this.name)
                .fileSize(this.initialSize)
                .build();
        return pq;
    }

    @Test
    public void testBuilder() {
        PersistentQueue<String> pq = getPersistentQueue();
        pq.close();
    }

    @Test
    public void testAddWithString() {
        PersistentQueue<String> pq = getPersistentQueue();
        try {
            String prefix = oneKBText;
            int totalElements = 10;
            for (int i = 0; i < totalElements; i++) {
                pq.add(prefix + i);
            }
            Assert.assertEquals("Total size mismatch", totalElements, pq.size());
            for (int i = 0; i < totalElements; i++) {
                String str = prefix + 0;
                String tempStr = pq.peek();
                Assert.assertEquals("Multiple peek call returning different results", str, tempStr);
            }
            Assert.assertEquals("Total size mismatch", totalElements, pq.size());
            for (int i = 0; i < totalElements; i++) {
                String str = prefix + i;
                String tempStr = pq.poll();
                Assert.assertEquals("Multiple poll call fails results", str, tempStr);
            }
            Assert.assertEquals("Total size mismatch after poll", 0, pq.size());
        } catch (Exception e) {
            logger.info(e.getMessage(), e);
        } finally {
            pq.close();
        }
    }

    @Test
    public void testAddWithInteger() {
        PersistentQueue<Integer> pq = getPersistentQueue();
        try {
            int totalElements = 10;
            for (int i = 0; i < totalElements; i++) {
                pq.add(i);
            }
            Assert.assertEquals("Total size mismatch", totalElements, pq.size());
            for (int i = 0; i < totalElements; i++) {
                int tempInt = pq.peek();
                Assert.assertEquals("Multiple peek call returning different results", 0, tempInt);
            }
            Assert.assertEquals("Total size mismatch", totalElements, pq.size());
            for (int i = 0; i < totalElements; i++) {
                int tempInt = pq.poll();
                Assert.assertEquals("Multiple poll call fails results", i, tempInt);
            }
            Assert.assertEquals("Total size mismatch after poll", 0, pq.size());
        } finally {
            pq.close();
        }
    }

    //@Test
    //TODO this needs be tested
    public void testAddWithListOfStrings() {
        //PersistentQueue<List<String>> pq = getPersistentQueue((Class<List<String>>)new ArrayList<String>().getClass());
        PersistentQueue<Object> pq = getPersistentQueue();
        try {
            List<String> list = new ArrayList<>();
            list.add("string 1");
            list.add("string 2");
            list.add("string 3");
            pq.add(list);
            Assert.assertEquals("Total size mismatch", 1, pq.size());
            List<String> retrievedList = (List<String>) pq.peek();
            Assert.assertEquals("Total size mismatch after peek", 1, pq.size());
            Assert.assertEquals("List size mismatch after peek", list.size(), retrievedList.size());
            for (int i = 0; i < list.size(); i++) {
                Assert.assertEquals("List elements are not matching after peek", list.get(i), retrievedList.get(i));
            }
            retrievedList = (List<String>) pq.poll();
            Assert.assertEquals("Total size mismatch after poll", 0, pq.size());
            Assert.assertEquals("List size mismatch after poll", list.size(), retrievedList.size());
            for (int i = 0; i < list.size(); i++) {
                Assert.assertEquals("List elements are not matching after poll", list.get(i), retrievedList.get(i));
            }
        } finally {
            pq.close();
        }
    }

    @Test
    public void testRemove() {
        PersistentQueue<Integer> pq = getPersistentQueue();
        try {
            int totalElements = 10;
            for (int i = 0; i < totalElements; i++) {
                pq.add(i);
            }
            Assert.assertEquals("Total size mismatch", totalElements, pq.size());
            for (int i = 0; i < totalElements; i++) {
                int temp = pq.remove();
                Assert.assertEquals("Elements mismatch", i, temp);
            }
            boolean thrown = false;
            try {
                pq.remove();
            } catch (NoSuchElementException e) {
                thrown = true;
            }
            Assert.assertEquals("Expecting NoSuchElementException in remove", true, thrown);
            Integer temp = pq.poll();
            Assert.assertEquals("Expecting null value with poll", null, temp);
            temp = pq.peek();
            Assert.assertEquals("Expecting null value with peek", null, temp);
            thrown = false;
            try {
                pq.element();
            } catch (NoSuchElementException e) {
                thrown = true;
            }
            Assert.assertEquals("Expecting NoSuchElementException in element()", true, thrown);
        } finally {
            pq.close();
        }
    }

    @Test
    public void testClear() {
        PersistentQueue<Integer> pq = getPersistentQueue();
        try {
            int totalElements = 10;
            for (int i = 0; i < totalElements; i++) {
                pq.add(i);
            }
            Assert.assertEquals("Total size mismatch", totalElements, pq.size());
            pq.clear();
            Assert.assertEquals("Total size mismatch after clear", 0, pq.size());
            for (int i = 0; i < totalElements; i++) {
                pq.add(i);
            }
            Assert.assertEquals("Total size mismatch", totalElements, pq.size());
            for (int i = 0; i < totalElements; i++) {
                int tempInt = pq.poll();
                Assert.assertEquals("Multiple poll call fails results", i, tempInt);
            }
            Assert.assertEquals("Total size mismatch after poll", 0, pq.size());
        } finally {
            pq.close();
        }
    }

    @Test
    public void testAddAllWithString() {
        PersistentQueue<String> pq = getPersistentQueue();
        try {
            int totalElements = 1000;
            List<String> list = new ArrayList<>();
            for (int i = 0; i < totalElements; i++) {
                list.add(threeKBText + i);
            }
            pq.addAll(list);
            Assert.assertEquals("Total size mismatch", totalElements, pq.size());
            for (int i = 0; i < totalElements; i++) {
                String tempStr = threeKBText + i;
                String retrievedStr = pq.poll();
                Assert.assertEquals("Multiple poll call fails results", tempStr, retrievedStr);
            }
        } finally {
            pq.close();
        }
    }

    @Test
    public void testAddWith1KBString() {
        int originalSize = this.initialSize;
        this.initialSize = 64*1024*1024;
        PersistentQueue<String> pq = getPersistentQueue();
        boolean loadTest = false;
        try {
            int loopCount = 1;
            int totalElements = 10000;
            if (loadTest){
                loopCount = 1000;
                totalElements = 1000000;
            }
            for (int j = 0; j < loopCount; j++) {
                logger.info("iteration :" + j);


                logger.info("Total elements:" + totalElements);
                List<String> list = new ArrayList<>();
                long startTime = System.currentTimeMillis();
                for (int i = 0; i < totalElements; i++) {
                    pq.add(oneKBText + i);
                }
                long endTime = System.currentTimeMillis();
                logger.info("Total time for add:" + (endTime - startTime) + "millis");
                Assert.assertEquals("Total size mismatch", totalElements, pq.size());
                startTime = System.currentTimeMillis();
                for (int i = 0; i < totalElements; i++) {
                    String tempStr = oneKBText + i;
                    String retrievedStr = pq.poll();
                    Assert.assertEquals("Multiple poll call fails results", tempStr, retrievedStr);
                }
                endTime = System.currentTimeMillis();
                logger.info("Total time for poll:" + (endTime - startTime) + "millis");
            }
        } finally {
            pq.close();
            this.initialSize = originalSize;
        }
    }

    @Test
    public void testAddAllWithInteger() {
        //this.initialSize = 2*1024*1024;
        PersistentQueue<Integer> pq = getPersistentQueue();
        try {
            int totalElements = 25000;
            logger.info("Total elements :" + totalElements);
            List<Integer> list = new ArrayList<>();
            for (int i = 0; i < totalElements; i++) {
                list.add(i);
            }
            long startTime = System.currentTimeMillis();
            pq.addAll(list);
            long endTime = System.currentTimeMillis();
            logger.info("Total time for addAll:" + (endTime-startTime) + "millis");
            Assert.assertEquals("Total size mismatch", totalElements, pq.size());
            startTime = System.currentTimeMillis();
            for (int i = 0; i < totalElements; i++) {
                int retrievedInt = pq.poll();
                Assert.assertEquals("Multiple poll call fails results", i, retrievedInt);
            }
            endTime = System.currentTimeMillis();
            logger.info("Total time for poll:" + (endTime-startTime) + "millis");
            Assert.assertEquals("Total size mismatch", 0, pq.size());
        } finally {
            pq.close();
        }
    }

    @Test
    public void testIterator() {
        int originalSize = this.initialSize;
        this.initialSize = 5 * 1024 * 1024;
        PersistentQueue<Integer> pq = getPersistentQueue();
        try {
            int totalElements = 1000000;
            logger.info("Total elements :" + totalElements);
            long startTime = System.currentTimeMillis();
            for (int i = 0; i < totalElements; i++) {
                pq.add(i);
            }
            long endTime = System.currentTimeMillis();
            logger.info("Total time for add:" + (endTime-startTime) + "millis");
            Assert.assertEquals("Total size mismatch", totalElements, pq.size());
            Iterator<Integer> it = pq.iterator();
            int j = 0;
            startTime = System.currentTimeMillis();
            while (it.hasNext()) {
                int retrievedInt = it.next();
                Assert.assertEquals("it.next() failed", j, retrievedInt);
                j++;
            }
            endTime = System.currentTimeMillis();
            logger.info("Total time for iterator :" + (endTime-startTime) + "millis");
            startTime = System.currentTimeMillis();
            for (int i = 0; i < totalElements; i++) {
                int retrievedInt = pq.poll();
                Assert.assertEquals("Multiple poll call fails results", i, retrievedInt);
            }
            endTime = System.currentTimeMillis();
            logger.info("Total time for poll :" + (endTime-startTime) + "millis");

            Assert.assertEquals("Total size mismatch", 0, pq.size());
        } finally {
            pq.close();
            this.initialSize = originalSize;
        }
    }

    @Test
    public void testSimpleJdkQueue() {
        Queue<Integer> queue = new LinkedList<>();
        int totalElements = 500000;
        for (int i = 0; i < totalElements; i++) {
            queue.add(i);
        }
        Assert.assertEquals("Total size mismatch", totalElements, queue.size());
        Iterator<Integer> it = queue.iterator();
        int j = 0;
        while (it.hasNext()) {
            int retrievedInt = it.next();
            Assert.assertEquals("it.next() failed", j, retrievedInt);
            j++;
        }
        for (int i = 0; i < totalElements; i++) {
            int retrievedInt = queue.poll();
            Assert.assertEquals("Multiple poll call fails results", i, retrievedInt);
        }
        Assert.assertEquals("Total size mismatch", 0, queue.size());

    }

    @Test
    public void testCustomObjects() {
        PersistentQueue<Record> pq = getPersistentQueue();
        try {
            int totalElements = 10000;
            for (int i = 0; i < totalElements; i++) {
                Record r = Record.createRecord(i);
                pq.add(r);
            }
            Assert.assertEquals("Total size mismatch", totalElements, pq.size());
            for (int i = 0; i < totalElements; i++) {
                Record r = Record.createRecord(i);
                Record retrievedRecord = pq.poll();
                Assert.assertEquals("Records retrieved mismatch", true, r.compare(retrievedRecord));
            }
            Assert.assertEquals("Total size mismatch", 0, pq.size());
        } finally {
            pq.close();
        }
    }

    private static class Record implements Externalizable {
        private String name;
        private int value;
        private List<String> list = new ArrayList<>();
        private Map<String, String> map = new HashMap<>();
        private ChildRecord childRecord;
        public Record(){
        }

        static Record createRecord(int index) {
            Record r = new Record();
            r.name = "Testname " + index;
            r.value = 100;
            List<String> list = new ArrayList<>();
            list.add("String " + index);
            list.add("String " + index);
            r.list = list;
            Map<String, String> map = new HashMap<>();
            map.put("Key" + index, "Value" + index);
            map.put("Key" + index + 1, "Value" + index + 1);
            r.map = map;
            r.childRecord = ChildRecord.createChildRecord("child1",1);
            return r;
        }

        boolean compare(Record r) {
            boolean status = false;
            if (this.name.equalsIgnoreCase(r.name) &&
                    this.value == r.value &&
                    this.list.size() == r.list.size() &&
                    this.map.size() == r.map.size() &&
                    compareCollections(r.list, r.map)) {
                status = true;
            }
            return status;
        }

        boolean compareCollections(List<String> list, Map<String, String> map) {
            for (int i = 0; i < list.size(); i++) {
                if (!this.list.get(i).equalsIgnoreCase(list.get(i))) {
                    return false;
                }
            }
            for (String key : map.keySet()) {
                if (!this.map.get(key).equalsIgnoreCase(map.get(key))) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(this.name);
            out.writeInt(value);
            out.writeObject(this.list);
            out.writeObject(map);
            out.writeObject(childRecord);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            this.name = (String)in.readObject();
            this.value = in.readInt();
            this.list = (List<String>)in.readObject();
            this.map = (Map<String, String>)in.readObject();
            this.childRecord = (ChildRecord)in.readObject();
        }
    }

    private static class ChildRecord implements Externalizable{
        private String childName;
        private int childValue;

        public ChildRecord(){
        }

        private static ChildRecord createChildRecord(String name, int value){
            ChildRecord cr = new ChildRecord();
            cr.childName = name;
            cr.childValue = value;
            return cr;
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(this.childName);
            out.writeInt(this.childValue);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            this.childName = (String)in.readObject();
            this.childValue = in.readInt();
        }
    }
}
