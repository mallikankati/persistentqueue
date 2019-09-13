package com.persistentqueue;

import com.persistentqueue.storage.AbstractBaseStorageTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

public class PersistentQueueTest extends AbstractBaseStorageTest {

    private <T> PersistentQueue<T> getPersistentQueue(Class<T> typeClass) {
        PersistentQueue<T> pq = new PersistentQueueBuilder<T>()
                .with($ -> {
                    $.path = this.path;
                    $.name = this.name;
                    $.fileSize = this.initialSize;
                    $.typeClass = typeClass;
                   // $.segmentType = StorageSegment.SegmentType.FILE;
                }).build();
        return pq;
    }

    @Test
    public void testBuilder() {
        PersistentQueue<String> pq = getPersistentQueue(String.class);
        pq.close();
    }

    @Test
    public void testAddWithString() {
        PersistentQueue<String> pq = getPersistentQueue(String.class);
        try {
            String prefix = fourKBText;
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
        } finally {
            pq.close();
        }
    }

    @Test
    public void testAddWithInteger() {
        PersistentQueue<Integer> pq = getPersistentQueue(Integer.class);
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
        PersistentQueue<Object> pq = getPersistentQueue(Object.class);
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
        PersistentQueue<Integer> pq = getPersistentQueue(Integer.class);
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
        PersistentQueue<Integer> pq = getPersistentQueue(Integer.class);
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
        PersistentQueue<String> pq = getPersistentQueue(String.class);
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
    public void testAddAllWithInteger() {
        //this.initialSize = 2*1024*1024;
        PersistentQueue<Integer> pq = getPersistentQueue(Integer.class);
        try {
            int totalElements = 500000;
            List<Integer> list = new ArrayList<>();
            for (int i = 0; i < totalElements; i++) {
                list.add(i);
            }
            pq.addAll(list);
            Assert.assertEquals("Total size mismatch", totalElements, pq.size());
            for (int i = 0; i < totalElements; i++) {
                int retrievedInt = pq.poll();
                Assert.assertEquals("Multiple poll call fails results", i, retrievedInt);
            }
            Assert.assertEquals("Total size mismatch", 0, pq.size());
        } finally {
            pq.close();
        }
    }

    @Test
    public void testIterator(){
        PersistentQueue<Integer> pq = getPersistentQueue(Integer.class);
        try {
            int totalElements = 600000;
            for (int i = 0; i < totalElements; i++) {
                pq.add(i);
            }
            Assert.assertEquals("Total size mismatch", totalElements, pq.size());
            Iterator<Integer> it = pq.iterator();
            int j = 0;
            while(it.hasNext()){
                int retrievedInt = it.next();
                Assert.assertEquals("it.next() failed", j, retrievedInt);
                j++;
            }
            for (int i = 0; i < totalElements; i++) {
                int retrievedInt = pq.poll();
                Assert.assertEquals("Multiple poll call fails results", i, retrievedInt);
            }
            Assert.assertEquals("Total size mismatch", 0, pq.size());
        } finally {
            pq.close();
        }
    }

    @Test
    public void testCustomObjects(){
        PersistentQueue<Record> pq = getPersistentQueue(Record.class);
        try {
            int totalElements = 10;
            for (int i = 0; i<totalElements; i++) {
                Record r = Record.createRecord(i);
                pq.add(r);
            }
            Assert.assertEquals("Total size mismatch", totalElements, pq.size());
            for (int i = 0; i<totalElements; i++) {
                Record r = Record.createRecord(i);
                Record retrievedRecord = pq.poll();
                Assert.assertEquals("Records retrieved mismatch", true, r.compare(retrievedRecord));
            }
            Assert.assertEquals("Total size mismatch", 0, pq.size());
        } finally {
            pq.close();
        }
    }

    private static class Record {
        private String name;
        private int value;
        private List<String>  list = new ArrayList<>();
        private Map<String, String> map = new HashMap<>();

        static Record createRecord(int index){
            Record r= new Record();
            r.name = "Testname " + index;
            r.value = 100;
            List<String> list = new ArrayList<>();
            list.add("String " + index);
            list.add("String " + index);
            r.list = list;
            Map<String, String> map = new HashMap<>();
            map.put("Key" + index, "Value" + index);
            map.put("Key" + index +1, "Value" + index +1);
            r.map = map;
            return r;
        }

        boolean compare(Record r){
            boolean status = false;
            if (this.name.equalsIgnoreCase(r.name) &&
                    this.value == r.value &&
                    this.list.size() == r.list.size() &&
                    this.map.size() == r.map.size() &&
                    compareCollections(r.list, r.map)){
                status = true;
            }
            return status;
        }

        boolean compareCollections(List<String> list, Map<String, String> map){
            for (int i = 0; i < list.size(); i++){
                if (!this.list.get(i).equalsIgnoreCase(list.get(i))){
                    return false;
                }
            }
            for (String key : map.keySet()){
                if (!this.map.get(key).equalsIgnoreCase(map.get(key))){
                    return false;
                }
            }
            return true;
        }
    }
}
