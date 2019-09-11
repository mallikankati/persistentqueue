package com.persistentqueue.storage;

public class SegmentIndexerWithMemoryMappedTest extends AbstractSegmentIndexerTest {

    protected SegmentIndexer getIndexer() {
        SegmentIndexer indexer = new SegmentIndexer();
        indexer.setSegmentType(StorageSegment.SegmentType.MEMORYMAPPED);
        indexer.initialize(this.path, this.name, this.initialSize);
        return indexer;
    }
}
