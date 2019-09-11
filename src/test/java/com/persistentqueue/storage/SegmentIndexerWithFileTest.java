package com.persistentqueue.storage;

public class SegmentIndexerWithFileTest extends AbstractSegmentIndexerTest {

    protected SegmentIndexer getIndexer() {
        SegmentIndexer indexer = new SegmentIndexer();
        indexer.setSegmentType(StorageSegment.SegmentType.FILE);
        indexer.initialize(this.path, this.name, this.initialSize);
        return indexer;
    }
}
