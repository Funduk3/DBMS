package ru.open.cu.student.index.hash;

import ru.open.cu.student.catalog.operation.OperationManager;
import ru.open.cu.student.index.IndexType;
import ru.open.cu.student.index.TID;
import ru.open.cu.student.memory.manager.PageFileManager;
import ru.open.cu.student.memory.page.HeapPage;
import ru.open.cu.student.memory.page.Page;

import java.io.*;
import java.nio.file.Path;
import java.util.*;

public class HashIndexImpl implements HashIndex {

    private final String indexName;
    private final String columnName;
    private final PageFileManager pageManager;
    private final OperationManager operationManager;
    private final Path indexPath;

    private static final int MAX_RECORDS_PER_PAGE = 100;
    private static final int RECORD_SIZE = 10;
    private static final int PAGE_HEADER_SIZE = 12;
    private static final int META_PAGE_ID = 0;
    private static final int FIRST_BUCKET_PAGE_ID = 1;
    private static final double SPLIT_THRESHOLD = 0.7;

    private record MetaPage(
            int numBuckets,
            int maxBucket,
            int lowmask,
            int highmask,
            int splitPointer,
            long recordCount
    ) {
    }

    private MetaPage metaPage;
    private final Map<Integer, BucketPage> bucketCache = new HashMap<>();
    private final Map<Integer, BucketPage> overflowCache = new HashMap<>();

    public HashIndexImpl(String indexName,
                         String columnName,
                         PageFileManager pageManager,
                         OperationManager operationManager,
                         Path indexPath) {

        this.indexName = indexName;
        this.columnName = columnName;
        this.pageManager = pageManager;
        this.operationManager = operationManager;
        this.indexPath = indexPath;

        this.metaPage = new MetaPage(
                16,
                15,
                0xF,
                0xF,
                0,
                0L
        );

        initializeIndexFile();
    }

    private void initializeIndexFile() {
        try {
            Page metaDiskPage = new HeapPage(META_PAGE_ID);
            byte[] metaBytes = serializeMetaPage(this.metaPage);
            metaDiskPage.write(metaBytes);
            pageManager.write(metaDiskPage, indexPath);

            for (int i = 0; i < this.metaPage.numBuckets(); i++) {
                int pageId = FIRST_BUCKET_PAGE_ID + i;
                Page bucketDiskPage = new HeapPage(pageId);
                BucketPage bucket = new BucketPage();
                bucket.nextOverflowPageId = -1;
                bucket.freeSpace = HeapPage.PAGE_SIZE - PAGE_HEADER_SIZE;
                byte[] bucketBytes = serializeBucketPage(bucket);
                bucketDiskPage.write(bucketBytes);
                pageManager.write(bucketDiskPage, indexPath);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize hash index file", e);
        }
    }

    @Override
    public void insert(Comparable<?> key, TID tid) {
        int hash = hashFunction(key);
        int bucketId = computeBucket(hash);

        BucketPage bucketPage = readBucketPage(bucketId);
        BucketRecord newRecord = new BucketRecord(hash, tid);
        insertRecordIntoBucket(bucketPage, newRecord);
        writeBucketPage(bucketId, bucketPage);
        checkSplitCondition(bucketId);
    }

    private void insertRecordIntoBucket(BucketPage bucketPage, BucketRecord record) {
        int i = 0;
        while (i < bucketPage.records.size() &&
                bucketPage.records.get(i).hash() < record.hash()) {
            i++;
        }

        bucketPage.records.add(i, record);
        bucketPage.freeSpace -= RECORD_SIZE;

        metaPage = new MetaPage(
                metaPage.numBuckets(),
                metaPage.maxBucket(),
                metaPage.lowmask(),
                metaPage.highmask(),
                metaPage.splitPointer(),
                metaPage.recordCount() + 1
        );
    }

    private void checkSplitCondition(int bucketId) {
        if (bucketId == metaPage.splitPointer()) {
            BucketPage bucketPage = readBucketPage(bucketId);
            int capacity =
                    MAX_RECORDS_PER_PAGE +
                            (bucketPage.records.size() / MAX_RECORDS_PER_PAGE) * MAX_RECORDS_PER_PAGE;
            double fillRatio = (double) bucketPage.records.size() / capacity;

            if (fillRatio > SPLIT_THRESHOLD) {
                performSplit();
            }
        }
    }

    @Override
    public List<TID> search(Comparable<?> key) {
        List<TID> results = new ArrayList<>();
        int hash = hashFunction(key);
        int bucketId = computeBucket(hash);

        BucketPage bucketPage = readBucketPage(bucketId);

        for (BucketRecord record : bucketPage.records) {
            if (record.hash() == hash) {
                results.add(record.tid());
            }
        }

        int currentPageId = bucketPage.nextOverflowPageId;
        while (currentPageId != -1) {
            BucketPage overflowPage = readOverflowPage(currentPageId);
            for (BucketRecord record : overflowPage.records) {
                if (record.hash() == hash) {
                    results.add(record.tid());
                }
            }
            currentPageId = overflowPage.nextOverflowPageId;
        }

        return results;
    }

    @Override
    public List<TID> scanAll() {
        List<TID> allResults = new ArrayList<>();

        for (int bucketId = 0; bucketId <= metaPage.maxBucket(); bucketId++) {
            BucketPage bucketPage = readBucketPage(bucketId);

            for (BucketRecord record : bucketPage.records) {
                allResults.add(record.tid());
            }

            int currentPageId = bucketPage.nextOverflowPageId;
            while (currentPageId != -1) {
                BucketPage overflowPage = readOverflowPage(currentPageId);
                for (BucketRecord record : overflowPage.records) {
                    allResults.add(record.tid());
                }
                currentPageId = overflowPage.nextOverflowPageId;
            }
        }

        return allResults;
    }

    @Override
    public String getName() {
        return indexName;
    }

    @Override
    public String getColumnName() {
        return columnName;
    }

    @Override
    public IndexType getType() {
        return IndexType.HASH;
    }

    @Override
    public int getNumBuckets() {
        return metaPage.numBuckets();
    }

    @Override
    public long getRecordCount() {
        return metaPage.recordCount();
    }

    @Override
    public int getMaxBucket() {
        return metaPage.maxBucket();
    }

    private int hashFunction(Comparable<?> key) {
        if (key == null) return 0;

        if (key instanceof Integer i) return i;
        if (key instanceof Long l) return (int) (l ^ (l >>> 32));
        if (key instanceof String s) return s.hashCode();
        if (key instanceof Double d) return Double.hashCode(d);

        return key.hashCode();
    }

    private int computeBucket(int hash) {
        int bucket = hash & metaPage.highmask();
        if (bucket > metaPage.maxBucket()) {
            bucket = hash & metaPage.lowmask();
        }
        return bucket;
    }

    private void performSplit() {
        int oldBucketId = metaPage.splitPointer();
        int newBucketId = metaPage.maxBucket() + 1;

        List<BucketRecord> allRecords = new ArrayList<>();
        BucketPage oldPrimaryPage = readBucketPage(oldBucketId);
        allRecords.addAll(oldPrimaryPage.records);

        int currentPageId = oldPrimaryPage.nextOverflowPageId;
        while (currentPageId != -1) {
            BucketPage overflowPage = readOverflowPage(currentPageId);
            allRecords.addAll(overflowPage.records);
            currentPageId = overflowPage.nextOverflowPageId;
        }

        BucketPage newPrimaryPage = new BucketPage();
        newPrimaryPage.nextOverflowPageId = -1;
        newPrimaryPage.freeSpace = HeapPage.PAGE_SIZE - PAGE_HEADER_SIZE;

        List<BucketRecord> oldBucketRecords = new ArrayList<>();
        List<BucketRecord> newBucketRecords = new ArrayList<>();

        for (BucketRecord record : allRecords) {
            int bucket = record.hash() & metaPage.highmask();
            if (bucket == newBucketId) {
                newBucketRecords.add(record);
            } else {
                oldBucketRecords.add(record);
            }
        }

        oldPrimaryPage.records = oldBucketRecords;
        oldPrimaryPage.freeSpace =
                HeapPage.PAGE_SIZE - PAGE_HEADER_SIZE - oldBucketRecords.size() * RECORD_SIZE;

        newPrimaryPage.records = newBucketRecords;
        newPrimaryPage.freeSpace =
                HeapPage.PAGE_SIZE - PAGE_HEADER_SIZE - newBucketRecords.size() * RECORD_SIZE;

        writeBucketPage(oldBucketId, oldPrimaryPage);
        writeNewBucketPage(newBucketId, newPrimaryPage);

        int newNumBuckets = metaPage.numBuckets() + 1;
        int newMaxBucket = metaPage.maxBucket() + 1;
        int newSplitPointer = (metaPage.splitPointer() + 1) % (metaPage.maxBucket() + 1);

        int newLowmask = metaPage.lowmask();
        int newHighmask = metaPage.highmask();

        if (newSplitPointer == 0) {
            newLowmask = metaPage.highmask();
            newHighmask = (metaPage.highmask() << 1) | 1;
        }

        metaPage = new MetaPage(
                newNumBuckets,
                newMaxBucket,
                newLowmask,
                newHighmask,
                newSplitPointer,
                metaPage.recordCount()
        );

        saveMetaPage();
    }

    private void saveMetaPage() {
        try {
            Page metaDiskPage;
            try {
                metaDiskPage = pageManager.read(META_PAGE_ID, indexPath);
                if (metaDiskPage == null || !metaDiskPage.isValid()) {
                    metaDiskPage = new HeapPage(META_PAGE_ID);
                }
            } catch (Exception e) {
                metaDiskPage = new HeapPage(META_PAGE_ID);
            }

            byte[] metaBytes = serializeMetaPage(this.metaPage);
            metaDiskPage = new HeapPage(META_PAGE_ID);
            metaDiskPage.write(metaBytes);
            pageManager.write(metaDiskPage, indexPath);
        } catch (Exception e) {
            throw new RuntimeException("Failed to save meta page", e);
        }
    }

    private static class BucketPage {
        int nextOverflowPageId = -1;
        int freeSpace;
        List<BucketRecord> records = new ArrayList<>();
    }

    private record BucketRecord(int hash, TID tid) {
    }

    private BucketPage readBucketPage(int bucketId) {
        if (bucketCache.containsKey(bucketId)) {
            return bucketCache.get(bucketId);
        }

        try {
            int pageId = FIRST_BUCKET_PAGE_ID + bucketId;
            Page page = pageManager.read(pageId, indexPath);
            if (page == null || !page.isValid()) {
                throw new IllegalArgumentException();
            }

            byte[] data = page.read(0);
            BucketPage bucketPage = deserializeBucketPage(data);
            bucketCache.put(bucketId, bucketPage);
            return bucketPage;
        } catch (Exception e) {
            throw new RuntimeException("Failed to read bucket page " + bucketId, e);
        }
    }

    private void writeBucketPage(int bucketId, BucketPage page) {
        try {
            int pageId = FIRST_BUCKET_PAGE_ID + bucketId;
            Page diskPage = new HeapPage(pageId);
            byte[] data = serializeBucketPage(page);
            diskPage.write(data);
            pageManager.write(diskPage, indexPath);
            bucketCache.put(bucketId, page);
        } catch (Exception e) {
            throw new RuntimeException("Failed to write bucket page " + bucketId, e);
        }
    }

    private void writeNewBucketPage(int bucketId, BucketPage page) {
        try {
            int pageId = allocateNewPage();
            Page diskPage = new HeapPage(pageId);
            byte[] data = serializeBucketPage(page);
            diskPage.write(data);
            pageManager.write(diskPage, indexPath);
            bucketCache.put(bucketId, page);
        } catch (Exception e) {
            throw new RuntimeException("Failed to write new bucket page " + bucketId, e);
        }
    }

    private BucketPage readOverflowPage(int pageId) {
        if (overflowCache.containsKey(pageId)) {
            return overflowCache.get(pageId);
        }

        try {
            Page page = pageManager.read(pageId, indexPath);
            if (page == null || !page.isValid()) {
                throw new IllegalArgumentException();
            }

            byte[] data = page.read(0);
            BucketPage bucketPage = deserializeBucketPage(data);
            overflowCache.put(pageId, bucketPage);
            return bucketPage;
        } catch (Exception e) {
            throw new RuntimeException("Failed to read overflow page " + pageId, e);
        }
    }

    private int allocateNewPage() {
        int maxPageId =
                FIRST_BUCKET_PAGE_ID +
                        metaPage.numBuckets() +
                        (int) (metaPage.recordCount() / MAX_RECORDS_PER_PAGE);

        for (int pageId = maxPageId; ; pageId++) {
            try {
                pageManager.read(pageId, indexPath);
            } catch (Exception e) {
                return pageId;
            }
        }
    }

    private byte[] serializeMetaPage(MetaPage meta) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {

            dos.writeInt(meta.numBuckets());
            dos.writeInt(meta.maxBucket());
            dos.writeInt(meta.lowmask());
            dos.writeInt(meta.highmask());
            dos.writeInt(meta.splitPointer());
            dos.writeLong(meta.recordCount());

            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private byte[] serializeBucketPage(BucketPage page) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {

            dos.writeInt(page.nextOverflowPageId);
            dos.writeInt(page.freeSpace);
            dos.writeInt(page.records.size());

            for (BucketRecord record : page.records) {
                dos.writeInt(record.hash());
                dos.writeInt(record.tid().pageId());
                dos.writeShort(record.tid().slotId());
            }

            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private BucketPage deserializeBucketPage(byte[] data) {
        try (DataInputStream dis =
                     new DataInputStream(new ByteArrayInputStream(data))) {

            BucketPage page = new BucketPage();
            page.nextOverflowPageId = dis.readInt();
            page.freeSpace = dis.readInt();
            int recordCount = dis.readInt();

            for (int i = 0; i < recordCount; i++) {
                int hash = dis.readInt();
                int pageId = dis.readInt();
                short slotId = dis.readShort();
                page.records.add(new BucketRecord(hash, new TID(pageId, slotId)));
            }

            return page;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
