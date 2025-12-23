package ru.open.cu.student.index;

import org.junit.jupiter.api.*;
import ru.open.cu.student.catalog.manager.CatalogManagerImpl;
import ru.open.cu.student.catalog.operation.OperationManager;
import ru.open.cu.student.catalog.operation.OperationManagerImpl;
import ru.open.cu.student.index.hash.HashIndex;
import ru.open.cu.student.index.hash.HashIndexImpl;
import ru.open.cu.student.memory.buffer.DefaultBufferPoolManager;
import ru.open.cu.student.memory.manager.HeapPageFileManager;
import ru.open.cu.student.memory.manager.PageFileManager;
import ru.open.cu.student.memory.replacer.ClockReplacer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HashIndexImplTest {

    private PageFileManager pageManager;
    private CatalogManagerImpl catalogManager;
    private OperationManager operationManager;
    private Path testIndexPath;

    @BeforeAll
    void setupOnce() {
        pageManager = new HeapPageFileManager();
        catalogManager = new CatalogManagerImpl(
                new DefaultBufferPoolManager(16, new HeapPageFileManager(), new ClockReplacer(16)),
                new HeapPageFileManager()
        );
        operationManager = new OperationManagerImpl(catalogManager, pageManager);
    }

    @BeforeEach
    void setup() throws IOException {
        cleanupTestData();
        testIndexPath = Paths.get("data", "test_hash_index");
        Files.createDirectories(testIndexPath.getParent());
    }

    @AfterEach
    void cleanup() throws IOException {
        cleanupTestData();
    }

    private void cleanupTestData() throws IOException {
        Path dataDir = Paths.get("data");
        if (Files.exists(dataDir)) {
            try (var paths = Files.walk(dataDir)) {
                paths.sorted(Comparator.reverseOrder())
                        .forEach(path -> {
                            try {
                                Files.delete(path);
                            } catch (IOException ignored) {
                            }
                        });
            }
        }
    }

    @Test
    void testIndexCreation() {
        HashIndex index = new HashIndexImpl(
                "test_index",
                "test_column",
                pageManager,
                operationManager,
                testIndexPath
        );

        assertEquals("test_index", index.getName());
        assertEquals("test_column", index.getColumnName());
        assertEquals(IndexType.HASH, index.getType());
        assertEquals(16, index.getNumBuckets());
        assertEquals(15, index.getMaxBucket());
        assertEquals(0, index.getRecordCount());
    }

    @Test
    void testInsertAndSearchSingleRecord() {
        HashIndex index = new HashIndexImpl(
                "test_index",
                "id",
                pageManager,
                operationManager,
                testIndexPath
        );

        TID tid = new TID(0, (short) 0);
        index.insert(100, tid);

        assertEquals(1, index.getRecordCount());

        List<TID> results = index.search(100);
        assertEquals(1, results.size());
        assertEquals(tid, results.get(0));
    }

    @Test
    void testInsertMultipleRecords() {
        HashIndex index = new HashIndexImpl(
                "test_index",
                "id",
                pageManager,
                operationManager,
                testIndexPath
        );

        TID tid1 = new TID(0, (short) 0);
        TID tid2 = new TID(0, (short) 1);
        TID tid3 = new TID(0, (short) 2);

        index.insert(100, tid1);
        index.insert(200, tid2);
        index.insert(300, tid3);

        assertEquals(3, index.getRecordCount());

        assertEquals(tid1, index.search(100).get(0));
        assertEquals(tid2, index.search(200).get(0));
        assertEquals(tid3, index.search(300).get(0));
    }

    @Test
    void testInsertDuplicates() {
        HashIndex index = new HashIndexImpl(
                "test_index",
                "id",
                pageManager,
                operationManager,
                testIndexPath
        );

        TID tid1 = new TID(0, (short) 0);
        TID tid2 = new TID(0, (short) 1);
        TID tid3 = new TID(1, (short) 0);

        index.insert(100, tid1);
        index.insert(100, tid2);
        index.insert(100, tid3);

        assertEquals(3, index.getRecordCount());

        List<TID> results = index.search(100);
        assertEquals(3, results.size());
        assertTrue(results.contains(tid1));
        assertTrue(results.contains(tid2));
        assertTrue(results.contains(tid3));
    }

    @Test
    void testScanAll() {
        HashIndex index = new HashIndexImpl(
                "test_index",
                "id",
                pageManager,
                operationManager,
                testIndexPath
        );

        List<TID> insertedTids = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            TID tid = new TID(0, (short) i);
            index.insert(1000 + i, tid);
            insertedTids.add(tid);
        }

        List<TID> allResults = index.scanAll();
        assertEquals(20, allResults.size());

        for (TID tid : insertedTids) {
            assertTrue(allResults.contains(tid));
        }
    }

    @Test
    void testIntegerKeys() {
        HashIndex index = new HashIndexImpl(
                "test_index",
                "int_column",
                pageManager,
                operationManager,
                testIndexPath
        );

        TID tid1 = new TID(0, (short) 0);
        TID tid2 = new TID(0, (short) 1);

        index.insert(42, tid1);
        index.insert(-100, tid2);

        assertEquals(tid1, index.search(42).get(0));
        assertEquals(tid2, index.search(-100).get(0));
    }

    @Test
    void testLongKeys() {
        HashIndex index = new HashIndexImpl(
                "test_index",
                "long_column",
                pageManager,
                operationManager,
                testIndexPath
        );

        TID tid1 = new TID(0, (short) 0);
        TID tid2 = new TID(0, (short) 1);

        index.insert(1234567890L, tid1);
        index.insert(9876543210L, tid2);

        assertEquals(tid1, index.search(1234567890L).get(0));
        assertEquals(tid2, index.search(9876543210L).get(0));
    }

    @Test
    void testStringKeys() {
        HashIndex index = new HashIndexImpl(
                "test_index",
                "string_column",
                pageManager,
                operationManager,
                testIndexPath
        );

        TID tid1 = new TID(0, (short) 0);
        TID tid2 = new TID(0, (short) 1);

        index.insert("hello", tid1);
        index.insert("world", tid2);

        assertEquals(tid1, index.search("hello").get(0));
        assertEquals(tid2, index.search("world").get(0));
    }

    @Test
    void testSearchNonExistentKey() {
        HashIndex index = new HashIndexImpl(
                "test_index",
                "id",
                pageManager,
                operationManager,
                testIndexPath
        );

        index.insert(100, new TID(0, (short) 0));

        assertTrue(index.search(999).isEmpty());
    }

    @Test
    void testSearchInEmptyIndex() {
        HashIndex index = new HashIndexImpl(
                "test_index",
                "id",
                pageManager,
                operationManager,
                testIndexPath
        );

        assertTrue(index.search(100).isEmpty());
    }

    @Test
    void testScanEmptyIndex() {
        HashIndex index = new HashIndexImpl(
                "test_index",
                "id",
                pageManager,
                operationManager,
                testIndexPath
        );

        assertTrue(index.scanAll().isEmpty());
    }

    @Test
    void testInsertNullKey() {
        HashIndex index = new HashIndexImpl(
                "test_index",
                "id",
                pageManager,
                operationManager,
                testIndexPath
        );

        TID tid = new TID(0, (short) 0);

        assertDoesNotThrow(() -> index.insert(null, tid));

        List<TID> results = index.search(null);
        assertEquals(1, results.size());
        assertEquals(tid, results.get(0));
    }

    @Test
    void testSearchNullKeyInNonEmptyIndex() {
        HashIndex index = new HashIndexImpl(
                "test_index",
                "id",
                pageManager,
                operationManager,
                testIndexPath
        );

        index.insert(100, new TID(0, (short) 0));
        index.insert(200, new TID(0, (short) 1));

        assertTrue(index.search(null).isEmpty());
    }

    @Test
    void testInsertSameTID() {
        HashIndex index = new HashIndexImpl(
                "test_index",
                "id",
                pageManager,
                operationManager,
                testIndexPath
        );

        TID tid = new TID(5, (short) 10);

        index.insert(100, tid);
        index.insert(200, tid);

        assertEquals(2, index.getRecordCount());
        assertEquals(tid, index.search(100).get(0));
        assertEquals(tid, index.search(200).get(0));
    }

    @Test
    void testInsertManyDuplicatesInOneBucket() {
        HashIndex index = new HashIndexImpl(
                "test_index",
                "id",
                pageManager,
                operationManager,
                testIndexPath
        );

        int key = 12345;

        for (int i = 0; i < 50; i++) {
            index.insert(key, new TID(i, (short) 0));
        }

        assertEquals(50, index.getRecordCount());
        assertEquals(50, index.search(key).size());
    }

    @Test
    void testHashCollisions() {
        HashIndex index = new HashIndexImpl(
                "test_index",
                "id",
                pageManager,
                operationManager,
                testIndexPath
        );

        List<Integer> keys = List.of(0, 16, 32, 48, 64);

        for (int i = 0; i < keys.size(); i++) {
            index.insert(keys.get(i), new TID(0, (short) i));
        }

        for (Integer key : keys) {
            assertEquals(1, index.search(key).size());
        }
    }

    @Test
    void testInsertNegativeNumbers() {
        HashIndex index = new HashIndexImpl(
                "test_index",
                "id",
                pageManager,
                operationManager,
                testIndexPath
        );

        TID tid1 = new TID(0, (short) 0);
        TID tid2 = new TID(0, (short) 1);
        TID tid3 = new TID(0, (short) 2);

        index.insert(-100, tid1);
        index.insert(-1, tid2);
        index.insert(Integer.MIN_VALUE, tid3);

        assertEquals(tid1, index.search(-100).get(0));
        assertEquals(tid2, index.search(-1).get(0));
        assertEquals(tid3, index.search(Integer.MIN_VALUE).get(0));
    }

    @Test
    void testInsertMaxValues() {
        HashIndex index = new HashIndexImpl(
                "test_index",
                "id",
                pageManager,
                operationManager,
                testIndexPath
        );

        TID tid1 = new TID(0, (short) 0);
        TID tid2 = new TID(Integer.MAX_VALUE, Short.MAX_VALUE);

        index.insert(Integer.MAX_VALUE, tid1);
        index.insert(Long.MAX_VALUE, tid2);

        assertEquals(tid1, index.search(Integer.MAX_VALUE).get(0));
        assertEquals(tid2, index.search(Long.MAX_VALUE).get(0));
    }
}
