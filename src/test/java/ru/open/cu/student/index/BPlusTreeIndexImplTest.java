package ru.open.cu.student.index;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.open.cu.student.index.btree.BPlusTreeIndexImpl;
import ru.open.cu.student.memory.manager.HeapPageFileManager;
import ru.open.cu.student.memory.manager.PageFileManager;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class BPlusTreeIndexImplTest {

    @TempDir
    Path tempDir;

    private BPlusTreeIndexImpl index;
    private PageFileManager pageManager;

    @BeforeEach
    void setUp() {
        pageManager = new HeapPageFileManager();
        Path indexFile = tempDir.resolve("index.dat");
        index = new BPlusTreeIndexImpl("test_index", "test_column", 3, pageManager, indexFile);
    }

    @Test
    void testInsertAndSearch_SingleValue() {
        TID tid = new TID(1, (short) 0);

        index.insert(42, tid);
        List<TID> results = index.search(42);

        assertEquals(1, results.size());
        assertEquals(tid, results.get(0));
    }

    @Test
    void testRangeSearch_MultipleValues() {
        TID tid1 = new TID(1, (short) 0);
        TID tid2 = new TID(2, (short) 1);
        TID tid3 = new TID(3, (short) 2);
        TID tid4 = new TID(4, (short) 3);

        index.insert(10, tid1);
        index.insert(20, tid2);
        index.insert(30, tid3);
        index.insert(40, tid4);

        List<TID> results = index.rangeSearch(15, 35, true);

        assertEquals(2, results.size());
        assertTrue(results.contains(tid2));
        assertTrue(results.contains(tid3));
    }

    @Test
    void testScanAll_ReturnsAllInsertedValues() {
        TID tid1 = new TID(1, (short) 0);
        TID tid2 = new TID(2, (short) 1);
        TID tid3 = new TID(3, (short) 2);

        index.insert(5, tid1);
        index.insert(15, tid2);
        index.insert(10, tid3);

        List<TID> results = index.scanAll();

        assertEquals(3, results.size());
        assertTrue(results.contains(tid1));
        assertTrue(results.contains(tid2));
        assertTrue(results.contains(tid3));
    }

    @Test
    void testSearch_NonExistentKey_ReturnsEmptyList() {
        index.insert(10, new TID(1, (short) 0));
        index.insert(20, new TID(2, (short) 1));

        List<TID> results = index.search(99);

        assertTrue(results.isEmpty());
    }

    @Test
    void testRangeSearch_InvalidRange_ReturnsEmptyList() {
        index.insert(10, new TID(1, (short) 0));
        index.insert(20, new TID(2, (short) 1));

        List<TID> results = index.rangeSearch(30, 10, true);

        assertTrue(results.isEmpty());
    }

    @Test
    void testCompareKeys_DifferentTypes_ThrowsException() {
        index.insert(10, new TID(1, (short) 0));

        assertThrows(ClassCastException.class, () -> {
            index.search("string_key");
        });
    }
}
