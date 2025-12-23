package ru.open.cu.student.executor;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.open.cu.student.catalog.model.TableDefinition;
import ru.open.cu.student.catalog.operation.OperationManager;
import ru.open.cu.student.execution.executors.BTreeIndexScanExecutor;
import ru.open.cu.student.index.TID;
import ru.open.cu.student.index.btree.BPlusTreeIndex;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class BTreeIndexScanExecutorTest {

    @Mock
    private OperationManager operationManager;

    @Mock
    private BPlusTreeIndex index;

    @Mock
    private TableDefinition tableDefinition;

    @Test
    void testSingleValueSearch_ReturnsMatchingRows() {
        TID tid1 = new TID(1, (short) 0);
        TID tid2 = new TID(2, (short) 1);
        List<TID> tids = Arrays.asList(tid1, tid2);

        List<Object> row1 = Arrays.asList(1L, "Alice");
        List<Object> row2 = Arrays.asList(2L, "Bob");

        when(index.search(42)).thenReturn(tids);
        when(tableDefinition.getName()).thenReturn("test_table");
        when(operationManager.selectRowByTid("test_table", tid1)).thenReturn(row1);
        when(operationManager.selectRowByTid("test_table", tid2)).thenReturn(row2);

        BTreeIndexScanExecutor executor = new BTreeIndexScanExecutor(
                operationManager, index, 42, tableDefinition
        );

        executor.open();
        Object result1 = executor.next();
        Object result2 = executor.next();
        Object result3 = executor.next();

        assertEquals(row1, result1);
        assertEquals(row2, result2);
        assertNull(result3);
        verify(index).search(42);
    }

    @Test
    void testRangeSearch_ReturnsRowsInRange() {
        TID tid1 = new TID(1, (short) 0);
        TID tid2 = new TID(2, (short) 1);
        List<TID> tids = Arrays.asList(tid1, tid2);

        List<Object> row1 = Arrays.asList(20L, "Charlie");
        List<Object> row2 = Arrays.asList(30L, "Diana");

        when(index.rangeSearch(10, 50, true)).thenReturn(tids);
        when(tableDefinition.getName()).thenReturn("test_table");
        when(operationManager.selectRowByTid("test_table", tid1)).thenReturn(row1);
        when(operationManager.selectRowByTid("test_table", tid2)).thenReturn(row2);

        BTreeIndexScanExecutor executor = new BTreeIndexScanExecutor(
                operationManager, index, 10, 50, tableDefinition
        );

        executor.open();
        Object result1 = executor.next();
        Object result2 = executor.next();

        assertEquals(row1, result1);
        assertEquals(row2, result2);
        verify(index).rangeSearch(10, 50, true);
    }

    @Test
    void testAutoOpen_OpensOnFirstNext() {
        TID tid = new TID(1, (short) 0);
        List<Object> row = Arrays.asList(1L, "Eve");

        when(index.search(42)).thenReturn(Collections.singletonList(tid));
        when(tableDefinition.getName()).thenReturn("test_table");
        when(operationManager.selectRowByTid("test_table", tid)).thenReturn(row);

        BTreeIndexScanExecutor executor = new BTreeIndexScanExecutor(
                operationManager, index, 42, tableDefinition
        );

        Object result = executor.next();

        assertEquals(row, result);
        verify(index).search(42);
    }

    @Test
    void testNoResults_ReturnsNull() {
        when(index.search(99)).thenReturn(Collections.emptyList());

        BTreeIndexScanExecutor executor = new BTreeIndexScanExecutor(
                operationManager, index, 99, tableDefinition
        );

        executor.open();
        Object result = executor.next();

        assertNull(result);
    }

    @Test
    void testCloseAndReopen_ResetsIterator() {
        TID tid = new TID(1, (short) 0);
        List<Object> row = Arrays.asList(1L, "Grace");

        when(index.search(42)).thenReturn(Collections.singletonList(tid));
        when(tableDefinition.getName()).thenReturn("test_table");
        when(operationManager.selectRowByTid("test_table", tid)).thenReturn(row);

        BTreeIndexScanExecutor executor = new BTreeIndexScanExecutor(
                operationManager, index, 42, tableDefinition
        );

        executor.open();
        Object firstResult = executor.next();
        Object secondResult = executor.next();
        executor.close();

        Object resultAfterReopen = executor.next();

        assertEquals(row, firstResult);
        assertNull(secondResult);
        assertEquals(row, resultAfterReopen);
        verify(index, times(2)).search(42);
    }
}
