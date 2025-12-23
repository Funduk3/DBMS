package ru.open.cu.student.executor;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.open.cu.student.catalog.model.TableDefinition;
import ru.open.cu.student.catalog.operation.OperationManager;
import ru.open.cu.student.execution.executors.HashIndexScanExecutor;
import ru.open.cu.student.index.TID;
import ru.open.cu.student.index.hash.HashIndex;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class HashIndexScanExecutorTest {

    @Mock
    private OperationManager operationManager;

    @Mock
    private HashIndex index;

    @Mock
    private TableDefinition tableDefinition;

    @Test
    void testSearchByKey_ReturnsMatchingRows() {
        TID tid1 = new TID(1, (short) 0);
        TID tid2 = new TID(2, (short) 1);
        List<Object> row1 = Arrays.asList(1L, "Alice");
        List<Object> row2 = Arrays.asList(2L, "Bob");

        when(index.search("key123")).thenReturn(Arrays.asList(tid1, tid2));
        when(tableDefinition.getName()).thenReturn("users");
        when(operationManager.selectRowByTid("users", tid1)).thenReturn(row1);
        when(operationManager.selectRowByTid("users", tid2)).thenReturn(row2);

        HashIndexScanExecutor executor = new HashIndexScanExecutor(
                operationManager, index, "key123", tableDefinition
        );

        executor.open();
        Object result1 = executor.next();
        Object result2 = executor.next();
        Object result3 = executor.next();

        assertEquals(row1, result1);
        assertEquals(row2, result2);
        assertNull(result3);
        verify(index).search("key123");
    }

    @Test
    void testAutoOpen_OpensAutomaticallyOnFirstNext() {
        TID tid = new TID(1, (short) 0);
        List<Object> row = Arrays.asList(1L, "Charlie");

        when(index.search(42)).thenReturn(Collections.singletonList(tid));
        when(tableDefinition.getName()).thenReturn("users");
        when(operationManager.selectRowByTid("users", tid)).thenReturn(row);

        HashIndexScanExecutor executor = new HashIndexScanExecutor(
                operationManager, index, 42, tableDefinition
        );

        Object result = executor.next();

        assertEquals(row, result);
        verify(index).search(42);
    }

    @Test
    void testSkipInvalidTID_ReturnsNextValidRow() {
        TID tidBad = new TID(1, (short) 0);
        TID tidGood = new TID(2, (short) 1);
        List<Object> validRow = Arrays.asList(2L, "Diana");

        when(index.search("key")).thenReturn(Arrays.asList(tidBad, tidGood));
        when(tableDefinition.getName()).thenReturn("users");
        when(operationManager.selectRowByTid("users", tidBad))
                .thenThrow(new RuntimeException("Invalid TID"));
        when(operationManager.selectRowByTid("users", tidGood)).thenReturn(validRow);

        HashIndexScanExecutor executor = new HashIndexScanExecutor(
                operationManager, index, "key", tableDefinition
        );

        Object result = executor.next();

        assertEquals(validRow, result);
    }

    @Test
    void testSearchNonExistentKey_ReturnsNull() {
        when(index.search("nonexistent")).thenReturn(Collections.emptyList());

        HashIndexScanExecutor executor = new HashIndexScanExecutor(
                operationManager, index, "nonexistent", tableDefinition
        );

        executor.open();
        Object result = executor.next();

        assertNull(result);
    }

    @Test
    void testAllTIDsInvalid_ReturnsNull() {
        TID tid1 = new TID(1, (short) 0);
        TID tid2 = new TID(2, (short) 1);

        when(index.search("key")).thenReturn(Arrays.asList(tid1, tid2));
        when(tableDefinition.getName()).thenReturn("users");
        when(operationManager.selectRowByTid(anyString(), any()))
                .thenThrow(new RuntimeException("All TIDs invalid"));

        HashIndexScanExecutor executor = new HashIndexScanExecutor(
                operationManager, index, "key", tableDefinition
        );

        Object result = executor.next();

        assertNull(result);
        verify(operationManager, times(2)).selectRowByTid(anyString(), any());
    }
}
