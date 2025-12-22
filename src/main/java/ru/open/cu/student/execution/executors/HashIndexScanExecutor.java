package ru.open.cu.student.execution.executors;

import ru.open.cu.student.catalog.operation.OperationManager;
import ru.open.cu.student.index.TID;
import ru.open.cu.student.index.hash.HashIndex;
import ru.open.cu.student.catalog.model.TableDefinition;

import java.util.Iterator;
import java.util.List;

public class HashIndexScanExecutor implements Executor {

    private final OperationManager operationManager;
    private final HashIndex index;
    private final Comparable<?> searchKey;
    private final TableDefinition tableDef;

    private Iterator<TID> tidIterator;
    private boolean isOpen;

    public HashIndexScanExecutor(OperationManager operationManager,
                                 HashIndex index,
                                 Comparable<?> searchKey,
                                 TableDefinition tableDef) {
        this.operationManager = operationManager;
        this.index = index;
        this.searchKey = searchKey;
        this.tableDef = tableDef;
    }

    @Override
    public void open() {
        if (isOpen) return;

        List<TID> tids = index.search(searchKey);
        this.tidIterator = tids.iterator();
        this.isOpen = true;
    }

    @Override
    public Object next() {
        if (!isOpen) {
            open();
        }

        if (tidIterator == null || !tidIterator.hasNext()) {
            return null;
        }

        TID tid = tidIterator.next();
        try {
            return operationManager.selectRowByTid(tableDef.getName(), tid);
        } catch (Exception e) {
            System.err.println("Warning: Failed to read row at TID " + tid + ": " + e.getMessage());
            return next();
        }
    }

    @Override
    public void close() {
        tidIterator = null;
        isOpen = false;
    }
}