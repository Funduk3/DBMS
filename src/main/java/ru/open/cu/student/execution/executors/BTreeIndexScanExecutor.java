package ru.open.cu.student.execution.executors;

import ru.open.cu.student.catalog.operation.OperationManager;
import ru.open.cu.student.index.TID;
import ru.open.cu.student.catalog.model.TableDefinition;
import ru.open.cu.student.index.btree.BPlusTreeIndex;

import java.util.Iterator;
import java.util.List;

public class BTreeIndexScanExecutor implements Executor {
    private final OperationManager operationManager;
    private final BPlusTreeIndex index;
    private final TableDefinition tableDefinition;
    private final Comparable<?> rangeFrom;
    private final Comparable<?> rangeTo;
    private final boolean hasRange;
    private final Comparable<?> value;

    private Iterator<TID> tidIterator;
    private List<TID> tids;
    private boolean isOpen;

    public BTreeIndexScanExecutor(OperationManager operationManager,
                                  BPlusTreeIndex index,
                                  Comparable<?> value,
                                  TableDefinition tableDefinition) {
        this.operationManager = operationManager;
        this.index = index;
        this.tableDefinition = tableDefinition;
        this.value = value;
        this.hasRange = false;
        this.rangeFrom = null;
        this.rangeTo = null;
        this.tidIterator = null;
        this.isOpen = false;
    }

    public BTreeIndexScanExecutor(OperationManager operationManager,
                                  BPlusTreeIndex index,
                                  Comparable<?> rangeFrom,
                                  Comparable<?> rangeTo,
                                  TableDefinition tableDefinition) {
        this.operationManager = operationManager;
        this.index = index;
        this.tableDefinition = tableDefinition;
        this.rangeFrom = rangeFrom;
        this.rangeTo = rangeTo;
        this.hasRange = true;
        this.value = null;
        this.tidIterator = null;
        this.isOpen = false;
    }

    @Override
    public void open() {
        if (isOpen) {
            return;
        }

        if (hasRange) {
            boolean inclusive = true;
            tids = index.rangeSearch(rangeFrom, rangeTo, inclusive);
        } else {
            tids = index.search(value);
        }

        tidIterator = tids.iterator();
        isOpen = true;
    }

    @Override
    public Object next() {
        if (!isOpen) {
            open();
        }

        if (tidIterator != null && tidIterator.hasNext()) {
            TID tid = tidIterator.next();
            return operationManager.selectRowByTid(tableDefinition.getName(), tid);
        }
        return null;
    }

    @Override
    public void close() {
        tidIterator = null;
        tids = null;
        isOpen = false;
    }
}