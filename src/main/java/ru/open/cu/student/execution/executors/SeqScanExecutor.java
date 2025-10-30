package ru.open.cu.student.execution.executors;

import ru.open.cu.student.catalog.model.TableDefinition;
import ru.open.cu.student.catalog.operation.OperationManager;

import java.util.Iterator;
import java.util.List;

public class SeqScanExecutor implements Executor {

    private final OperationManager operationManager;
    private final TableDefinition tableDefinition;
    private Iterator<List<Object>> rowIterator;

    public SeqScanExecutor(OperationManager operationManager, TableDefinition tableDefinition) {
        this.operationManager = operationManager;
        this.tableDefinition = tableDefinition;
    }

    @Override
    public void open() {
    }

    @Override
    public Object next() {
        List<List<Object>> allRows = operationManager.selectAll(tableDefinition.getName());
        this.rowIterator = allRows.iterator();
        if (rowIterator.hasNext()) {
            return rowIterator.next();
        }
        return null;
    }

    @Override
    public void close() {
    }
}