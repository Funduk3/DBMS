package ru.open.cu.student.execution.executors;


import ru.open.cu.student.catalog.model.TableDefinition;
import ru.open.cu.student.catalog.operation.OperationManager;

import java.util.Iterator;
import java.util.List;

public class SeqScanExecutor implements Executor {

    private final OperationManager operationManager;
    private final TableDefinition tableDefinition;

    private Iterator<List<Object>> iterator;

    public SeqScanExecutor(OperationManager operationManager,
                           TableDefinition tableDefinition) {
        this.operationManager = operationManager;
        this.tableDefinition = tableDefinition;
    }

    @Override
    public void open() {
        List<List<Object>> rows =
                operationManager.selectAll(tableDefinition.getName());
        iterator = rows.iterator();
    }

    @Override
    public Object next() {
        if (iterator != null && iterator.hasNext()) {
            return iterator.next();
        }
        return null;
    }

    @Override
    public void close() {
        iterator = null;
    }
}
