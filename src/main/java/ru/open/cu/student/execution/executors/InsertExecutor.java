package ru.open.cu.student.execution.executors;


import ru.open.cu.student.catalog.model.TableDefinition;
import ru.open.cu.student.catalog.operation.OperationManager;
import ru.open.cu.student.parser.nodes.AConst;
import ru.open.cu.student.parser.nodes.Expr;

import java.util.List;

public class InsertExecutor implements Executor {

    private final OperationManager operationManager;
    private final TableDefinition tableDefinition;
    private final List<Expr> values;

    public InsertExecutor(OperationManager operationManager, TableDefinition tableDefinition, List<Expr> values) {
        this.operationManager = operationManager;
        this.tableDefinition = tableDefinition;
        this.values = values;
    }

    @Override
    public void open() { }

    @Override
    public Object next() {
        Object[] rowValues = values.stream()
                .map(expr -> {
                    if (expr instanceof AConst ac) {
                        return ac.value;
                    } else {
                        throw new IllegalArgumentException("Expected AConst but found " + expr.getClass().getName() + " : " + expr);
                    }
                })
                .toArray();

        operationManager.insert(tableDefinition.getName(), rowValues);

        return null;
    }

    @Override
    public void close() { }
}