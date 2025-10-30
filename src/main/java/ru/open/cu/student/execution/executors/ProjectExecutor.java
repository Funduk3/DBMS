package ru.open.cu.student.execution.executors;

import ru.open.cu.student.ast.AConst;
import ru.open.cu.student.ast.ColumnRef;
import ru.open.cu.student.ast.Expr;
import ru.open.cu.student.ast.TargetEntry;
import ru.open.cu.student.catalog.model.ColumnDefinition;
import ru.open.cu.student.catalog.model.TableDefinition;

import java.util.ArrayList;
import java.util.List;

public class ProjectExecutor implements Executor {

    private final Executor child;
    private final List<TargetEntry> targetList;
    private final TableDefinition tableDefinition;

    public ProjectExecutor(Executor child, List<TargetEntry> targetList, TableDefinition tableDefinition) {
        this.child = child;
        this.targetList = targetList;
        this.tableDefinition = tableDefinition;
    }

    @Override
    public void open() {
        child.open();
    }

    @Override
    public Object next() {
        Object row = child.next();

        if (row == null) {
            return null;
        }

        if (!(row instanceof List)) {
            return null;
        }

        List<Object> rowData = (List<Object>) row;
        List<Object> projectedRow = new ArrayList<>();

        for (TargetEntry te : targetList) {
            Object value = evaluateExpr(te.expr, rowData);
            projectedRow.add(value);
        }

        return projectedRow;
    }

    @Override
    public void close() {
        child.close();
    }

    private Object evaluateExpr(Expr expr, List<Object> rowData) {
        if (expr instanceof AConst aConst) {
            return aConst.value;
        } else if (expr instanceof ColumnRef colRef) {
            return getColumnValue(colRef, rowData);
        }
        return null;
    }

    private Object getColumnValue(ColumnRef colRef, List<Object> rowData) {
        String columnName = colRef.column;
        List<ColumnDefinition> columns = tableDefinition.getColumns();

        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).getName().equals(columnName)) {
                if (i < rowData.size()) {
                    return rowData.get(i);
                }
            }
        }
        return null;
    }
}