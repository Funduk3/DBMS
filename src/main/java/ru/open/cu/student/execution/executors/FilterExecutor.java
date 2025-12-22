package ru.open.cu.student.execution.executors;

import ru.open.cu.student.catalog.model.ColumnDefinition;
import ru.open.cu.student.catalog.model.TableDefinition;
import ru.open.cu.student.parser.nodes.AConst;
import ru.open.cu.student.parser.nodes.AExpr;
import ru.open.cu.student.parser.nodes.ColumnRef;
import ru.open.cu.student.parser.nodes.Expr;

import java.util.List;

public class FilterExecutor implements Executor {

    private final Executor child;
    private final Expr predicate;
    private final TableDefinition tableDefinition;

    public FilterExecutor(Executor child, Expr predicate, TableDefinition tableDefinition) {
        this.child = child;
        this.predicate = predicate;
        this.tableDefinition = tableDefinition;
    }

    @Override
    public void open() {
        child.open();
    }

    @Override
    public Object next() {
        Object row;
        while ((row = child.next()) != null) {
            if (evaluatePredicate(row, predicate)) {
                return row;
            }
        }
        return null;
    }

    @Override
    public void close() {
        child.close();
    }

    private boolean evaluatePredicate(Object row, Expr expr) {
        if (!(row instanceof List)) {
            return false;
        }

        List<Object> rowData = (List<Object>) row;

        if (expr instanceof AExpr aExpr) {
            Object leftValue = evaluateExpr(aExpr.left, rowData);
            Object rightValue = evaluateExpr(aExpr.right, rowData);

            return compareValues(leftValue, rightValue, aExpr.op);
        }

        return false;
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
                return rowData.get(i);
            }
        }
        return null;
    }

    private boolean compareValues(Object left, Object right, String operator) {
        if (left == null || right == null) {
            return false;
        }

        return switch (operator) {
            case "=" -> left.equals(right);
            case "!=" -> !left.equals(right);
            case ">" -> compareNumeric(left, right) > 0;
            case ">=" -> compareNumeric(left, right) >= 0;
            case "<" -> compareNumeric(left, right) < 0;
            case "<=" -> compareNumeric(left, right) <= 0;
            default -> false;
        };
    }

    private int compareNumeric(Object left, Object right) {
        if (left instanceof Number && right instanceof Number) {
            double leftNum = ((Number) left).doubleValue();
            double rightNum = ((Number) right).doubleValue();
            return Double.compare(leftNum, rightNum);
        }
        return left.toString().compareTo(right.toString());
    }
}