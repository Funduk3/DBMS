package ru.open.cu.student.planner;

import ru.open.cu.student.ast.QueryTree;
import ru.open.cu.student.ast.ColumnDef;
import ru.open.cu.student.ast.TargetEntry;
import ru.open.cu.student.catalog.manager.CatalogManager;
import ru.open.cu.student.catalog.model.ColumnDefinition;
import ru.open.cu.student.catalog.model.TableDefinition;
import ru.open.cu.student.catalog.model.TypeDefinition;
import ru.open.cu.student.ast.Expr;
import ru.open.cu.student.planner.node.*;

import java.util.ArrayList;
import java.util.List;

public class PlannerImpl implements Planner {

    private final CatalogManager catalogManager;

    public PlannerImpl(CatalogManager catalogManager) {
        this.catalogManager = catalogManager;
    }

    @Override
    public LogicalPlanNode plan(QueryTree queryTree) {
        if (queryTree == null) throw new IllegalArgumentException("QueryTree is null");

        return switch (queryTree.getQueryType()) {
            case CREATE -> planCreate(queryTree);
            case INSERT -> planInsert(queryTree);
            case SELECT -> planSelect(queryTree);
        };
    }

    private LogicalPlanNode planCreate(QueryTree q) {
        String tableName = extractTableName(q);

        List<ColumnDefinition> columns = new ArrayList<>();
        int position = 0;

        List<ColumnDef> createCols = q.getCreateColumns();
        if (createCols != null) {
            for (ColumnDef cd : createCols) {
                String typeName = cd.getTypeName().getName();
                TypeDefinition type = catalogManager.getTypeByName(typeName);

                columns.add(new ColumnDefinition(
                        position++,
                        cd.getColname(),
                        type.getOid()
                ));
            }
        }

        TableDefinition tableDef = new TableDefinition(0, tableName, "USER", tableName, 0);
        tableDef.setColumns(columns);

        return new CreateTableNode(tableDef);
    }

    private LogicalPlanNode planInsert(QueryTree q) {
        String tableName = extractTableName(q);
        TableDefinition tableDef = catalogManager.getTable(tableName);

        List<Expr> values = new ArrayList<>();
        List<?> rawValues = q.getInsertValues();
        if (rawValues != null) {
            for (Object v : rawValues) {
                values.add((Expr) v);
            }
        }

        return new InsertNode(tableDef, values);
    }

    private LogicalPlanNode planSelect(QueryTree q) {
        String tableName = extractTableName(q);
        TableDefinition tableDef = catalogManager.getTable(tableName);

        LogicalPlanNode currentNode = new ScanNode(tableDef);

        Expr where = q.getFilter();
        if (where != null) {
            currentNode = new FilterNode(currentNode, where);
        }

        List<?> targets = q.getTargetColumns();
        if (targets != null && !targets.isEmpty()) {
            currentNode = new ProjectNode(currentNode, (List<TargetEntry>) targets);
        }

        return currentNode;
    }

    private String extractTableName(QueryTree q) {
        if (q.getFromTables() != null && !q.getFromTables().isEmpty() && q.getFromTables().get(0) != null) {
            return q.getFromTables().get(0).getName();
        }
        if (q.getCreateTable() != null && q.getCreateTable().getName() != null && !q.getCreateTable().getName().isEmpty()) {
            return q.getCreateTable().getName();
        }
        if (q.getInsertTable() != null && q.getInsertTable().getName() != null && !q.getInsertTable().getName().isEmpty()) {
            return q.getInsertTable().getName();
        }
        throw new IllegalArgumentException("Cannot determine table name");
    }
}
