package ru.open.cu.student.planner;

import ru.open.cu.student.parser.nodes.ColumnRef;
import ru.open.cu.student.parser.nodes.Expr;
import ru.open.cu.student.semantic.QueryTree;
import ru.open.cu.student.parser.nodes.TargetEntry;
import ru.open.cu.student.catalog.manager.CatalogManager;
import ru.open.cu.student.catalog.model.ColumnDefinition;
import ru.open.cu.student.catalog.model.TableDefinition;
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
        String tableName = q.getCreateTableName();
        List<ColumnDefinition> columns = q.getCreateColumns();

        if (tableName == null || tableName.isEmpty()) {
            throw new IllegalArgumentException("Table name is required for CREATE");
        }
        if (columns == null || columns.isEmpty()) {
            throw new IllegalArgumentException("Columns are required for CREATE");
        }

        TableDefinition tableDef = new TableDefinition(0, tableName, "USER", tableName, 0);
        tableDef.setColumns(columns);

        return new CreateTableNode(tableDef);
    }

    private LogicalPlanNode planInsert(QueryTree q) {
        TableDefinition tableDef = q.getInsertTable();

        if (tableDef == null) {
            throw new IllegalArgumentException("Table definition is required for INSERT");
        }

        List<Expr> values = new ArrayList<>();
        List<Object> rawValues = q.getInsertValues();

        if (rawValues != null) {
            for (Object v : rawValues) {
                values.add((Expr) v);
            }
        }

        return new InsertNode(tableDef, values);
    }

    private LogicalPlanNode planSelect(QueryTree q) {
        List<TableDefinition> tables = q.getFromTables();

        if (tables == null || tables.isEmpty()) {
            throw new IllegalArgumentException("FROM clause is required for SELECT");
        }

        // Проверяем существование КАЖДОЙ таблицы из FROM в каталоге
        for (TableDefinition table : tables) {
            // Используем существующий метод getTable для проверки
            if (catalogManager.getTable(table.getName()) == null) {
                throw new IllegalArgumentException("Table '" + table.getName() + "' does not exist");
            }
        }

        // Получаем актуальное определение таблицы из каталога
        TableDefinition tableDef = catalogManager.getTable(tables.get(0).getName());
        LogicalPlanNode currentNode = new ScanNode(tableDef);

        Expr where = q.getFilter();
        if (where != null) {
            currentNode = new FilterNode(currentNode, where);
        }

        List<ColumnDefinition> targetColumns = q.getTargetColumns();
        if (targetColumns != null && !targetColumns.isEmpty()) {
            List<TargetEntry> targets = new ArrayList<>();
            for (ColumnDefinition col : targetColumns) {
                boolean columnExists = false;
                for (ColumnDefinition tableCol : catalogManager.getTableColumns(tableDef.getOid())) {
                    if (tableCol.getName().equals(col.getName())) {
                        columnExists = true;
                        break;
                    }
                }

                if (!columnExists) {
                    throw new IllegalArgumentException("Column '" + col.getName() +
                            "' does not exist in table '" + tableDef.getName() + "'");
                }

                ColumnRef columnRef = new ColumnRef(tableDef.getName(), col.getName());
                String typeName = catalogManager.getType(col.getTypeOid()).getName();
                TargetEntry entry = new TargetEntry(columnRef, col.getName());
                entry.resultType = typeName;
                targets.add(entry);
            }
            currentNode = new ProjectNode(currentNode, targets);
        }

        return currentNode;
    }
}