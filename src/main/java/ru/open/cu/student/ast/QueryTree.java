package ru.open.cu.student.ast;

import ru.open.cu.student.catalog.model.ColumnDefinition;
import ru.open.cu.student.catalog.model.TableDefinition;

import java.util.List;
import java.util.stream.Collectors;

public class QueryTree {
    private final QueryType queryType;
    private List<ColumnDefinition> targetColumns;
    private List<TableDefinition> fromTables;
    private AExpr filter;
    private List<Object> insertValues;
    private TableDefinition insertTable;
    private TableDefinition createTable;
    private List<ColumnDef> createColumns;

    public enum QueryType {
        SELECT, INSERT, CREATE
    }

    public QueryTree(List<ColumnDefinition> targetColumns, List<TableDefinition> fromTables, AExpr filter) {
        this.queryType = QueryType.SELECT;
        this.targetColumns = targetColumns;
        this.fromTables = fromTables;
        this.filter = filter;
    }

    @SuppressWarnings("unchecked")
    public QueryTree(TableDefinition table, List<?> data, QueryType type) {
        if (type.equals(QueryType.INSERT)) {
            this.queryType = QueryType.INSERT;
            this.insertTable = table;
            this.insertValues = (List<Object>) data;
            this.fromTables = List.of(table);
        } else if (type.equals(QueryType.CREATE)) {
            this.queryType = QueryType.CREATE;
            this.createTable = table;
            this.createColumns = (List<ColumnDef>) data;
        } else {
            throw new IllegalArgumentException("Unsupported query type for this constructor: " + type);
        }
    }

    public QueryType getQueryType() {
        return queryType;
    }

    public List<ColumnDefinition> getTargetColumns() {
        return targetColumns;
    }

    public List<TableDefinition> getFromTables() {
        return fromTables;
    }

    public AExpr getFilter() {
        return filter;
    }

    public List<Object> getInsertValues() {
        return insertValues;
    }

    public TableDefinition getInsertTable() {
        return insertTable != null ? insertTable :
                (fromTables != null && !fromTables.isEmpty() ? fromTables.get(0) : null);
    }

    public TableDefinition getCreateTable() {
        return createTable;
    }

    public List<ColumnDef> getCreateColumns() {
        return createColumns;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("QueryTree(").append(queryType).append(")\n");

        switch (queryType) {
            case SELECT:
                sb.append("├── targetColumns: [");
                sb.append(targetColumns.stream()
                        .map(c -> c.getName())
                        .collect(Collectors.joining(", ")));
                sb.append("]\n");

                sb.append("├── fromTables: [");
                sb.append(fromTables.stream()
                        .map(TableDefinition::getName)
                        .collect(Collectors.joining(", ")));
                sb.append("]\n");

                sb.append("└── filter: ");
                sb.append(filter != null ? filter.toString() : "null");
                break;

            case INSERT:
                TableDefinition insertTable = getInsertTable();
                sb.append("├── table: ").append(insertTable != null ? insertTable.getName() : "null").append("\n");
                sb.append("└── values: ").append(insertValues != null ? insertValues.toString() : "null");
                break;

            case CREATE:
                sb.append("├── table: ").append(createTable.getName()).append("\n");
                sb.append("└── columns: [");
                sb.append(createColumns.stream()
                        .map(cd -> cd.colname + ":" + cd.typeName.getName())
                        .collect(Collectors.joining(", ")));
                sb.append("]");
                break;
        }

        return sb.toString();
    }
}