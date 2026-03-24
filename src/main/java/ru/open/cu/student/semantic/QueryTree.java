package ru.open.cu.student.semantic;

import ru.open.cu.student.catalog.model.ColumnDefinition;
import ru.open.cu.student.catalog.model.TableDefinition;
import ru.open.cu.student.parser.nodes.*;

import java.util.List;
import java.util.stream.Collectors;

public class QueryTree {
    private final QueryType queryType;
    private List<ColumnDefinition> targetColumns;
    private List<TableDefinition> fromTables;
    private AExpr filter;
    private List<Object> insertValues;
    private TableDefinition insertTable;

    private String createTableName;
    private List<ColumnDefinition> createColumns;
    
    // Для CREATE INDEX
    private String createIndexName;
    private String createIndexTableName;
    private String createIndexColumnName;
    private String createIndexType;

    public enum QueryType {
        SELECT, INSERT, CREATE, CREATE_INDEX
    }

    public QueryTree(List<ColumnDefinition> targetColumns, List<TableDefinition> fromTables, AExpr filter) {
        this.queryType = QueryType.SELECT;
        this.targetColumns = targetColumns;
        this.fromTables = fromTables;
        this.filter = filter;
    }

    public QueryTree(TableDefinition table, List<Object> values, QueryType type) {
        if (type != QueryType.INSERT) {
            throw new IllegalArgumentException("This constructor is only for INSERT queries");
        }
        this.queryType = QueryType.INSERT;
        this.insertTable = table;
        this.insertValues = values;
        this.fromTables = List.of(table);
    }

    public QueryTree(String tableName, List<ColumnDefinition> columns, QueryType type) {
        if (type != QueryType.CREATE) {
            throw new IllegalArgumentException("This constructor is only for CREATE queries");
        }
        this.queryType = QueryType.CREATE;
        this.createTableName = tableName;
        this.createColumns = columns;
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

    public String getCreateTableName() {
        return createTableName;
    }

    public List<ColumnDefinition> getCreateColumns() {
        return createColumns;
    }

    // Конструктор для CREATE INDEX
    public QueryTree(String indexName, String tableName, String columnName, String indexType) {
        this.queryType = QueryType.CREATE_INDEX;
        this.createIndexName = indexName;
        this.createIndexTableName = tableName;
        this.createIndexColumnName = columnName;
        this.createIndexType = indexType;
    }

    public String getCreateIndexName() {
        return createIndexName;
    }

    public String getCreateIndexTableName() {
        return createIndexTableName;
    }

    public String getCreateIndexColumnName() {
        return createIndexColumnName;
    }

    public String getCreateIndexType() {
        return createIndexType;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("QueryTree(").append(queryType).append(")\n");

        switch (queryType) {
            case SELECT:
                sb.append("├── targetColumns: [");
                sb.append(targetColumns.stream()
                        .map(ColumnDefinition::getName)
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
                sb.append("├── tableName: ").append(createTableName).append("\n");
                sb.append("└── columns: [");
                sb.append(createColumns.stream()
                        .map(cd -> cd.getName() + ":" + cd.getTypeOid())
                        .collect(Collectors.joining(", ")));
                sb.append("]");
                break;

            case CREATE_INDEX:
                sb.append("├── indexName: ").append(createIndexName).append("\n");
                sb.append("├── tableName: ").append(createIndexTableName).append("\n");
                sb.append("├── columnName: ").append(createIndexColumnName).append("\n");
                sb.append("└── indexType: ").append(createIndexType);
                break;
        }

        return sb.toString();
    }
}