package ru.open.cu.student.parser.nodes.Statements;

import ru.open.cu.student.parser.nodes.AstNode;

public class CreateIndexStmt implements AstNode {
    String indexName;
    String tableName;
    String columnName;
    String indexType; // "HASH" or "BTREE"

    public CreateIndexStmt(String indexName, String tableName, String columnName, String indexType) {
        this.indexName = indexName;
        this.tableName = tableName;
        this.columnName = columnName;
        this.indexType = indexType != null ? indexType.toUpperCase() : "BTREE"; // По умолчанию BTREE
    }

    @Override
    public String toString() {
        return String.format("CREATE INDEX %s ON %s(%s) USING %s", 
                indexName, tableName, columnName, indexType);
    }

    public String getIndexName() {
        return indexName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getColumnName() {
        return columnName;
    }

    public String getIndexType() {
        return indexType;
    }
}

