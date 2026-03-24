package ru.open.cu.student.planner.node;

public class CreateIndexNode extends LogicalPlanNode {
    private final String indexName;
    private final String tableName;
    private final String columnName;
    private final String indexType; // "HASH" or "BTREE"

    public CreateIndexNode(String indexName, String tableName, String columnName, String indexType) {
        super("CreateIndex");
        this.indexName = indexName;
        this.tableName = tableName;
        this.columnName = columnName;
        this.indexType = indexType;
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

    @Override
    public String prettyPrint(String indent) {
        return indent + "CreateIndex(" + indexName + ", " + tableName + "." + columnName + ", " + indexType + ")\n";
    }
}

