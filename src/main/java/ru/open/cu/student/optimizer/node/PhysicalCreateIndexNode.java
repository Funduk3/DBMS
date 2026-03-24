package ru.open.cu.student.optimizer.node;

public class PhysicalCreateIndexNode extends PhysicalPlanNode {
    private final String indexName;
    private final String tableName;
    private final String columnName;
    private final String indexType; // "HASH" or "BTREE"

    public PhysicalCreateIndexNode(String indexName, String tableName, String columnName, String indexType) {
        super("PhysicalCreateIndex");
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
        return indent + "PhysicalCreateIndex(" + indexName + ", " + tableName + "." + columnName + ", " + indexType + ")\n";
    }
}

