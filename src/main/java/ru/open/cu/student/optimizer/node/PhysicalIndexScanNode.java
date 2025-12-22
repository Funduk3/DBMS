package ru.open.cu.student.optimizer.node;

import ru.open.cu.student.index.IndexType;

public class PhysicalIndexScanNode extends PhysicalPlanNode {

    private final String indexName;
    private final String tableName;
    private final IndexType indexType;
    private Comparable<?> rangeFrom;
    private Comparable<?> rangeTo;
    private Comparable<?> value;
    private boolean hasRange;
    private boolean inclusive;

    public PhysicalIndexScanNode(String indexName, String tableName, IndexType indexType) {
        super("PhysicalIndexScan");
        this.indexName = indexName;
        this.tableName = tableName;
        this.indexType = indexType;
        this.hasRange = false;
        this.inclusive = true;
    }

    public String getIndexName() {
        return indexName;
    }

    public String getTableName() {
        return tableName;
    }

    public IndexType getIndexType() {
        return indexType;
    }

    public Comparable<?> getRangeFrom() {
        return rangeFrom;
    }

    public Comparable<?> getRangeTo() {
        return rangeTo;
    }

    public Comparable<?> getValue() {
        return value;
    }

    public boolean hasRange() {
        return hasRange;
    }

    public boolean isInclusive() {
        return inclusive;
    }

    public void setRange(Comparable<?> rangeFrom, Comparable<?> rangeTo, boolean inclusive) {
        this.rangeFrom = rangeFrom;
        this.rangeTo = rangeTo;
        this.hasRange = true;
        this.inclusive = inclusive;
    }

    public void setValue(Comparable<?> value) {
        this.value = value;
        this.hasRange = false;
    }

    @Override
    public String prettyPrint(String indent) {
        if (hasRange) {
            return indent + "PhysicalIndexScan(" + indexName + ", " + tableName +
                    ", range=[" + rangeFrom + ", " + rangeTo + "]" +
                    (inclusive ? " inclusive" : "") + ")\n";
        } else {
            return indent + "PhysicalIndexScan(" + indexName + ", " + tableName +
                    ", value=" + value + ")\n";
        }
    }
}