package ru.open.cu.student.optimizer.node;

import ru.open.cu.student.catalog.model.TableDefinition;

public class PhysicalSeqScanNode extends PhysicalPlanNode {

    private final TableDefinition tableDefinition;

    public PhysicalSeqScanNode(TableDefinition tableDefinition) {
        super("PhysicalSeqScan");
        this.tableDefinition = tableDefinition;
    }

    public TableDefinition getTableDefinition() {
        return tableDefinition;
    }

    @Override
    public String prettyPrint(String indent) {
        return indent + "PhysicalSeqScan(" + tableDefinition.getName() + ")\n";
    }
}