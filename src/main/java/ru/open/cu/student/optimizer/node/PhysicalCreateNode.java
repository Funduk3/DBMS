package ru.open.cu.student.optimizer.node;


import ru.open.cu.student.catalog.model.TableDefinition;

public class PhysicalCreateNode extends PhysicalPlanNode {

    private final TableDefinition tableDefinition;

    public PhysicalCreateNode(TableDefinition tableDefinition) {
        super("PhysicalCreate");
        this.tableDefinition = tableDefinition;
    }

    public TableDefinition getTableDefinition() {
        return tableDefinition;
    }

    @Override
    public String prettyPrint(String indent) {
        return indent + "PhysicalCreate(" + tableDefinition.getName() + ")\n";
    }
}